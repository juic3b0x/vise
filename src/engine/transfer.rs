use super::{
    Job, Manager, Metadata,
    http::{check_response, content_range},
    journal::sync_directory_blocking,
    layout::make_chunks,
};
use crate::{
    checksum,
    error::{Error, Result},
    events::EventKind,
    model::{Chunk, ChunkStatus, Record, Status, timestamp_millis},
    net,
};
use bytes::{Buf, Bytes, BytesMut};
use futures_util::StreamExt;
use reqwest::{Response, StatusCode};
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    fs,
    io::{AsyncSeekExt, AsyncWriteExt},
};
use tokio_util::sync::CancellationToken;
#[cfg(test)]
use uuid::Uuid;

const BUFFER_SIZE: usize = 256 * 1024;

impl Manager {
    pub(super) async fn execute(&self, job: Arc<Job>, cancel: CancellationToken) -> Result<()> {
        let _permit = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(Error::Cancelled),
            permit = self.0.pool.acquire() => permit.map_err(|error| Error::Worker(error.to_string()))?,
        };
        job.prompt_recovery.reset();
        self.transition(&job, Status::Probing).await?;
        self.refresh_cookies(&job).await?;
        let url = job.record.read().await.url.clone();
        let client = job.client.read().await.clone();
        let fresh = job.record.read().await.chunks.is_empty();
        let request_started = Instant::now();
        let (metadata, response) = if fresh {
            let range = if self.0.config.connections_per_download == 1 {
                super::http::InitialRange::Whole
            } else {
                super::http::InitialRange::FreshPrefix
            };
            self.initial_response(&client, &url, &cancel, range).await?
        } else {
            (self.probe(&client, &url, &cancel).await?, None)
        };
        let setup = request_started.elapsed();
        let prefix = if metadata.ranged {
            response
                .as_ref()
                .filter(|r| r.status() == StatusCode::PARTIAL_CONTENT)
                .map(content_range)
                .transpose()?
                .map(|(_, end, _)| end + 1)
        } else {
            None
        };
        let (mut file, path) = self.prepare_partial(&job, metadata, prefix).await?;
        job.adaptive
            .reset(&*job.record.read().await, self.0.rate_limiter.limit());
        job.adaptive.record_setup(setup);
        self.transition(&job, Status::Downloading).await?;
        let transfer_timing = crate::performance::Timer::new(crate::performance::Phase::Transfer);
        match response {
            Some(response) if job.record.read().await.chunks.len() == 1 => {
                self.transfer_chunk_with_response(
                    job.clone(),
                    0,
                    &cancel,
                    Some((response, &mut file)),
                )
                .await?;
            }
            response => self.run_chunks(&job, &cancel, response).await?,
        }
        transfer_timing.finish();
        self.publish_file(&job, file, path, &cancel).await
    }

    async fn refresh_cookies(&self, job: &Job) -> Result<()> {
        let (path, proxy, context) = {
            let record = job.record.read().await;
            (
                record.cookies_file.clone(),
                record.proxy_url.clone(),
                record.request_credentials.clone(),
            )
        };
        if path.is_some() || context.is_some() {
            let (client, _) = net::Transport::new(
                self.0.config.read_timeout,
                proxy.as_ref(),
                path.as_deref(),
                context.as_deref(),
            )
            .await?;
            *job.client.write().await = client;
        }
        Ok(())
    }

    async fn prepare_partial(
        &self,
        job: &Job,
        metadata: Metadata,
        prefix: Option<u64>,
    ) -> Result<(fs::File, PathBuf)> {
        let _timing = crate::performance::Timer::new(crate::performance::Phase::Preparation);
        validate_size(metadata.total)?;
        let path = job.part_path.clone();
        let _checkpoint = self.checkpoint_gate().await;
        let record = job.record.read().await.clone();
        let fresh =
            !job.staged.load(std::sync::atomic::Ordering::Acquire) && record.chunks.is_empty();
        // Fresh jobs skip checkpoint inspection until this first access. Validate
        // here, under the same gate as creation and captured staging eligibility.
        let open_path = path.clone();
        let (file, created) =
            tokio::task::spawn_blocking(move || open_partial(&record, &open_path))
                .await
                .map_err(|error| Error::Worker(error.to_string()))??;
        job.staged.store(true, std::sync::atomic::Ordering::Release);
        let total = metadata.total;
        let reusable = apply_metadata(
            &mut *job.record.write().await,
            metadata,
            self.0.config.connections_per_download,
        );
        if !reusable && let Some(end) = prefix {
            super::layout::retain_prefix(&mut job.record.write().await.chunks, end);
        }
        if !reusable {
            // Invalidate durable offsets before destroying their corresponding data.
            if !(fresh && created) {
                self.checkpoint_locked().await?;
            }
        }
        job.durability.invalidate();
        let file =
            tokio::task::spawn_blocking(move || size_partial(file, total, reusable, created))
                .await
                .map_err(|error| Error::Worker(error.to_string()))??;
        #[cfg(test)]
        if fresh && created {
            crate::database::split_test_hook("after-fresh-preparation");
        }
        Ok((file, path))
    }

    async fn publish_file(
        &self,
        job: &Job,
        mut file: fs::File,
        path: PathBuf,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let _timing = crate::performance::Timer::new(crate::performance::Phase::Publication);
        let record = job.record.read().await;
        let downloaded = record.downloaded();
        if record.total_bytes.is_some_and(|total| total != downloaded) {
            return Err(Error::Protocol(
                "download length does not match expected length".into(),
            ));
        }
        let target = job.output_path.clone();
        drop(record);
        job.record.write().await.total_bytes = Some(downloaded);
        // Known-length files were sized at preparation; unknown streams need
        // their final length established before the snapshot durability barrier.
        if file.metadata().await?.len() != downloaded {
            job.durability.invalidate();
            file.set_len(downloaded).await?;
        }
        file.flush().await?;
        let file = self.certify_partial(job, file).await?;
        self.verify_staged(job, &path, cancel).await?;
        // Save all offsets before publishing, making an interrupted commit recoverable.
        self.checkpoint().await?;
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        job.durability.forget();
        // Close the staging handle before changing its name (notably on Windows).
        drop(file);
        tokio::task::spawn_blocking(move || {
            std::fs::hard_link(&path, &target).map_err(|e| {
                if e.kind() == std::io::ErrorKind::AlreadyExists {
                    Error::Conflict(format!("output already exists: {}", target.display()))
                } else {
                    e.into()
                }
            })?;
            sync_directory_blocking(super::storage::parent(&target)?)?;
            // Retain same-inode recovery evidence until terminal SQL commit.
            std::fs::rename(&path, super::journal::publication_path(&path))?;
            sync_directory_blocking(super::storage::parent(&path)?)
        })
        .await
        .map_err(|e| Error::Worker(e.to_string()))??;
        Ok(())
    }

    async fn certify_partial(&self, job: &Job, file: fs::File) -> Result<std::fs::File> {
        let snapshot = job.record.read().await.clone();
        let revision = job.durability.revision();
        let durability = job.durability.clone();
        let file = file.into_std().await;
        #[cfg(test)]
        let trace = self.0.journal_trace.clone();
        // Keep the expensive barrier parallel across ready jobs. Its data-only
        // certificate lets the grouped checkpoint avoid repeating it, without
        // trusting the pathname or claiming namespace durability here.
        let file = tokio::task::spawn_blocking(move || {
            let synced = durability.certify_data(&file, &snapshot, revision, || {
                let _timing = crate::performance::Timer::new(crate::performance::Phase::FileSync);
                #[cfg(test)]
                trace.check_file()?;
                file.sync_all()?;
                Ok(())
            })?;
            #[cfg(not(test))]
            let _ = synced;
            #[cfg(test)]
            if synced {
                trace.record(super::journal::barriers::Phase::PublicationFile);
            }
            Ok::<_, Error>(file)
        })
        .await
        .map_err(|e| Error::Worker(e.to_string()))??;
        Ok(file)
    }

    async fn verify_staged(
        &self,
        job: &Job,
        path: &std::path::Path,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let expected = job.record.read().await.checksums.expected();
        if !expected.is_empty() {
            self.transition(job, Status::Verifying).await?;
            let mut report =
                checksum::hash_file(path.to_path_buf(), expected, cancel.clone()).await?;
            let mismatch = report.mismatch();
            if mismatch.is_none() {
                report.verified_at = Some(timestamp_millis());
            }
            job.record.write().await.checksums = report;
            if let Some(details) = mismatch {
                return Err(Error::ChecksumMismatch(details));
            }
            self.publish_job(EventKind::Verified, job).await;
        }
        Ok(())
    }

    pub(super) async fn transfer_chunk(
        &self,
        job: Arc<Job>,
        index: u64,
        cancel: &CancellationToken,
    ) -> Result<()> {
        self.transfer_chunk_with_response(job, index, cancel, None)
            .await
    }

    pub(super) async fn transfer_initial_chunk(
        &self,
        job: Arc<Job>,
        response: Response,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(&job.part_path)
            .await?;
        self.transfer_chunk_with_response(job, 0, cancel, Some((response, &mut file)))
            .await
    }

    async fn transfer_chunk_with_response(
        &self,
        job: Arc<Job>,
        index: u64,
        cancel: &CancellationToken,
        mut initial: Option<(Response, &mut fs::File)>,
    ) -> Result<()> {
        let mut attempt = 0;
        loop {
            if cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }
            job.adaptive.resolved(index, cancel).await?;
            let written_before = chunk_at(&*job.record.read().await, index)?.written;
            chunk_at_mut(&mut *job.record.write().await, index)?.status = ChunkStatus::Downloading;
            let result = if let Some((response, file)) = initial.take() {
                // Metadata, checksums and range bounds were validated before
                // staging preparation. Charge every payload byte as usual.
                let end = chunk_at(&*job.record.read().await, index)?.end;
                job.adaptive.begin_body(index, 0);
                // The freshly opened preparation handle is still at zero.
                self.stream_chunk_from_file(&job, index, response, (0, end), cancel, file)
                    .await
            } else {
                self.transfer_once(&job, index, cancel).await
            };
            if let Err(error) = &result {
                job.adaptive.refused(error);
            }
            job.adaptive.stopped(index);
            let result = if job.record.read().await.request_credentials.is_some() {
                result.map_err(Error::without_request_details)
            } else {
                result
            };
            match result {
                Ok(()) => {
                    job.adaptive.resolved(index, cancel).await?;
                    chunk_at_mut(&mut *job.record.write().await, index)?.status =
                        ChunkStatus::Completed;
                    return Ok(());
                }
                Err(error) if error.retryable() && attempt < self.0.config.retries => {
                    self.retry_chunk(&job, index, attempt, written_before, &error, cancel)
                        .await?;
                    attempt += 1;
                }
                Err(error) => {
                    chunk_at_mut(&mut *job.record.write().await, index)?.status =
                        if matches!(error, Error::Cancelled) {
                            ChunkStatus::Paused
                        } else {
                            ChunkStatus::Failed
                        };
                    return Err(error);
                }
            }
        }
    }

    pub(super) async fn transfer_once(
        &self,
        job: &Arc<Job>,
        index: u64,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let (url, ranged, validator, chunk) = {
            let record = job.record.read().await;
            (
                record.url.clone(),
                record.ranged,
                record.validator.clone(),
                chunk_at(&record, index)?.clone(),
            )
        };
        if chunk.end == Some(0)
            || (ranged
                && chunk
                    .end
                    .is_some_and(|end| chunk.start + chunk.written == end))
        {
            return Ok(());
        }
        let offset = if ranged {
            chunk.start + chunk.written
        } else {
            0
        };
        let range = requested_range(ranged, offset, chunk.end)?;
        let client = job.client.read().await.clone();
        let request_started = Instant::now();
        let response = self
            .request(
                &client,
                &url,
                range.as_deref(),
                if ranged { validator.as_deref() } else { None },
                cancel,
            )
            .await?;
        job.adaptive.record_setup(request_started.elapsed());
        check_response(&response)?;
        if ranged {
            let expected = job.record.read().await.total_bytes;
            validate_range(&response, offset, chunk.end, expected)?;
        } else if response.status() != StatusCode::OK {
            return Err(Error::Protocol(format!(
                "expected HTTP 200 for stream, got {}",
                response.status()
            )));
        }
        validate_validator(&response, validator.as_deref())?;
        if !ranged {
            self.reset_stream(job, index, &response).await?;
        }
        self.learn_checksums(job, &response).await?;
        let request_end = if ranged {
            chunk.end
        } else {
            chunk_at(&*job.record.read().await, index)?.end
        };
        job.adaptive.begin_body(index, offset);
        self.stream_chunk(job, index, response, offset, request_end, cancel)
            .await
    }

    async fn reset_stream(&self, job: &Job, index: u64, response: &Response) -> Result<()> {
        validate_size(response.content_length())?;
        let _checkpoint = self.checkpoint_gate().await;
        let mut record = job.record.write().await;
        let chunk = chunk_at_mut(&mut record, index)?;
        chunk.end = response.content_length();
        chunk.written = 0;
        record.total_bytes = response.content_length();
        job.adaptive.reset(&record, self.0.rate_limiter.limit());
        drop(record);
        self.checkpoint_locked().await?;
        job.durability.invalidate();
        fs::OpenOptions::new()
            .write(true)
            .open(&job.part_path)
            .await?
            .set_len(0)
            .await?;
        Ok(())
    }

    async fn learn_checksums(&self, job: &Job, response: &Response) -> Result<()> {
        let discovered = checksum::from_response(response)?;
        let changed = {
            let mut record = job.record.write().await;
            let mut changed = false;
            if record.checksums.expected_sha256.is_none() && discovered.sha256.is_some() {
                record.checksums.expected_sha256 = discovered.sha256;
                changed = true;
            }
            if record.checksums.expected_md5.is_none() && discovered.md5.is_some() {
                record.checksums.expected_md5 = discovered.md5;
                changed = true;
            }
            changed
        };
        if changed {
            self.checkpoint().await?;
        }
        Ok(())
    }

    pub(super) async fn stream_chunk(
        &self,
        job: &Job,
        key: u64,
        response: Response,
        offset: u64,
        request_end: Option<u64>,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(&job.part_path)
            .await?;
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        self.stream_chunk_from_file(job, key, response, (offset, request_end), cancel, &mut file)
            .await
    }

    async fn stream_chunk_from_file(
        &self,
        job: &Job,
        key: u64,
        response: Response,
        bounds: (u64, Option<u64>),
        cancel: &CancellationToken,
        file: &mut fs::File,
    ) -> Result<()> {
        let record = job.record.read().await;
        let fixed = self.0.config.connections_per_download == 1
            || !record.ranged
            || self.0.config.min_split_size == 0
            || record.total_bytes.is_some_and(|total| {
                total
                    < self
                        .0
                        .config
                        .min_split_size
                        .saturating_mul(2)
                        .saturating_add(super::adaptive::MARGIN)
            });
        drop(record);
        let mut body = ChunkBody::new(self, job, key, file, bounds, fixed);

        let result = body.receive(response, cancel).await;
        body.account_discarded();
        result
    }
}

fn open_partial(record: &Record, path: &std::path::Path) -> Result<(std::fs::File, bool)> {
    crate::paths::staging_files(record)?
        .ok_or_else(|| Error::Invalid("staging directory is unavailable".into()))?;
    match std::fs::OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(path)
    {
        Ok(file) => Ok((file, true)),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok((
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)?,
            false,
        )),
        Err(error) => Err(Error::Io(error)),
    }
}

fn size_partial(
    file: std::fs::File,
    total: Option<u64>,
    reusable: bool,
    created: bool,
) -> Result<fs::File> {
    if !reusable && !created {
        file.set_len(0)?;
    }
    if let Some(total) = total {
        file.set_len(total)?;
    }
    Ok(fs::File::from_std(file))
}

/// One response's file buffer and offsets; the HTTP request end remains fixed
/// while the job's owned end may shrink during streaming.
struct ChunkBody<'a> {
    manager: &'a Manager,
    job: &'a Job,
    key: u64,
    file: &'a mut fs::File,
    pending: BytesMut,
    last_flush: Instant,
    offset: u64,
    request_end: Option<u64>,
    received: u64,
    position: u64,
    fixed: bool,
}

impl<'a> ChunkBody<'a> {
    fn new(
        manager: &'a Manager,
        job: &'a Job,
        key: u64,
        file: &'a mut fs::File,
        bounds: (u64, Option<u64>),
        fixed: bool,
    ) -> Self {
        Self {
            manager,
            job,
            key,
            file,
            pending: BytesMut::with_capacity(BUFFER_SIZE),
            last_flush: Instant::now(),
            offset: bounds.0,
            request_end: bounds.1,
            received: 0,
            position: bounds.0,
            fixed,
        }
    }

    async fn receive(&mut self, response: Response, cancel: &CancellationToken) -> Result<()> {
        // Keep the raw frame visible: throttle's private pending frame cannot be
        // counted or charged when a shortened response is dropped.
        let mut stream = response.bytes_stream().boxed();
        loop {
            let next = tokio::select! { biased;
                _ = cancel.cancelled() => return Err(Error::Cancelled),
                value = stream.next() => value,
            };
            match next {
                Some(Ok(mut bytes)) => {
                    self.record_frame(bytes.len())?;
                    if self.write_frame(&mut bytes, cancel).await? {
                        // Close upstream before flushing or charging the frame
                        // remainder; never drain the abandoned HTTP suffix.
                        drop(stream);
                        return self.finish_split(bytes, cancel).await;
                    }
                }
                Some(Err(error)) => {
                    self.flush().await?;
                    return Err(error.into());
                }
                None => return self.finish_stream().await,
            }
        }
    }

    fn record_frame(&mut self, size: usize) -> Result<()> {
        self.received = self
            .received
            .checked_add(size as u64)
            .ok_or_else(|| Error::Protocol("response is too large".into()))?;
        if self
            .request_end
            .is_some_and(|end| self.received > end - self.offset)
        {
            return Err(Error::Protocol("response exceeds requested length".into()));
        }
        if !self.fixed {
            self.job
                .adaptive
                .received(self.key, self.offset + self.received);
        }
        Ok(())
    }

    // Returns true as soon as a shortened ownership boundary is reached.
    async fn write_frame(&mut self, bytes: &mut Bytes, cancel: &CancellationToken) -> Result<bool> {
        while !bytes.is_empty() {
            let wait = (!self.fixed).then(tokio::time::Instant::now);
            let grant = self
                .manager
                .0
                .rate_limiter
                .acquire(bytes.len(), cancel)
                .await?;
            let piece = bytes.split_to(grant);
            let end = if self.fixed {
                self.request_end
            } else {
                self.job.adaptive.end(self.key)
            };
            let useful = end.map_or(grant, |end| {
                end.saturating_sub(self.position).min(grant as u64) as usize
            });
            if let Some(wait) = wait {
                self.job
                    .adaptive
                    .observe(self.key, useful as u64, wait.elapsed());
            }
            let useful_bytes = piece
                .get(..useful)
                .ok_or_else(|| Error::Worker("body slice exceeds frame length".into()))?;
            buffer_body(
                self.file,
                self.job,
                self.key,
                &mut self.pending,
                useful_bytes,
                &mut self.last_flush,
                self.fixed,
            )
            .await?;
            self.position += useful as u64;
            if end.is_some_and(|end| self.position == end && Some(end) != self.request_end) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn finish_split(&mut self, mut bytes: Bytes, cancel: &CancellationToken) -> Result<()> {
        self.flush().await?;
        // Charge only bytes already fetched, keeping cancellation responsive.
        while !bytes.is_empty() {
            let count = self
                .manager
                .0
                .rate_limiter
                .acquire(bytes.len(), cancel)
                .await?;
            bytes.advance(count);
        }
        self.job.adaptive.resolved(self.key, cancel).await
    }

    async fn finish_stream(&mut self) -> Result<()> {
        self.flush().await?;
        if self
            .request_end
            .is_some_and(|end| self.received != end - self.offset)
        {
            return Err(Error::Protocol("unexpected end of response body".into()));
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        write_pending(self.file, self.job, self.key, &mut self.pending, self.fixed).await
    }

    fn account_discarded(&self) {
        if !self.fixed
            && let Some(end) = self.job.adaptive.end(self.key)
            && Some(end) != self.request_end
        {
            self.job
                .adaptive
                .discard((self.offset + self.received).saturating_sub(end));
        }
    }
}

async fn buffer_body(
    file: &mut fs::File,
    job: &Job,
    key: u64,
    pending: &mut BytesMut,
    mut bytes: &[u8],
    last_flush: &mut Instant,
    fixed: bool,
) -> Result<()> {
    while !bytes.is_empty() {
        let size = bytes.len().min(BUFFER_SIZE - pending.len());
        let (piece, rest) = bytes
            .split_at_checked(size)
            .ok_or_else(|| Error::Worker("body buffer split exceeds frame length".into()))?;
        pending.extend_from_slice(piece);
        bytes = rest;
        if pending.len() == BUFFER_SIZE {
            write_pending(file, job, key, pending, fixed).await?;
            *last_flush = Instant::now();
        }
    }
    if last_flush.elapsed() >= Duration::from_millis(250) {
        write_pending(file, job, key, pending, fixed).await?;
        *last_flush = Instant::now();
    }
    Ok(())
}

pub(super) async fn write_pending(
    file: &mut fs::File,
    job: &Job,
    key: u64,
    buffer: &mut BytesMut,
    fixed: bool,
) -> Result<()> {
    if !buffer.is_empty() {
        // Validate the stable key BEFORE writing, including the error path.
        {
            let record = job.record.read().await;
            let chunk = chunk_at(&record, key)?;
            if fixed
                && chunk.end.is_some_and(|end| {
                    buffer.len() as u64 > end.saturating_sub(chunk.start + chunk.written)
                })
            {
                return Err(Error::Protocol("write exceeds fixed chunk length".into()));
            }
        }
        // Fixed layouts cannot acquire a split child. They need the ordinary
        // flush/progress durability ordering, but no adaptive reservation.
        let size = if fixed {
            buffer.len()
        } else {
            job.adaptive.reserve(key, buffer.len())?
        };
        let bytes = buffer
            .get(..size)
            .ok_or_else(|| Error::Worker("write reservation exceeds buffer length".into()))?;
        job.durability.invalidate();
        file.write_all(bytes).await?;
        file.flush().await?;
        // Keep the reservation visible through the awaited progress update.
        chunk_at_mut(&mut *job.record.write().await, key)?.written += size as u64;
        if !fixed {
            job.adaptive.written(key, size);
        }
        #[cfg(test)]
        if job.adaptive.is_child(key) {
            super::adaptive::tests::hook("after-child-write");
        }
        buffer.clear();
    }
    Ok(())
}
pub(super) fn chunk_at(record: &Record, key: u64) -> Result<&Chunk> {
    record
        .chunks
        .binary_search_by_key(&key, |c| c.start)
        .ok()
        .and_then(|i| record.chunks.get(i))
        .ok_or_else(|| missing_chunk(key))
}
fn chunk_at_mut(record: &mut Record, key: u64) -> Result<&mut Chunk> {
    let i = record
        .chunks
        .binary_search_by_key(&key, |c| c.start)
        .map_err(|_| missing_chunk(key))?;
    record.chunks.get_mut(i).ok_or_else(|| missing_chunk(key))
}
pub(super) fn missing_chunk(key: u64) -> Error {
    Error::Worker(format!("missing chunk at byte {key}"))
}

fn validate_size(total: Option<u64>) -> Result<()> {
    if total.is_some_and(|size| size > i64::MAX as u64) {
        return Err(Error::Invalid(
            "resource size exceeds SQLite's signed 64-bit integer range".into(),
        ));
    }
    Ok(())
}

fn apply_metadata(record: &mut Record, metadata: Metadata, connections: usize) -> bool {
    let reusable = metadata.ranged
        && metadata.validator.is_some()
        && record.validator == metadata.validator
        && record.total_bytes == metadata.total
        && record.ranged
        && !record.chunks.is_empty();
    if !reusable {
        record.chunks = make_chunks(metadata.total, metadata.ranged, connections);
    }
    record.total_bytes = metadata.total;
    if record.checksums.expected_sha256.is_none() {
        record.checksums.expected_sha256 = metadata.expected.sha256;
    }
    if record.checksums.expected_md5.is_none() {
        record.checksums.expected_md5 = metadata.expected.md5;
    }
    record.ranged = metadata.ranged;
    record.validator = metadata.validator;
    reusable
}

fn requested_range(ranged: bool, offset: u64, end: Option<u64>) -> Result<Option<String>> {
    if !ranged {
        return Ok(None);
    }
    let end = end.ok_or_else(|| Error::Protocol("range has no end".into()))?;
    Ok(Some(format!("bytes={offset}-{}", end - 1)))
}

fn validate_range(
    response: &Response,
    offset: u64,
    end: Option<u64>,
    total: Option<u64>,
) -> Result<()> {
    if response.status() != StatusCode::PARTIAL_CONTENT {
        return Err(Error::Protocol(
            "server ignored range or the remote file changed; resume to reprobe".into(),
        ));
    }
    let (actual_start, actual_end, actual_total) = content_range(response)?;
    if actual_start != offset || Some(actual_end + 1) != end || Some(actual_total) != total {
        return Err(Error::Protocol(
            "Content-Range does not match the requested segment".into(),
        ));
    }
    Ok(())
}

fn validate_validator(response: &Response, expected: Option<&str>) -> Result<()> {
    if let (Some(expected), Some(actual)) = (expected, super::http::validator(response))
        && expected != actual
    {
        return Err(Error::Protocol(
            "remote file validator changed; resume to reprobe".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
