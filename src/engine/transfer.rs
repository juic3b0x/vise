use super::{
    Job, Manager, Metadata,
    http::{check_response, content_range},
    journal::sync_directory,
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
        self.transition(&job, Status::Probing).await?;
        self.refresh_cookies(&job).await?;
        let url = job.record.read().await.url.clone();
        let client = job.client.read().await.clone();
        let metadata = self.probe(&client, &url, &cancel).await?;
        let (file, path) = self.prepare_partial(&job, metadata).await?;
        job.adaptive
            .reset(&*job.record.read().await, self.0.rate_limiter.limit());
        self.transition(&job, Status::Downloading).await?;
        self.run_chunks(&job, &cancel).await?;
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

    async fn prepare_partial(&self, job: &Job, metadata: Metadata) -> Result<(fs::File, PathBuf)> {
        validate_size(metadata.total)?;
        let path = job.part_path.clone();
        let _checkpoint = self.0.checkpoint.lock().await;
        let record = job.record.read().await.clone();
        // Fresh jobs skip checkpoint inspection until this first access. Validate
        // here, under the same gate as creation and captured staging eligibility.
        tokio::task::spawn_blocking(move || paths_for_partial(&record))
            .await
            .map_err(|error| Error::Worker(error.to_string()))??;
        job.staged.store(true, std::sync::atomic::Ordering::Release);
        let file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .await?;
        let total = metadata.total;
        let reusable = apply_metadata(
            &mut *job.record.write().await,
            metadata,
            self.0.config.connections_per_download,
        );
        if !reusable {
            // Invalidate durable offsets before destroying their corresponding data.
            self.checkpoint_locked().await?;
            file.set_len(0).await?;
        }
        if let Some(total) = total {
            file.set_len(total).await?;
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
        file.set_len(downloaded).await?;
        file.flush().await?;
        file.sync_all().await?;
        self.verify_staged(job, &path, cancel).await?;
        // Save all offsets before publishing, making an interrupted commit recoverable.
        self.checkpoint().await?;
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        // Close the staging handle before changing its name (notably on Windows).
        drop(file);
        fs::hard_link(&path, &target).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                Error::Conflict(format!("output already exists: {}", target.display()))
            } else {
                e.into()
            }
        })?;
        sync_directory(super::storage::parent(&target)?.to_path_buf()).await?;
        // Await the staging rename before execute succeeds and the queue can
        // expose Completed. Retain the same-inode evidence under a private name
        // until SQLite commits, so a crash here is recoverable without HTTP.
        fs::rename(&path, super::journal::publication_path(&path)).await?;
        sync_directory(super::storage::parent(&path)?.to_path_buf()).await?;
        Ok(())
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
        let mut attempt = 0;
        loop {
            if cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }
            job.adaptive.resolved(index, cancel).await?;
            chunk_at_mut(&mut *job.record.write().await, index)?.status = ChunkStatus::Downloading;
            let result = self.transfer_once(&job, index, cancel).await;
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
                    let position = job
                        .record
                        .read()
                        .await
                        .chunks
                        .binary_search_by_key(&index, |c| c.start)
                        .map_err(|_| missing_chunk(index))?;
                    tracing::warn!(chunk = position, retry = attempt + 1, %error, "retrying transfer");
                    self.publish_job(
                        EventKind::Retrying {
                            chunk: position,
                            attempt: attempt + 1,
                            error: error.to_string(),
                        },
                        &job,
                    )
                    .await;
                    self.backoff(attempt, &error, cancel).await?;
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
        let response = self
            .request(
                &client,
                &url,
                range.as_deref(),
                if ranged { validator.as_deref() } else { None },
                cancel,
            )
            .await?;
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
        let _checkpoint = self.0.checkpoint.lock().await;
        let mut record = job.record.write().await;
        let chunk = chunk_at_mut(&mut record, index)?;
        chunk.end = response.content_length();
        chunk.written = 0;
        record.total_bytes = response.content_length();
        job.adaptive.reset(&record, self.0.rate_limiter.limit());
        drop(record);
        self.checkpoint_locked().await?;
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
        let mut body = ChunkBody::open(self, job, key, offset, request_end).await?;
        let result = body.receive(response, cancel).await;
        body.account_discarded();
        result
    }
}

fn paths_for_partial(record: &Record) -> Result<()> {
    crate::paths::staging_files(record)?
        .ok_or_else(|| Error::Invalid("staging directory is unavailable".into()))?;
    Ok(())
}

/// One response's file buffer and offsets; the HTTP request end remains fixed
/// while the job's owned end may shrink during streaming.
struct ChunkBody<'a> {
    manager: &'a Manager,
    job: &'a Job,
    key: u64,
    file: fs::File,
    pending: BytesMut,
    last_flush: Instant,
    offset: u64,
    request_end: Option<u64>,
    received: u64,
    position: u64,
}

impl<'a> ChunkBody<'a> {
    async fn open(
        manager: &'a Manager,
        job: &'a Job,
        key: u64,
        offset: u64,
        request_end: Option<u64>,
    ) -> Result<Self> {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(&job.part_path)
            .await?;
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        Ok(Self {
            manager,
            job,
            key,
            file,
            pending: BytesMut::with_capacity(BUFFER_SIZE),
            last_flush: Instant::now(),
            offset,
            request_end,
            received: 0,
            position: offset,
        })
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
        self.job
            .adaptive
            .received(self.key, self.offset + self.received);
        Ok(())
    }

    // Returns true as soon as a shortened ownership boundary is reached.
    async fn write_frame(&mut self, bytes: &mut Bytes, cancel: &CancellationToken) -> Result<bool> {
        while !bytes.is_empty() {
            let wait = tokio::time::Instant::now();
            let grant = self
                .manager
                .0
                .rate_limiter
                .acquire(bytes.len(), cancel)
                .await?;
            let piece = bytes.split_to(grant);
            let end = self.job.adaptive.end(self.key);
            let useful = end.map_or(grant, |end| {
                end.saturating_sub(self.position).min(grant as u64) as usize
            });
            self.job
                .adaptive
                .observe(self.key, useful as u64, wait.elapsed());
            let useful_bytes = piece
                .get(..useful)
                .ok_or_else(|| Error::Worker("body slice exceeds frame length".into()))?;
            buffer_body(
                &mut self.file,
                self.job,
                self.key,
                &mut self.pending,
                useful_bytes,
                &mut self.last_flush,
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
        write_pending(&mut self.file, self.job, self.key, &mut self.pending).await
    }

    fn account_discarded(&self) {
        if let Some(end) = self.job.adaptive.end(self.key)
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
) -> Result<()> {
    while !bytes.is_empty() {
        let size = bytes.len().min(BUFFER_SIZE - pending.len());
        let (piece, rest) = bytes
            .split_at_checked(size)
            .ok_or_else(|| Error::Worker("body buffer split exceeds frame length".into()))?;
        pending.extend_from_slice(piece);
        bytes = rest;
        if pending.len() == BUFFER_SIZE {
            write_pending(file, job, key, pending).await?;
            *last_flush = Instant::now();
        }
    }
    if last_flush.elapsed() >= Duration::from_millis(250) {
        write_pending(file, job, key, pending).await?;
        *last_flush = Instant::now();
    }
    Ok(())
}

pub(super) async fn write_pending(
    file: &mut fs::File,
    job: &Job,
    key: u64,
    buffer: &mut BytesMut,
) -> Result<()> {
    if !buffer.is_empty() {
        // Validate the stable key BEFORE writing, including the error path.
        chunk_at(&*job.record.read().await, key)?;
        let size = job.adaptive.reserve(key, buffer.len())?;
        let bytes = buffer
            .get(..size)
            .ok_or_else(|| Error::Worker("write reservation exceeds buffer length".into()))?;
        file.write_all(bytes).await?;
        file.flush().await?;
        // Keep the reservation visible through the awaited progress update.
        chunk_at_mut(&mut *job.record.write().await, key)?.written += size as u64;
        job.adaptive.written(key, size);
        #[cfg(test)]
        if job.adaptive.is_child(key) {
            super::adaptive::tests::hook("after-child-write");
        }
        buffer.clear();
    }
    Ok(())
}
fn chunk_at(record: &Record, key: u64) -> Result<&Chunk> {
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
fn missing_chunk(key: u64) -> Error {
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
mod tests {
    use super::*;
    use crate::engine::Config;

    async fn staged_job() -> (tempfile::TempDir, Manager, Arc<Job>) {
        let dir = tempfile::tempdir().unwrap();
        let manager = Manager::open(Config {
            output_dir: dir.path().into(),
            home_dir: Some(dir.path().join("test-home")),
            ..Config::default()
        })
        .await
        .unwrap();
        let id = Uuid::new_v4();
        let job = Arc::new(Job::new(
            Record {
                id,
                url: "http://127.0.0.1:1/unused".into(),
                filename: "payload.bin".into(),
                output_path: manager.0.config.output_dir.join("payload.bin"),
                part_path: manager
                    .0
                    .config
                    .output_dir
                    .join(".vise")
                    .join(format!("{id}.part")),
                proxy_url: None,
                cookies_file: None,
                request_credentials: None,
                status: Status::Downloading,
                total_bytes: Some(3),
                ranged: true,
                validator: Some("test".into()),
                chunks: vec![Chunk {
                    start: 0,
                    end: Some(3),
                    written: 0,
                    status: ChunkStatus::Queued,
                }],
                error: None,
                checksums: Default::default(),
                speed: 0.0,
                created_at: timestamp_millis(),
            },
            reqwest::Client::new(),
        ));
        (dir, manager, job)
    }

    #[tokio::test]
    async fn fresh_staging_creation_waits_for_the_capture_gate() {
        let (_root, manager, template) = staged_job().await;
        let record = template.record.read().await.clone();
        let job = Arc::new(Job::fresh(record.clone(), reqwest::Client::new()));
        manager.0.jobs.write().await.insert(record.id, job.clone());
        manager.checkpoint().await.unwrap();
        assert!(!job.staged.load(std::sync::atomic::Ordering::Acquire));
        let checkpoint = manager.0.checkpoint.lock().await;
        let mut creation = Box::pin(manager.prepare_partial(
            &job,
            Metadata {
                total: Some(3),
                ranged: true,
                validator: Some("new".into()),
                expected: Default::default(),
            },
        ));
        assert!(futures_util::poll!(creation.as_mut()).is_pending());
        assert!(
            !job.part_path.exists(),
            "creation bypassed the capture gate"
        );
        assert!(!job.staged.load(std::sync::atomic::Ordering::Acquire));
        drop(checkpoint);
        creation.await.unwrap();
        assert!(job.staged.load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(std::fs::metadata(&job.part_path).unwrap().len(), 3);
        manager.shutdown().await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn first_open_of_fresh_staging_rejects_a_substituted_symlink() {
        let (root, manager, template) = staged_job().await;
        let record = template.record.read().await.clone();
        let job = Arc::new(Job::fresh(record.clone(), reqwest::Client::new()));
        manager.0.jobs.write().await.insert(record.id, job.clone());
        manager.checkpoint().await.unwrap();
        let unrelated = root.path().join("unrelated");
        std::fs::write(&unrelated, b"keep").unwrap();
        std::os::unix::fs::symlink(&unrelated, &job.part_path).unwrap();
        assert!(
            manager
                .prepare_partial(
                    &job,
                    Metadata {
                        total: Some(3),
                        ranged: true,
                        validator: Some("new".into()),
                        expected: Default::default()
                    }
                )
                .await
                .is_err()
        );
        assert_eq!(std::fs::read(&unrelated).unwrap(), b"keep");
        assert!(!job.staged.load(std::sync::atomic::Ordering::Acquire));
        std::fs::remove_file(&job.part_path).unwrap();
        manager.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn reset_does_not_truncate_before_durable_offsets() {
        let (_dir, manager, job) = staged_job().await;
        job.record.write().await.chunks[0].written = 3;
        manager
            .0
            .jobs
            .write()
            .await
            .insert(job.record.read().await.id, job.clone());
        fs::write(&job.part_path, b"abc").await.unwrap();
        manager.checkpoint().await.unwrap();
        let (entered, ready) = tokio::sync::oneshot::channel();
        let (release, wait) = tokio::sync::oneshot::channel();
        *manager.0.checkpoint_hook.lock().await = Some((entered, wait));
        let worker = manager.clone();
        let task = job.clone();
        let pending = tokio::spawn(async move {
            worker
                .prepare_partial(
                    &task,
                    Metadata {
                        total: Some(5),
                        ranged: true,
                        validator: Some("new".into()),
                        expected: Default::default(),
                    },
                )
                .await
        });
        ready.await.unwrap();
        assert_eq!(
            manager.0.database.load().await.unwrap()[0].chunks[0].written,
            3
        );
        assert_eq!(
            fs::read(&job.part_path).await.unwrap(),
            b"abc",
            "truncate preceded durable offset invalidation"
        );
        release.send(()).unwrap();
        pending.await.unwrap().unwrap();
        assert_eq!(
            manager.0.database.load().await.unwrap()[0].chunks[0].written,
            0
        );
        assert_eq!(fs::metadata(&job.part_path).await.unwrap().len(), 5);
        manager.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn missing_chunks_fail_workers_and_do_not_commit_progress() {
        let (_dir, manager, job) = staged_job().await;
        let cancel = CancellationToken::new();
        for index in [1, u64::MAX] {
            assert!(
                matches!(manager.transfer_chunk(job.clone(), index, &cancel).await, Err(Error::Worker(message)) if message.contains("missing chunk"))
            );
            assert!(
                matches!(manager.transfer_once(&job, index, &cancel).await, Err(Error::Worker(message)) if message.contains("missing chunk"))
            );
        }
        assert!(!job.part_path.exists());
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&job.part_path)
            .await
            .unwrap();
        let mut pending = BytesMut::from(b"abc".as_slice());
        assert!(matches!(
            write_pending(&mut file, &job, 1, &mut pending).await,
            Err(Error::Worker(_))
        ));
        assert_eq!(pending.as_ref(), b"abc");
        assert_eq!(chunk_at(&*job.record.read().await, 0).unwrap().written, 0);
        manager.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn publication_finishes_staging_rename_before_returning() {
        let (_dir, manager, job) = staged_job().await;
        manager
            .0
            .jobs
            .write()
            .await
            .insert(job.record.read().await.id, job.clone());
        chunk_at_mut(&mut *job.record.write().await, 0)
            .unwrap()
            .written = 3;
        let mut file = fs::File::create(&job.part_path).await.unwrap();
        file.write_all(b"abc").await.unwrap();
        manager
            .publish_file(&job, file, job.part_path.clone(), &CancellationToken::new())
            .await
            .unwrap();
        // No polling here: publish_file must await this rename, independently of
        // queue scheduling, SQL completion, or any later cleanup task.
        assert!(!job.part_path.try_exists().unwrap());
        assert_eq!(fs::read(&job.output_path).await.unwrap(), b"abc");
        assert!(
            same_file::is_same_file(
                &job.output_path,
                super::super::journal::publication_path(&job.part_path)
            )
            .unwrap()
        );
        assert_eq!(
            manager
                .0
                .database
                .list()
                .await
                .unwrap()
                .first()
                .unwrap()
                .status,
            Status::Downloading
        );
        manager.shutdown().await.unwrap();
    }
}
