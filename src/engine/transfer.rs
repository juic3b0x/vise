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
use bytes::BytesMut;
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
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
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
        self.transition(&job, Status::Downloading).await?;
        self.run_chunks(&job, &cancel).await?;
        self.publish_file(&job, file, path, &cancel).await
    }

    async fn refresh_cookies(&self, job: &Job) -> Result<()> {
        let (path, proxy) = {
            let record = job.record.read().await;
            (record.cookies_file.clone(), record.proxy_url.clone())
        };
        if let Some(path) = path {
            let (client, _) =
                net::client(self.0.config.read_timeout, proxy.as_ref(), Some(&path)).await?;
            *job.client.write().await = client;
        }
        Ok(())
    }

    async fn prepare_partial(&self, job: &Job, metadata: Metadata) -> Result<(fs::File, PathBuf)> {
        validate_size(metadata.total)?;
        let id = job.record.read().await.id;
        let path = self.part_path(id);
        let file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .await?;
        let _checkpoint = self.0.checkpoint.lock().await;
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

    async fn run_chunks(&self, job: &Arc<Job>, cancel: &CancellationToken) -> Result<()> {
        let mut workers = JoinSet::new();
        let worker_cancel = cancel.child_token();
        let count = job.record.read().await.chunks.len();
        for index in 0..count {
            let manager = self.clone();
            let job = job.clone();
            let cancel = worker_cancel.clone();
            workers.spawn(async move { manager.transfer_chunk(job, index, &cancel).await });
        }
        let mut failure = None;
        while let Some(result) = workers.join_next().await {
            let result = result.unwrap_or_else(|e| Err(Error::Worker(e.to_string())));
            if let Err(error) = result {
                if failure.is_none() {
                    failure = Some(error);
                    worker_cancel.cancel();
                }
            }
        }
        // Do not abort tasks: cancellation waits for file writes and offset updates.
        if let Some(error) = failure {
            return Err(error);
        }
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        Ok(())
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
        let target = self.0.config.output_dir.join(&record.filename);
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
        fs::hard_link(&path, &target).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                Error::Conflict(format!("output already exists: {}", target.display()))
            } else {
                e.into()
            }
        })?;
        sync_directory(self.0.config.output_dir.clone()).await?;
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
        index: usize,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let mut attempt = 0;
        loop {
            if cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }
            chunk_at_mut(&mut *job.record.write().await, index)?.status = ChunkStatus::Downloading;
            let result = self.transfer_once(&job, index, cancel).await;
            match result {
                Ok(()) => {
                    chunk_at_mut(&mut *job.record.write().await, index)?.status =
                        ChunkStatus::Completed;
                    return Ok(());
                }
                Err(error) if error.retryable() && attempt < self.0.config.retries => {
                    tracing::warn!(chunk = index, retry = attempt + 1, %error, "retrying transfer");
                    self.publish_job(
                        EventKind::Retrying {
                            chunk: index,
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
        index: usize,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let (id, url, ranged, validator, chunk) = {
            let record = job.record.read().await;
            (
                record.id,
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
            self.reset_stream(job, index, &response, id).await?;
        }
        self.learn_checksums(job, &response).await?;
        self.stream_chunk(job, index, response, offset, cancel)
            .await
    }

    async fn reset_stream(
        &self,
        job: &Job,
        index: usize,
        response: &Response,
        id: Uuid,
    ) -> Result<()> {
        validate_size(response.content_length())?;
        let _checkpoint = self.0.checkpoint.lock().await;
        let mut record = job.record.write().await;
        let chunk = chunk_at_mut(&mut record, index)?;
        chunk.end = response.content_length();
        chunk.written = 0;
        record.total_bytes = response.content_length();
        drop(record);
        self.checkpoint_locked().await?;
        fs::OpenOptions::new()
            .write(true)
            .open(self.part_path(id))
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

    async fn stream_chunk(
        &self,
        job: &Job,
        index: usize,
        response: Response,
        offset: u64,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let (id, expected) = {
            let record = job.record.read().await;
            (
                record.id,
                chunk_at(&record, index)?.end.map(|end| end - offset),
            )
        };
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(self.part_path(id))
            .await?;
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        let mut stream = self
            .0
            .rate_limiter
            .throttle(response.bytes_stream(), cancel.clone())
            .boxed();
        let mut pending = BytesMut::with_capacity(BUFFER_SIZE);
        let mut last_flush = Instant::now();
        let mut received = 0u64;
        loop {
            let next = tokio::select! { biased; _ = cancel.cancelled() => return Err(Error::Cancelled), value = stream.next() => value };
            match next {
                Some(Ok(bytes)) => {
                    received = received
                        .checked_add(bytes.len() as u64)
                        .ok_or_else(|| Error::Protocol("response is too large".into()))?;
                    if expected.is_some_and(|length| received > length) {
                        return Err(Error::Protocol("response exceeds requested length".into()));
                    }
                    // Bound the write buffer even when a peer sends very large frames.
                    let mut remaining = bytes.as_ref();
                    while !remaining.is_empty() {
                        let size = remaining.len().min(BUFFER_SIZE - pending.len());
                        let (piece, rest) = remaining.split_at_checked(size).ok_or_else(|| {
                            Error::Worker("response buffer split exceeds frame length".into())
                        })?;
                        pending.extend_from_slice(piece);
                        remaining = rest;
                        if pending.len() == BUFFER_SIZE {
                            write_pending(&mut file, job, index, &mut pending).await?;
                            last_flush = Instant::now();
                        }
                    }
                    // Keep low-bandwidth progress/checkpoints responsive without
                    // forcing tiny disk writes on every token grant.
                    if last_flush.elapsed() >= Duration::from_millis(250) {
                        write_pending(&mut file, job, index, &mut pending).await?;
                        last_flush = Instant::now();
                    }
                }
                Some(Err(error)) => {
                    write_pending(&mut file, job, index, &mut pending).await?;
                    return Err(error);
                }
                None => break,
            }
        }
        write_pending(&mut file, job, index, &mut pending).await?;
        if expected.is_some_and(|length| received != length) {
            return Err(Error::Protocol("unexpected end of response body".into()));
        }
        Ok(())
    }
}

pub(super) async fn write_pending(
    file: &mut fs::File,
    job: &Job,
    index: usize,
    buffer: &mut BytesMut,
) -> Result<()> {
    if !buffer.is_empty() {
        file.write_all(buffer).await?;
        // Tokio files buffer the blocking write; flush ensures it finished before accounting.
        file.flush().await?;
        chunk_at_mut(&mut *job.record.write().await, index)?.written += buffer.len() as u64;
        buffer.clear();
    }
    Ok(())
}

fn chunk_at(record: &Record, index: usize) -> Result<&Chunk> {
    record
        .chunks
        .get(index)
        .ok_or_else(|| missing_chunk(record.id, index))
}

fn chunk_at_mut(record: &mut Record, index: usize) -> Result<&mut Chunk> {
    let id = record.id;
    record
        .chunks
        .get_mut(index)
        .ok_or_else(|| missing_chunk(id, index))
}

fn missing_chunk(id: Uuid, index: usize) -> Error {
    Error::Worker(format!("missing chunk {index} for download {id}"))
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
    if let (Some(expected), Some(actual)) = (expected, super::http::validator(response)) {
        if expected != actual {
            return Err(Error::Protocol(
                "remote file validator changed; resume to reprobe".into(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Config;

    #[tokio::test]
    async fn missing_chunks_fail_workers_and_do_not_commit_progress() {
        let dir = tempfile::tempdir().unwrap();
        let manager = Manager::open(Config {
            output_dir: dir.path().into(),
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
                proxy_url: None,
                cookies_file: None,
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
        let cancel = CancellationToken::new();
        for index in [1, usize::MAX] {
            assert!(
                matches!(manager.transfer_chunk(job.clone(), index, &cancel).await, Err(Error::Worker(message)) if message.contains("missing chunk"))
            );
            assert!(
                matches!(manager.transfer_once(&job, index, &cancel).await, Err(Error::Worker(message)) if message.contains("missing chunk"))
            );
        }
        assert!(!manager.part_path(id).exists());
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(manager.part_path(id))
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
}
