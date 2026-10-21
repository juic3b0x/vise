use super::{
    Job, Manager, Run, TransferOptions,
    layout::{
        default_filename, normalize_chunks, numbered_filename, validate_filename, validate_url,
    },
};
use crate::{
    checksum::ExpectedChecksums,
    error::{Error, Result},
    events::EventKind,
    model::{Download, Record, Status, timestamp_millis},
    net,
};
use std::sync::{Arc, atomic::Ordering};
use tokio::fs;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

impl Manager {
    pub async fn add(&self, url: &str, filename: Option<&str>) -> Result<Download> {
        self.add_with_checksums(url, filename, ExpectedChecksums::default())
            .await
    }

    pub async fn add_with_checksums(
        &self,
        url: &str,
        filename: Option<&str>,
        expected: ExpectedChecksums,
    ) -> Result<Download> {
        self.add_with_options(
            url,
            filename,
            TransferOptions {
                expected,
                ..TransferOptions::default()
            },
        )
        .await
    }

    pub async fn add_with_options(
        &self,
        url: &str,
        filename: Option<&str>,
        options: TransferOptions,
    ) -> Result<Download> {
        let expected = options.expected;
        let expected = ExpectedChecksums::new(expected.sha256.as_deref(), expected.md5.as_deref())?;
        let url = validate_url(url)?;
        let proxy_url = options
            .proxy_url
            .or_else(|| self.0.config.proxy_url.clone());
        let cookies_file = options
            .cookies_file
            .or_else(|| self.0.config.cookies_file.clone());
        let (client, cookies_file) = net::client(
            self.0.config.read_timeout,
            proxy_url.as_ref(),
            cookies_file.as_deref(),
        )
        .await?;
        let _admission = self.0.admission.lock().await;
        if self.0.stopping.load(Ordering::Acquire) {
            return Err(Error::Conflict("manager is shutting down".into()));
        }
        let base = match filename {
            Some(name) => {
                validate_filename(name)?;
                name.to_owned()
            }
            None => default_filename(&url),
        };
        let mut jobs = self.0.jobs.write().await;
        let used: Vec<_> = jobs.values().cloned().collect();
        let mut names = Vec::new();
        for job in used {
            names.push(job.record.read().await.filename.clone());
        }
        let mut name = base.clone();
        let mut suffix = 1;
        while self.0.config.output_dir.join(&name).exists() || names.contains(&name) {
            if filename.is_some() {
                return Err(Error::Conflict(format!(
                    "output filename already in use: {name}"
                )));
            }
            name = numbered_filename(&base, suffix);
            suffix += 1;
        }
        let id = Uuid::new_v4();
        jobs.insert(
            id,
            Arc::new(Job::new(
                Record {
                    id,
                    url: url.into(),
                    filename: name,
                    proxy_url,
                    cookies_file,
                    status: Status::Paused,
                    total_bytes: None,
                    ranged: false,
                    validator: None,
                    chunks: vec![],
                    error: None,
                    checksums: expected.into(),
                    speed: 0.0,
                    created_at: timestamp_millis(),
                },
                client,
            )),
        );
        drop(jobs);
        if let Err(error) = self.checkpoint().await {
            self.0.jobs.write().await.remove(&id);
            return Err(error);
        }
        self.start(id, EventKind::Added, false).await?;
        self.get(id).await
    }

    pub async fn resume(&self, id: Uuid) -> Result<()> {
        self.start(id, EventKind::Resumed, false).await
    }

    /// A whole-file mismatch cannot identify the bad segment. Explicit retry invalidates every saved offset before the staging file is truncated.
    pub async fn retry(&self, id: Uuid) -> Result<()> {
        self.start(id, EventKind::Resumed, true).await
    }

    pub(super) async fn start(
        &self,
        id: Uuid,
        action: EventKind,
        retry_corrupt: bool,
    ) -> Result<()> {
        let job = self.job(id).await?;
        let _control = job.control.lock().await;
        if job.removed.load(Ordering::Acquire) {
            return Err(Error::NotFound(id));
        }
        if self.0.stopping.load(Ordering::Acquire) {
            return Err(Error::Conflict("manager is shutting down".into()));
        }
        let status = job.record.read().await.status;
        if status.active() {
            self.publish_job(action, &job).await;
            return Ok(());
        }
        if status == Status::Completed {
            return Err(Error::Conflict("download is already completed".into()));
        }
        if status == Status::Corrupt && !retry_corrupt {
            return Err(Error::Conflict(
                "download is corrupt; use retry to re-download all chunks".into(),
            ));
        }
        if let Some(run) = job.run.lock().await.take() {
            run.handle.await.map_err(|e| Error::Worker(e.to_string()))?;
        }
        let previous = {
            let mut record = job.record.write().await;
            let previous = record.clone();
            if status == Status::Corrupt {
                record.chunks.clear();
            }
            record.checksums.clear_result();
            record.status = Status::Queued;
            record.error = None;
            record.speed = 0.0;
            normalize_chunks(&mut record);
            previous
        };
        // Publish before spawning so even an empty file logs ADDED before DONE.
        if let Err(error) = self.checkpoint().await {
            *job.record.write().await = previous;
            return Err(error);
        }
        self.publish_job(action, &job).await;
        let cancel = CancellationToken::new();
        let manager = self.clone();
        let worker_job = job.clone();
        let worker_cancel = cancel.clone();
        let handle = tokio::spawn(async move {
            let result = manager
                .execute(worker_job.clone(), worker_cancel.clone())
                .await;
            let event = {
                let mut record = worker_job.record.write().await;
                let kind = match result {
                    Ok(()) => {
                        record.status = Status::Completed;
                        EventKind::Completed
                    }
                    Err(Error::Cancelled) => {
                        record.status = Status::Paused;
                        EventKind::Paused
                    }
                    Err(error @ Error::ChecksumMismatch(_)) => {
                        record.status = Status::Corrupt;
                        record.error = Some(error.to_string());
                        tracing::error!(%id, %error, "download checksum verification failed; staging file retained");
                        EventKind::Corrupt
                    }
                    Err(error) => {
                        record.status = Status::Failed;
                        record.error = Some(error.to_string());
                        EventKind::Failed
                    }
                };
                record.speed = 0.0;
                normalize_chunks(&mut record);
                (kind, record.snapshot())
            };
            manager.notify();
            if let Err(error) = manager.checkpoint().await {
                tracing::error!(%id, %error, "cannot persist download state");
                worker_job.record.write().await.error =
                    Some(format!("cannot persist state: {error}"));
                manager
                    .publish_job(
                        EventKind::Warning {
                            message: format!("cannot persist state: {error}"),
                        },
                        &worker_job,
                    )
                    .await;
            } else if worker_job.record.read().await.status == Status::Completed {
                if let Err(error) = fs::remove_file(manager.part_path(id)).await {
                    tracing::warn!(%id, %error, "cannot remove completed partial file");
                    manager
                        .publish_job(
                            EventKind::Warning {
                                message: format!("cannot remove completed partial file: {error}"),
                            },
                            &worker_job,
                        )
                        .await;
                }
            }
            manager.publish(event.0, Some(event.1));
            manager.notify();
        });
        *job.run.lock().await = Some(Run { cancel, handle });
        self.notify();
        Ok(())
    }

    /// Cancellation closes HTTP streams, then waits for every pending write to finish.
    pub async fn pause(&self, id: Uuid) -> Result<()> {
        let job = self.job(id).await?;
        let _control = job.control.lock().await;
        if job.removed.load(Ordering::Acquire) {
            return Err(Error::NotFound(id));
        }
        let status = job.record.read().await.status;
        if status == Status::Corrupt {
            return Err(Error::Conflict(
                "download is corrupt; use retry to re-download all chunks".into(),
            ));
        }
        if status == Status::Completed {
            return Err(Error::Conflict("download is already completed".into()));
        }
        self.publish_job(EventKind::PauseRequested, &job).await;
        let mut worker_reported_pause = false;
        if let Some(run) = job.run.lock().await.take() {
            run.cancel.cancel();
            run.handle.await.map_err(|e| Error::Worker(e.to_string()))?;
            worker_reported_pause = job.record.read().await.status == Status::Paused;
        }
        // A transfer may complete while cancellation is being requested.
        let mut record = job.record.write().await;
        if !matches!(record.status, Status::Completed | Status::Corrupt) {
            record.status = Status::Paused;
        }
        record.speed = 0.0;
        normalize_chunks(&mut record);
        drop(record);
        self.checkpoint().await?;
        if !worker_reported_pause && job.record.read().await.status == Status::Paused {
            self.publish_job(EventKind::Paused, &job).await;
        }
        self.notify();
        Ok(())
    }

    /// Remove a queue entry and its partial data; completed output files are kept.
    pub async fn delete(&self, id: Uuid) -> Result<()> {
        let _admission = self.0.admission.lock().await;
        if self.0.stopping.load(Ordering::Acquire) {
            return Err(Error::Conflict("manager is shutting down".into()));
        }
        let job = self.job(id).await?;
        let _control = job.control.lock().await;
        if job.removed.load(Ordering::Acquire) {
            return Err(Error::NotFound(id));
        }
        self.publish_job(EventKind::DeleteRequested, &job).await;
        if let Some(run) = job.run.lock().await.take() {
            run.cancel.cancel();
            run.handle.await.map_err(|e| Error::Worker(e.to_string()))?;
        }
        let snapshot = job.record.read().await.snapshot();
        {
            // Serialize removal with checkpoints and destructive file resets.
            let _checkpoint = self.0.checkpoint.lock().await;
            job.removed.store(true, Ordering::Release);
            self.0.jobs.write().await.remove(&id);
            if let Err(error) = self.checkpoint_locked().await {
                self.0.jobs.write().await.insert(id, job.clone());
                job.removed.store(false, Ordering::Release);
                return Err(error);
            }
        }
        match fs::remove_file(self.part_path(id)).await {
            Ok(()) => (),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
            Err(error) => self.publish(
                EventKind::Warning {
                    message: format!(
                        "queue entry removed but partial-file cleanup failed: {error}"
                    ),
                },
                Some(snapshot.clone()),
            ),
        }
        self.publish(EventKind::Deleted, Some(snapshot));
        self.notify();
        Ok(())
    }

    pub async fn resume_all(&self) -> Result<()> {
        for item in self.list().await {
            if matches!(item.status, Status::Paused | Status::Failed) {
                self.resume(item.id).await?;
            }
        }
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        let _admission = self.0.admission.lock().await;
        let _settings = self.0.settings.lock().await;
        self.0.stopping.store(true, Ordering::Release);
        // Cancel every transfer before awaiting any individual worker.
        let jobs: Vec<_> = self.0.jobs.read().await.values().cloned().collect();
        for job in &jobs {
            if let Some(run) = job.run.lock().await.as_ref() {
                run.cancel.cancel();
            }
        }
        let mut failure = None;
        for item in self.list().await {
            if let Err(error) = self.pause_for_shutdown(item).await {
                failure.get_or_insert(error);
            }
        }
        for job in jobs {
            if let Err(error) = wait_for_run(&job).await {
                failure.get_or_insert(error);
            }
        }
        if let Err(error) = self.checkpoint().await {
            failure.get_or_insert(error);
        }
        failure.map_or(Ok(()), Err)
    }

    async fn pause_for_shutdown(&self, item: Download) -> Result<()> {
        if !item.status.active() {
            return Ok(());
        }
        let Err(error) = self.pause(item.id).await else {
            return Ok(());
        };
        // Completion and corruption can win the race with a pause request.
        let settled = matches!(error, Error::Conflict(_))
            && self
                .get(item.id)
                .await
                .is_ok_and(|item| matches!(item.status, Status::Completed | Status::Corrupt));
        if settled { Ok(()) } else { Err(error) }
    }
}

async fn wait_for_run(job: &Job) -> Result<()> {
    let _control = job.control.lock().await;
    if let Some(run) = job.run.lock().await.take() {
        run.handle
            .await
            .map_err(|error| Error::Worker(error.to_string()))?;
    }
    Ok(())
}
