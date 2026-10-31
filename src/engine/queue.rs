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
    net, paths,
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
        let request_credentials = options
            .request
            .resolve(&self.0.config.request)?
            .map(Arc::new);
        if let Some(context) = &request_credentials {
            context.validate_urls(&[url.to_string()])?;
            self.0.database.check_credentials_permissions().await?;
        }
        let proxy_url = options
            .proxy_url
            .or_else(|| self.0.config.proxy_url.clone());
        let cookies_file = options
            .cookies_file
            .or_else(|| self.0.config.cookies_file.clone());
        let (client, cookies_file) = net::Transport::for_task(
            &self.0.stateless_client,
            self.0.config.read_timeout,
            proxy_url.as_ref(),
            cookies_file.as_deref(),
            request_credentials.as_deref(),
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
        let (output, staging) =
            super::storage::admission_directories(self.0.config.output_dir.clone()).await?;
        let mut jobs = self.0.jobs.write().await;
        let used: Vec<_> = jobs.values().cloned().collect();
        let mut paths = Vec::new();
        for job in used {
            paths.push(job.output_path.clone());
        }
        let (name, target) = destination(&output, &base, filename.is_some(), &paths).await?;
        let id = Uuid::new_v4();
        jobs.insert(
            id,
            Arc::new(Job::new(
                Record {
                    id,
                    url: url.into(),
                    output_path: target,
                    filename: name,
                    part_path: paths::bounded_file(&staging, &format!("{id}.part"))?,
                    proxy_url,
                    cookies_file,
                    request_credentials,
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
        let status = job.snapshot().await.status;
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
        // A new execution attempt starts here, even if its probe or cookie
        // refresh subsequently fails before a fresh layout is prepared.
        job.adaptive
            .reset(&*job.record.read().await, self.0.rate_limiter.limit());
        self.publish_job(action, &job).await;
        self.0.scheduled.write().await.insert(id);
        let cancel = CancellationToken::new();
        let manager = self.clone();
        let worker_job = job.clone();
        let worker_cancel = cancel.clone();
        let handle = tokio::spawn(async move {
            let result = manager
                .execute(worker_job.clone(), worker_cancel.clone())
                .await;
            manager.finish(&worker_job, result).await;
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
        let status = job.snapshot().await.status;
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
        let snapshot = job.adaptive.decorate(job.record.read().await.snapshot());
        let record = job.record.read().await.clone();
        let staging = tokio::task::spawn_blocking(move || paths::staging_files(&record))
            .await
            .map_err(|error| Error::Worker(error.to_string()))??;
        #[cfg(test)]
        if let Some((entered, release)) = self.0.delete_hook.lock().await.take() {
            let _ = entered.send(());
            let _ = release.await;
        }
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
        for path in staging.into_iter().flatten() {
            match fs::remove_file(path).await {
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
        self.0.journal_group.drain().await;
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

async fn destination(
    root: &std::path::Path,
    base: &str,
    explicit: bool,
    used: &[std::path::PathBuf],
) -> Result<(String, std::path::PathBuf)> {
    let mut name = base.to_owned();
    let mut suffix = 1;
    loop {
        validate_filename(&name)?;
        let target = paths::bounded_file(root, &name)?;
        if !occupied(&target).await? && !used.contains(&target) {
            return Ok((name, target));
        }
        if explicit {
            return Err(Error::Conflict(format!(
                "output filename already in use: {name}"
            )));
        }
        name = numbered_filename(base, suffix);
        suffix += 1;
    }
}

async fn occupied(path: &std::path::Path) -> Result<bool> {
    match fs::symlink_metadata(path).await {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
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
