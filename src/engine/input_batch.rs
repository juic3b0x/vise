//! Local --input startup has atomic admission and activation stages. Daemon
//! batches and positional-only admission continue to use their existing paths.
use super::{
    Job, Manager,
    layout::{default_filename, validate_filename, validate_url},
    queue::destination,
};
use crate::{
    checksum::ExpectedChecksums,
    error::{Error, Result},
    events::EventKind,
    input,
    model::{Record, Status, timestamp_millis},
    net, paths,
};
use std::{
    collections::BTreeSet,
    sync::{Arc, atomic::Ordering},
};
use uuid::Uuid;

impl Manager {
    /// Admit the resolved startup list in two durable stages. An activation
    /// failure retains the whole admitted list paused, with no Added events or
    /// workers. This is separate from the daemon's per-entry batch contract.
    pub async fn add_input(
        &self,
        urls: Vec<String>,
        filename: Option<String>,
        expected: ExpectedChecksums,
    ) -> Result<()> {
        let urls = input::validate_urls(urls)?;
        let expected = ExpectedChecksums::new(expected.sha256.as_deref(), expected.md5.as_deref())?;
        if filename.is_some() && urls.len() != 1 {
            return Err(Error::Invalid(
                "input filename requires exactly one URL".into(),
            ));
        }
        self.validate_request_batch(&urls, &Default::default())
            .await?;
        let manager = self.clone();
        // Once state mutation starts, disconnect/cancellation cannot split a stage.
        tokio::spawn(async move { manager.admit_input(urls, filename, expected).await })
            .await
            .map_err(|error| Error::Worker(error.to_string()))?
    }

    async fn admit_input(
        &self,
        urls: Vec<String>,
        filename: Option<String>,
        expected: ExpectedChecksums,
    ) -> Result<()> {
        let context = crate::net::credentials::RequestOverrides::default()
            .resolve(&self.0.config.request)?
            .map(Arc::new);
        let mut transports = Vec::with_capacity(urls.len());
        for _ in &urls {
            transports.push(
                net::Transport::for_task(
                    &self.0.stateless_client,
                    self.0.config.read_timeout,
                    self.0.config.proxy_url.as_ref(),
                    self.0.config.cookies_file.as_deref(),
                    context.as_deref(),
                )
                .await?,
            );
        }
        let _admission = self.0.admission.lock().await;
        if self.0.stopping.load(Ordering::Acquire) {
            return Err(Error::Conflict("manager is shutting down".into()));
        }
        let jobs = self
            .prepare_input_jobs(urls, filename, expected, transports, context)
            .await?;
        // Prevent direct library controls from starting provisional new entries.
        // The coordinator never acquires these locks while holding checkpoint.
        let mut controls = Vec::with_capacity(jobs.len());
        for (_, job) in &jobs {
            controls.push(job.control.lock().await);
        }
        let _checkpoint = self.0.checkpoint.lock().await;
        {
            let mut queue = self.0.jobs.write().await;
            for (id, job) in &jobs {
                queue.insert(*id, job.clone());
            }
        }
        // A: all identities, destinations and private request settings are durable.
        if let Err(error) = self.checkpoint_locked().await {
            return self.input_failure(&jobs, false, error).await;
        }
        #[cfg(test)]
        if let Some((entered, release)) = self.0.input_hook.lock().await.take() {
            let _ = entered.send(());
            let _ = release.await;
        }
        for (_, job) in &jobs {
            job.record.write().await.status = Status::Queued;
        }
        // B: every Queued transition commits before any event or worker.
        if let Err(error) = self.checkpoint_locked().await {
            return self.input_failure(&jobs, true, error).await;
        }
        for (_, job) in &jobs {
            job.adaptive
                .reset(&*job.record.read().await, self.0.rate_limiter.limit());
            self.publish_job(EventKind::Added, job).await;
        }
        for (id, job) in &jobs {
            self.schedule(*id, job.clone()).await;
        }
        self.notify();
        Ok(())
    }

    // Caller holds admission while reserving collision-safe destinations.
    // Preparation finishes before any provisional entry reaches the queue.
    async fn prepare_input_jobs(
        &self,
        urls: Vec<String>,
        filename: Option<String>,
        expected: ExpectedChecksums,
        transports: Vec<(net::Transport, Option<std::path::PathBuf>)>,
        context: Option<Arc<net::credentials::TaskRequestContext>>,
    ) -> Result<Vec<(Uuid, Arc<Job>)>> {
        let (output, staging) =
            super::storage::admission_directories(self.0.config.output_dir.clone()).await?;
        let existing = self.0.jobs.read().await;
        let mut used = BTreeSet::new();
        let mut jobs = Vec::with_capacity(urls.len());
        for (url, (client, cookies_file)) in urls.into_iter().zip(transports) {
            let url = validate_url(&url)?;
            let base = match &filename {
                Some(name) => {
                    validate_filename(name)?;
                    name.clone()
                }
                None => default_filename(&url),
            };
            let (name, target) = destination(&output, &base, filename.is_some(), |path| {
                existing.contains_path(path) || used.contains(path)
            })
            .await?;
            used.insert(target.clone());
            let id = Uuid::new_v4();
            let record = Record {
                id,
                url: url.into(),
                filename: name,
                output_path: target,
                part_path: paths::bounded_file(&staging, &format!("{id}.part"))?,
                proxy_url: self.0.config.proxy_url.clone(),
                cookies_file,
                request_credentials: context.clone(),
                status: Status::Paused,
                total_bytes: None,
                ranged: false,
                validator: None,
                chunks: vec![],
                error: None,
                checksums: expected.clone().into(),
                speed: 0.0,
                created_at: timestamp_millis(),
            };
            jobs.push((id, Arc::new(Job::fresh(record, client))));
        }
        Ok(jobs)
    }

    // Caller retains admission, all new-job control guards and checkpoint until
    // the durable outcome is reconciled. No later capture can overwrite it.
    async fn input_failure(
        &self,
        jobs: &[(Uuid, Arc<Job>)],
        activation: bool,
        error: Error,
    ) -> Result<()> {
        let ids: BTreeSet<_> = jobs.iter().map(|(id, _)| *id).collect();
        let rows = match self.0.database.admission_snapshot().await {
            Ok(rows) => rows
                .into_iter()
                .filter(|row| ids.contains(&row.id))
                .collect::<Vec<_>>(),
            Err(_) => return input_unresolved(jobs),
        };
        if !activation && rows.is_empty() {
            let mut queue = self.0.jobs.write().await;
            for (id, job) in jobs {
                job.removed.store(true, Ordering::Release);
                queue.remove(id);
            }
            return Err(error);
        }
        if rows.len() != jobs.len()
            || rows
                .iter()
                .any(|row| !matches!(row.status, Status::Paused | Status::Queued))
        {
            return input_unresolved(jobs);
        }
        for (_, job) in jobs {
            job.record.write().await.status = Status::Paused;
        }
        // A lost B acknowledgement may have committed Queued. The read above
        // reconciled the saved cache, so this repair cannot be an idle-cache hit.
        if rows.iter().any(|row| row.status != Status::Paused)
            && self.checkpoint_locked().await.is_err()
        {
            return input_unresolved(jobs);
        }
        if super::journal::sync_directory(
            super::storage::parent(self.0.database.path())?.to_path_buf(),
        )
        .await
        .is_err()
        {
            return input_unresolved(jobs);
        }
        Err(Error::Worker(format!(
            "input batch retained paused after {} failure: {error}",
            if activation {
                "activation"
            } else {
                "admission acknowledgement"
            }
        )))
    }
}

fn input_unresolved(jobs: &[(Uuid, Arc<Job>)]) -> Result<()> {
    for (_, job) in jobs {
        job.journal_blocked.store(true, Ordering::Release);
        job.record.block_journal();
    }
    Err(Error::Worker(
        "input batch outcome is unresolved; reopen the engine before continuing".into(),
    ))
}

#[cfg(test)]
mod tests;
