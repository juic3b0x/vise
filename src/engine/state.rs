use super::{Job, Manager};
use crate::{
    error::{Error, Result},
    events::{Event, EventKind},
    model::{Download, Status},
    rate_limiter::{RateConfiguration, RateLimit},
};
use std::sync::{Arc, atomic::Ordering};
use tokio::sync::{broadcast, watch};
use uuid::Uuid;

impl Manager {
    pub(crate) fn subscribe(&self) -> watch::Receiver<u64> {
        self.0.events.subscribe()
    }

    pub fn rate_configuration(&self) -> RateConfiguration {
        self.0.rate_limiter.limit().into()
    }

    pub async fn set_rate_limit(&self, rate: RateLimit) -> Result<RateConfiguration> {
        let manager = self.clone();
        // A disconnected HTTP client must not cancel the apply step after the
        // SQLite transaction commits. Shutdown serializes with this operation.
        tokio::spawn(async move {
            let _settings = manager.0.settings.lock().await;
            if manager.0.stopping.load(Ordering::Acquire) {
                return Err(Error::Conflict("manager is shutting down".into()));
            }
            manager.0.database.save_rate_limit(rate).await?;
            manager.0.rate_limiter.set_limit(rate);
            for job in manager.0.jobs.read().await.values() {
                job.adaptive.reset_speeds(rate);
            }
            manager.publish(
                EventKind::RateLimitChanged {
                    bytes_per_second: rate.bytes_per_second(),
                },
                None,
            );
            manager.notify();
            Ok(manager.rate_configuration())
        })
        .await
        .map_err(|error| Error::Worker(error.to_string()))?
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<Event> {
        self.0.activity.subscribe()
    }

    pub(crate) fn publish(&self, kind: EventKind, download: Option<Download>) {
        // Sending is synchronous and never waits for terminal I/O or a subscriber.
        let _ = self.0.activity.send(Event::new(kind, download));
    }

    pub(super) async fn publish_job(&self, kind: EventKind, job: &Job) {
        self.publish(kind, Some(job.snapshot().await));
    }

    pub(super) async fn transition(&self, job: &Job, to: Status) -> Result<()> {
        let mut record = job.record.write().await;
        let from = record.status;
        record.status = to;
        let snapshot = job.adaptive.decorate(record.snapshot());
        drop(record);
        self.checkpoint().await?;
        #[cfg(test)]
        crate::database::split_test_hook("after-transition-commit");
        if from != to {
            self.publish(EventKind::StateChanged { from, to }, Some(snapshot));
        }
        self.notify();
        Ok(())
    }

    pub(crate) fn server_started(&self, address: String) {
        self.publish(EventKind::ServerStarted { address }, None);
    }

    pub fn server_stopped(&self) {
        self.publish(EventKind::ServerStopped, None);
    }

    pub fn publish_shutdown_error(&self, message: String) {
        self.publish(
            EventKind::Warning {
                message: format!("shutdown checkpoint failed: {message}"),
            },
            None,
        );
    }

    pub(super) fn notify(&self) {
        self.0.events.send_modify(|v| *v = v.wrapping_add(1));
    }

    pub fn database_path(&self) -> &std::path::Path {
        self.0.database.path()
    }

    pub async fn list(&self) -> Vec<Download> {
        let jobs: Vec<_> = self.0.jobs.read().await.values().cloned().collect();
        let mut result = Vec::with_capacity(jobs.len());
        for job in jobs {
            result.push(job.snapshot().await);
        }
        result
    }

    /// Batch exit status concerns tasks scheduled by this process, not history.
    pub(crate) async fn session_downloads(&self) -> Vec<Download> {
        let scheduled = self.0.scheduled.read().await.clone();
        let jobs: Vec<_> = {
            let jobs = self.0.jobs.read().await;
            scheduled
                .iter()
                .filter_map(|id| jobs.get(id).cloned())
                .collect()
        };
        let mut downloads = Vec::with_capacity(jobs.len());
        for job in jobs {
            downloads.push(job.snapshot().await);
        }
        downloads
    }

    pub(crate) fn session_pending(&self) -> bool {
        !self
            .0
            .running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    }

    pub(super) async fn job(&self, id: Uuid) -> Result<Arc<Job>> {
        self.0
            .jobs
            .read()
            .await
            .get(&id)
            .cloned()
            .ok_or(Error::NotFound(id))
    }

    pub async fn get(&self, id: Uuid) -> Result<Download> {
        Ok(self.job(id).await?.snapshot().await)
    }
}
