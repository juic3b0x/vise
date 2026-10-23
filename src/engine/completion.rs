//! Settle worker results only after awaited filesystem publication, then persist
//! terminal state before emitting lifecycle events.
use super::{Job, Manager, layout::normalize_chunks};
use crate::{
    error::{Error, Result},
    events::EventKind,
    model::Status,
};
use tokio::fs;

impl Manager {
    pub(super) async fn finish(&self, job: &Job, result: Result<()>) {
        let id = job.record.read().await.id;
        let mut event = {
            let mut record = job.record.write().await;
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
        if let Err(error) = self.checkpoint().await {
            tracing::error!(%id, %error, "cannot persist download state");
            let mut record = job.record.write().await;
            if record.status == Status::Completed {
                record.status = Status::Failed;
            }
            record.error = Some(format!("cannot persist state: {error}"));
            if event.0 == EventKind::Completed {
                event.0 = EventKind::Failed;
            }
            event.1 = record.snapshot();
            drop(record);
            self.publish_job(
                EventKind::Warning {
                    message: format!("cannot persist state: {error}"),
                },
                job,
            )
            .await;
        } else if job.record.read().await.status == Status::Completed
            && let Err(error) = remove_publication_evidence(job).await
        {
            tracing::warn!(%id, %error, "cannot remove publication recovery link");
            self.publish_job(
                EventKind::Warning {
                    message: format!("cannot remove publication recovery link: {error}"),
                },
                job,
            )
            .await;
        }
        self.publish(event.0, Some(event.1));
        self.notify();
    }
}

async fn remove_publication_evidence(job: &Job) -> Result<()> {
    fs::remove_file(super::journal::publication_path(&job.part_path)).await?;
    super::journal::sync_directory(super::storage::parent(&job.part_path)?.to_path_buf()).await
}
