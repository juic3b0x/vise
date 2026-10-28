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
        // Lock order: settlement -> checkpoint -> record. Checkpoints deliberately
        // read raw records; public snapshots wait until commit/failure resolution
        // and publication-evidence cleanup have finished.
        let _settlement = job.settlement.write().await;
        let checkpoint = self.0.checkpoint.lock().await;
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
        let committed = if let Err(error) = self.checkpoint_locked().await {
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
            self.publish(
                EventKind::Warning {
                    message: format!("cannot persist state: {error}"),
                },
                Some(event.1.clone()),
            );
            false
        } else {
            true
        };
        // No other checkpoint may capture the provisional Completed status on
        // a failed commit. Release only after it has been corrected to Failed.
        drop(checkpoint);
        if committed
            && event.0 == EventKind::Completed
            && let Err(error) = remove_publication_evidence(job).await
        {
            tracing::warn!(%id, %error, "cannot remove publication recovery link");
            self.publish(
                EventKind::Warning {
                    message: format!("cannot remove publication recovery link: {error}"),
                },
                Some(event.1.clone()),
            );
        }
        self.publish(event.0, Some(event.1));
        self.notify();
    }
}

async fn remove_publication_evidence(job: &Job) -> Result<()> {
    fs::remove_file(super::journal::publication_path(&job.part_path)).await?;
    super::journal::sync_directory(super::storage::parent(&job.part_path)?.to_path_buf()).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        engine::Config,
        model::{Download, Record, timestamp_millis},
    };
    use std::{sync::Arc, task::Poll, time::Duration};
    use uuid::Uuid;

    async fn published_job(manager: &Manager) -> Arc<Job> {
        let id = Uuid::new_v4();
        let output = &manager.0.config.output_dir;
        let job = Arc::new(Job::new(
            Record {
                id,
                url: "https://example.com/empty.bin".into(),
                output_path: output.join("empty.bin"),
                part_path: output.join(".vise").join(format!("{id}.part")),
                filename: "empty.bin".into(),
                proxy_url: None,
                cookies_file: None,
                request_credentials: None,
                status: Status::Downloading,
                total_bytes: Some(0),
                ranged: false,
                validator: None,
                chunks: vec![],
                error: None,
                checksums: Default::default(),
                speed: 0.0,
                created_at: timestamp_millis(),
            },
            reqwest::Client::new(),
        ));
        fs::write(&job.output_path, []).await.unwrap();
        fs::hard_link(
            &job.output_path,
            super::super::journal::publication_path(&job.part_path),
        )
        .await
        .unwrap();
        manager.0.jobs.write().await.insert(id, job.clone());
        manager.checkpoint().await.unwrap();
        job
    }

    #[tokio::test]
    async fn terminal_snapshots_wait_for_successful_or_failed_commit() {
        for reject in [false, true] {
            check_blocked_commit(reject).await;
        }
    }

    async fn check_blocked_commit(reject: bool) {
        let root = tempfile::tempdir().unwrap();
        let manager = Manager::open(Config {
            home_dir: Some(root.path().join("home")),
            output_dir: root.path().join("output"),
            ..Config::default()
        })
        .await
        .unwrap();
        let job = published_job(&manager).await;
        let id = job.record.read().await.id;
        let connection = rusqlite::Connection::open(manager.database_path()).unwrap();
        if reject {
            connection.execute_batch("CREATE TRIGGER reject_completion BEFORE UPDATE OF status ON downloads WHEN NEW.status='completed' BEGIN SELECT RAISE(ABORT, 'injected completion failure'); END;").unwrap();
        }
        // Hold the actual journal transaction, rather than relying on a slow
        // fixture, a sleep, or scheduler load to expose the settlement window.
        connection.execute_batch("BEGIN IMMEDIATE").unwrap();
        let mut events = manager.subscribe_events();
        let worker = manager.clone();
        let finishing_job = job.clone();
        let finishing = tokio::spawn(async move { worker.finish(&finishing_job, Ok(())).await });
        tokio::time::timeout(Duration::from_secs(5), async {
            while job.record.read().await.status != Status::Completed {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let get = manager.get(id);
        let list = manager.list();
        tokio::pin!(get, list);
        assert!(
            matches!(futures_util::poll!(&mut get), Poll::Pending),
            "get exposed an uncommitted terminal state"
        );
        assert!(
            matches!(futures_util::poll!(&mut list), Poll::Pending),
            "list exposed an uncommitted terminal state"
        );
        connection.execute_batch("ROLLBACK").unwrap();
        finishing.await.unwrap();
        let snapshot = get.await.unwrap();
        let listed: Vec<Download> = list.await;
        let expected = if reject {
            Status::Failed
        } else {
            Status::Completed
        };
        assert_eq!(snapshot.status, expected);
        assert_eq!(listed.first().unwrap().status, expected);
        let saved: String = connection
            .query_row(
                "SELECT status FROM downloads WHERE id=?1",
                [id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(saved == "completed", !reject);
        assert_eq!(
            super::super::journal::publication_path(&job.part_path).exists(),
            reject
        );
        let mut completed = false;
        let mut failed = false;
        while let Ok(event) = events.try_recv() {
            completed |= event.kind == EventKind::Completed;
            failed |= event.kind == EventKind::Failed;
        }
        assert_eq!(completed, !reject);
        assert_eq!(failed, reject);
        manager.shutdown().await.unwrap();
    }
}
