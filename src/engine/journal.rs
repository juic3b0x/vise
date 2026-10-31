use super::Manager;
use crate::{
    error::{Error, Result},
    model::{Record, Status},
    paths,
};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

impl Manager {
    /// Drive progress samples and crash-safe checkpoints until cancellation.
    pub async fn monitor(&self, stop: CancellationToken) -> Result<()> {
        let mut interval = tokio::time::interval(Duration::from_millis(250));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut samples: BTreeMap<Uuid, (u64, Instant)> = BTreeMap::new();
        let mut last_save = Instant::now();
        loop {
            tokio::select! { _ = stop.cancelled() => return Ok(()), _ = interval.tick() => {} }
            let jobs: Vec<_> = self.0.jobs.read().await.values().cloned().collect();
            for job in jobs {
                let mut record = job.record.write().await;
                let now = Instant::now();
                let bytes = record.downloaded();
                if let Some((old, when)) = samples.insert(record.id, (bytes, now)) {
                    let speed = bytes.saturating_sub(old) as f64
                        / now.duration_since(when).as_secs_f64().max(0.001);
                    record.speed = if record.status == Status::Downloading {
                        record.speed * 0.6 + speed * 0.4
                    } else {
                        0.0
                    };
                }
            }
            self.notify();
            if last_save.elapsed() >= Duration::from_secs(1) {
                self.checkpoint().await?;
                last_save = Instant::now();
            }
        }
    }

    pub(super) async fn checkpoint(&self) -> Result<()> {
        let _guard = self.0.checkpoint.lock().await;
        self.checkpoint_locked().await
    }

    // Caller holds the checkpoint mutex, including around destructive resets.
    pub(super) async fn checkpoint_locked(&self) -> Result<()> {
        let downloads = self.capture_snapshots(None).await?;
        let downloads = sync_snapshots(downloads).await?;
        #[cfg(test)]
        {
            let hook = self.0.checkpoint_hook.lock().await.take();
            if let Some((entered, release)) = hook {
                let _ = entered.send(());
                let _ = release.await;
            }
        }
        self.0.database.persist(downloads.clone()).await?;
        sync_directory(super::storage::parent(self.0.database.path())?.to_path_buf()).await?;
        let jobs = self.0.jobs.read().await;
        for snapshot in downloads {
            if snapshot.status == Status::Completed
                && let Some(job) = jobs.get(&snapshot.id)
            {
                *job.completed_snapshot
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(snapshot);
            }
        }
        Ok(())
    }

    pub(super) async fn capture_snapshots(
        &self,
        replacement: Option<Record>,
    ) -> Result<Vec<Arc<Record>>> {
        let jobs: Vec<_> = self.0.jobs.read().await.values().cloned().collect();
        let mut downloads = Vec::with_capacity(jobs.len());
        for job in &jobs {
            if job
                .journal_blocked
                .load(std::sync::atomic::Ordering::Acquire)
            {
                return Err(Error::Worker(
                    "cannot checkpoint an unresolved split transaction; reopen the engine".into(),
                ));
            }
            let record = job.record.read().await;
            let candidate = replacement
                .as_ref()
                .filter(|r| r.id == record.id)
                .unwrap_or(&record);
            let cached = job
                .completed_snapshot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let snapshot = cached.as_ref().filter(|saved| {
                candidate.status == Status::Completed && saved.as_ref() == candidate
            });
            downloads.push(
                snapshot
                    .cloned()
                    .unwrap_or_else(|| Arc::new(candidate.clone())),
            );
        }
        Ok(downloads)
    }
}

// Snapshot offsets must be captured before entering this durability barrier.
pub(super) async fn sync_snapshots(downloads: Vec<Arc<Record>>) -> Result<Vec<Arc<Record>>> {
    let (downloads, directories) = tokio::task::spawn_blocking(move || sync_partials(downloads))
        .await
        .map_err(|error| Error::Worker(error.to_string()))??;
    for directory in directories {
        match sync_directory(directory).await {
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
            result => result?,
        }
    }
    Ok(downloads)
}

fn sync_partials(
    downloads: Vec<Arc<Record>>,
) -> Result<(Vec<Arc<Record>>, std::collections::BTreeSet<PathBuf>)> {
    let mut directories = std::collections::BTreeSet::new();
    for record in &downloads {
        paths::validate(record)?;
        if record.status == Status::Completed {
            continue;
        }
        let Some([part, _]) = paths::staging_files(record)? else {
            continue;
        };
        match std::fs::OpenOptions::new().write(true).open(&part) {
            Ok(file) => {
                file.sync_data()?;
                directories.insert(paths::parent(&part)?.to_path_buf());
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
            Err(error) => return Err(error.into()),
        }
    }
    Ok((downloads, directories))
}

/// Recovery evidence between the awaited staging rename and the SQL commit.
pub(super) fn publication_path(part: &Path) -> PathBuf {
    part.with_extension("published")
}

pub(super) async fn sync_directory(path: PathBuf) -> Result<()> {
    // Directory fsync makes renames/link creation durable on Unix. Windows does not
    // permit opening a directory as a normal File; individual files are still synced.
    tokio::task::spawn_blocking(move || {
        // Check existence, directory type, canonical identity, and traversal
        // before opening anything, on every platform (including Windows).
        let path = paths::directory(&path)?;
        #[cfg(unix)]
        std::fs::File::open(path)?.sync_all()?;
        #[cfg(not(unix))]
        drop(path);
        Ok::<_, Error>(())
    })
    .await
    .map_err(|e| Error::Worker(e.to_string()))??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn completed_job() -> (tempfile::TempDir, Manager, Arc<super::super::Job>) {
        let root = tempfile::tempdir().unwrap();
        let manager = Manager::open(super::super::Config {
            home_dir: Some(root.path().join("home")),
            output_dir: root.path().join("output"),
            ..Default::default()
        })
        .await
        .unwrap();
        let id = Uuid::new_v4();
        let record = Record {
            id,
            url: "http://127.0.0.1:1/unused".into(),
            filename: "data.bin".into(),
            output_path: manager.0.config.output_dir.join("data.bin"),
            part_path: manager
                .0
                .config
                .output_dir
                .join(".vise")
                .join(format!("{id}.part")),
            proxy_url: None,
            cookies_file: None,
            request_credentials: None,
            status: Status::Completed,
            total_bytes: Some(0),
            ranged: false,
            validator: None,
            chunks: vec![],
            error: None,
            checksums: Default::default(),
            speed: 0.0,
            created_at: crate::model::timestamp_millis(),
        };
        let job = Arc::new(super::super::Job::new(record, reqwest::Client::new()));
        manager.0.jobs.write().await.insert(id, job.clone());
        manager.checkpoint().await.unwrap();
        (root, manager, job)
    }

    async fn gate(
        manager: &Manager,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (entered, ready) = tokio::sync::oneshot::channel();
        let (release, wait) = tokio::sync::oneshot::channel();
        *manager.0.checkpoint_hook.lock().await = Some((entered, wait));
        (ready, release)
    }

    #[tokio::test]
    async fn completed_history_reuses_committed_snapshots_and_deletion_cannot_resurrect_it() {
        let (_root, manager, job) = completed_job().await;
        let first = manager.capture_snapshots(None).await.unwrap();
        manager.checkpoint().await.unwrap();
        let next = manager.capture_snapshots(None).await.unwrap();
        assert!(Arc::ptr_eq(&first[0], &next[0]));
        let id = job.record.read().await.id;
        manager.delete(id).await.unwrap();
        manager.checkpoint().await.unwrap();
        assert!(manager.0.database.load().await.unwrap().is_empty());
        assert!(manager.capture_snapshots(None).await.unwrap().is_empty());
        manager.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn mutation_during_checkpoint_is_not_hidden_by_completed_cache() {
        let (_root, manager, job) = completed_job().await;
        job.record.write().await.error = Some("captured".into());
        let (entered, release) = gate(&manager).await;
        let worker = manager.clone();
        let pending = tokio::spawn(async move { worker.checkpoint().await });
        entered.await.unwrap();
        job.record.write().await.error = Some("newer mutation".into());
        release.send(()).unwrap();
        pending.await.unwrap().unwrap();
        assert_eq!(
            manager.0.database.load().await.unwrap()[0].error.as_deref(),
            Some("captured")
        );
        manager.checkpoint().await.unwrap();
        assert_eq!(
            manager.0.database.load().await.unwrap()[0].error.as_deref(),
            Some("newer mutation")
        );
        manager.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn failed_or_cancelled_checkpoint_does_not_advance_completed_cache() {
        let (_root, manager, job) = completed_job().await;
        let saved = manager.capture_snapshots(None).await.unwrap().remove(0);
        job.record.write().await.error = Some("must be committed".into());
        let external = rusqlite::Connection::open(manager.database_path()).unwrap();
        external.execute_batch("CREATE TRIGGER reject_cache BEFORE UPDATE ON downloads BEGIN SELECT RAISE(ABORT, 'injected journal failure'); END;").unwrap();
        assert!(manager.checkpoint().await.is_err());
        assert!(Arc::ptr_eq(
            job.completed_snapshot.lock().unwrap().as_ref().unwrap(),
            &saved
        ));
        assert_eq!(manager.0.database.load().await.unwrap()[0].error, None);
        external.execute_batch("DROP TRIGGER reject_cache").unwrap();
        let (entered, release) = gate(&manager).await;
        let worker = manager.clone();
        let pending = tokio::spawn(async move { worker.checkpoint().await });
        entered.await.unwrap();
        pending.abort();
        assert!(pending.await.unwrap_err().is_cancelled());
        drop(release);
        assert!(Arc::ptr_eq(
            job.completed_snapshot.lock().unwrap().as_ref().unwrap(),
            &saved
        ));
        assert_eq!(manager.0.database.load().await.unwrap()[0].error, None);
        manager.checkpoint().await.unwrap();
        assert_eq!(
            manager.0.database.load().await.unwrap()[0].error.as_deref(),
            Some("must be committed")
        );
        manager.shutdown().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "child entry point used by crash_between_transitions_recovers_paused"]
    async fn transition_crash_child() {
        let root = PathBuf::from(std::env::var_os("VISE_TEST_JOURNAL_ROOT").unwrap());
        let manager = Manager::open(super::super::Config {
            home_dir: Some(root.join("home")),
            output_dir: root.join("output"),
            ..Default::default()
        })
        .await
        .unwrap();
        manager
            .add("http://127.0.0.1:1/unused", None)
            .await
            .unwrap();
        std::future::pending::<()>().await;
    }

    #[tokio::test]
    async fn crash_between_transitions_recovers_paused() {
        use tokio::io::AsyncReadExt;
        let root = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let phase = "after-transition-commit";
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "engine::journal::tests::transition_crash_child",
            ])
            .env("HOME", root.path())
            .env("USERPROFILE", root.path())
            .env("VISE_TEST_JOURNAL_ROOT", root.path())
            .env("VISE_TEST_SPLIT_PHASE", phase)
            .env(
                "VISE_TEST_SPLIT_ACK",
                listener.local_addr().unwrap().to_string(),
            )
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let acknowledgement = tokio::time::timeout(Duration::from_secs(10), async {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = vec![0; phase.len()];
            socket.read_exact(&mut bytes).await.unwrap();
            assert_eq!(bytes, phase.as_bytes());
        })
        .await;
        child.kill().unwrap();
        child.wait().unwrap();
        acknowledgement.unwrap();
        let sql = rusqlite::Connection::open(root.path().join("home/.vise/state.sqlite3")).unwrap();
        let status: String = sql
            .query_row("SELECT status FROM downloads", [], |row| row.get(0))
            .unwrap();
        assert_eq!(status, "probing");
        drop(sql);
        let manager = Manager::open(super::super::Config {
            home_dir: Some(root.path().join("home")),
            output_dir: root.path().join("output"),
            ..Default::default()
        })
        .await
        .unwrap();
        let items = manager.list().await;
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].status, Status::Paused);
        assert_eq!(
            manager.0.database.load().await.unwrap()[0].status,
            Status::Paused
        );
        manager.shutdown().await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cached_completed_credentials_still_check_journal_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let (_root, manager, job) = completed_job().await;
        let context = crate::net::credentials::RequestOverrides::from_cli(
            &["X-Secret: test-only".into()],
            None,
            None,
        )
        .unwrap()
        .resolve(&Default::default())
        .unwrap()
        .unwrap();
        job.record.write().await.request_credentials = Some(Arc::new(context));
        manager.checkpoint().await.unwrap();
        let saved = manager.capture_snapshots(None).await.unwrap().remove(0);
        std::fs::set_permissions(
            manager.database_path(),
            std::fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        assert!(manager.checkpoint().await.is_err());
        assert!(Arc::ptr_eq(
            job.completed_snapshot.lock().unwrap().as_ref().unwrap(),
            &saved
        ));
        std::fs::set_permissions(
            manager.database_path(),
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        manager.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn directory_sync_rejects_missing_files_and_traversal() {
        let temp = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(temp.path()).unwrap();
        sync_directory(root.clone()).await.unwrap();
        assert!(matches!(sync_directory(root.join("missing")).await,
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound));
        std::fs::write(root.join("file"), b"keep").unwrap();
        assert!(matches!(sync_directory(root.join("file")).await,
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotADirectory));
        let traversal = paths::unvalidated_path(&root, "..");
        assert!(
            traversal
                .components()
                .any(|part| part == std::path::Component::ParentDir)
        );
        assert!(matches!(
            sync_directory(traversal).await,
            Err(Error::Invalid(_))
        ));
        assert!(sync_directory(PathBuf::from("relative")).await.is_err());
    }
}
