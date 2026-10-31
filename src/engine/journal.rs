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
    time::Duration,
};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[cfg(test)]
pub(super) mod barriers;
pub(super) mod group;

impl Manager {
    /// Drive progress samples and crash-safe checkpoints until cancellation.
    pub async fn monitor(&self, stop: CancellationToken) -> Result<()> {
        let mut interval = tokio::time::interval(Duration::from_millis(250));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut samples: BTreeMap<Uuid, (u64, Instant)> = BTreeMap::new();
        let mut last_save = Instant::now();
        loop {
            tokio::select! { _ = stop.cancelled() => return Ok(()), _ = interval.tick() => {} }
            let ids = self.0.changes.sampling();
            let present: std::collections::BTreeSet<_> = ids.iter().copied().collect();
            samples.retain(|id, _| present.contains(id));
            let jobs: Vec<_> = {
                let jobs = self.0.jobs.read().await;
                ids.iter().filter_map(|id| jobs.get(id).cloned()).collect()
            };
            for job in &jobs {
                let record = job.record.read().await;
                let now = Instant::now();
                let bytes = record.downloaded();
                let speed = samples.insert(record.id, (bytes, now)).map(|(old, when)| {
                    bytes.saturating_sub(old) as f64
                        / now.duration_since(when).as_secs_f64().max(0.001)
                });
                drop(record);
                job.record.sample_speed(speed).await;
            }
            if !jobs.is_empty() {
                self.notify();
            }
            if last_save.elapsed() >= Duration::from_secs(1) && self.0.changes.has_pending() {
                self.checkpoint().await?;
                last_save = Instant::now();
            }
        }
    }

    pub(super) async fn checkpoint(&self) -> Result<()> {
        self.0.journal_group.submit(self.clone()).await
    }

    // Caller holds the checkpoint mutex, including around destructive resets.
    pub(super) async fn checkpoint_locked(&self) -> Result<()> {
        self.0.database.reconcile_checkpoint().await?;
        #[cfg(test)]
        self.0.journal_trace.at(barriers::Phase::Capture).await;
        let downloads = self.capture_snapshots(None).await?;
        let downloads = sync_snapshots(
            downloads,
            #[cfg(test)]
            Some(self.0.journal_trace.clone()),
        )
        .await?;
        #[cfg(test)]
        {
            let hook = self.0.checkpoint_hook.lock().await.take();
            if let Some((entered, release)) = hook {
                let _ = entered.send(());
                let _ = release.await;
            }
        }
        self.0
            .database
            .persist_delta(downloads.records.clone(), downloads.deleted.clone())
            .await?;
        #[cfg(test)]
        self.0
            .journal_trace
            .at(barriers::Phase::BeforeDirectory)
            .await;
        sync_directory(super::storage::parent(self.0.database.path())?.to_path_buf()).await?;
        #[cfg(test)]
        self.0.journal_trace.at(barriers::Phase::Directory).await;
        self.acknowledge_snapshots(&downloads);
        Ok(())
    }

    pub(super) fn acknowledge_snapshots(&self, snapshots: &Snapshots) {
        self.0.changes.acknowledge(&snapshots.generations);
    }

    pub(super) async fn capture_snapshots(&self, replacement: Option<Record>) -> Result<Snapshots> {
        self.0.database.reconcile_checkpoint().await?;
        if self
            .0
            .changes
            .blocked
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(Error::Worker(
                "cannot checkpoint an unresolved transaction; reopen the engine".into(),
            ));
        }
        let mut pending = self.0.changes.pending();
        if let Some(record) = &replacement
            && !pending.iter().any(|(id, _)| *id == record.id)
        {
            pending.push((record.id, 0));
        }
        let jobs: Vec<_> = {
            let jobs = self.0.jobs.read().await;
            pending
                .into_iter()
                .map(|(id, generation)| (id, generation, jobs.get(&id).cloned()))
                .collect()
        };
        let mut snapshots = Snapshots::default();
        for (id, generation, job) in jobs {
            let Some(job) = job else {
                snapshots.deleted.push(id);
                snapshots.generations.push((id, generation));
                continue;
            };
            #[cfg(test)]
            self.0.journal_trace.record(barriers::Phase::RecordCapture);
            let record = job.record.read().await;
            let candidate = replacement.as_ref().filter(|record| record.id == id);
            // The replacement layout was captured earlier. Never acknowledge
            // its generation using a later live-record generation.
            if candidate.is_none()
                && let Some(generation) = self.0.changes.generation(id)
            {
                snapshots.generations.push((id, generation));
            }
            let mut snapshot = candidate.unwrap_or(&record).clone();
            snapshot.speed = 0.0;
            if job.staged.load(std::sync::atomic::Ordering::Acquire) {
                snapshots.staged.insert(id);
            }
            snapshots.records.push(Arc::new(snapshot));
        }
        Ok(snapshots)
    }
}

#[derive(Default)]
pub(super) struct Snapshots {
    pub records: Vec<Arc<Record>>,
    pub deleted: Vec<Uuid>,
    generations: Vec<(Uuid, u64)>,
    staged: std::collections::BTreeSet<Uuid>,
}

// Snapshot offsets must be captured before entering this durability barrier.
pub(super) async fn sync_snapshots(
    downloads: Snapshots,
    #[cfg(test)] trace: Option<Arc<barriers::Trace>>,
) -> Result<Snapshots> {
    #[cfg(test)]
    let file_trace = trace.clone();
    let (downloads, directories) = tokio::task::spawn_blocking(move || {
        sync_partials(
            downloads,
            #[cfg(test)]
            file_trace,
        )
    })
    .await
    .map_err(|error| Error::Worker(error.to_string()))??;
    for directory in directories {
        #[cfg(test)]
        if let Some(trace) = &trace {
            trace.at(barriers::Phase::StagingDirectory).await;
        }
        match sync_directory(directory).await {
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
            result => result?,
        }
    }
    Ok(downloads)
}

fn sync_partials(
    downloads: Snapshots,
    #[cfg(test)] trace: Option<Arc<barriers::Trace>>,
) -> Result<(Snapshots, std::collections::BTreeSet<PathBuf>)> {
    let mut directories = std::collections::BTreeSet::new();
    for record in &downloads.records {
        paths::validate(record)?;
        if record.status == Status::Completed || !downloads.staged.contains(&record.id) {
            continue;
        }
        #[cfg(test)]
        if let Some(trace) = &trace {
            trace.record(barriers::Phase::StagingInspection);
        }
        let Some([part, _]) = paths::staging_files(record)? else {
            continue;
        };
        match std::fs::OpenOptions::new().write(true).open(&part) {
            Ok(file) => {
                file.sync_data()?;
                #[cfg(test)]
                if let Some(trace) = &trace {
                    trace.record(barriers::Phase::File);
                }
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
    async fn completed_history_is_not_recaptured_and_deletion_cannot_resurrect_it() {
        let (_root, manager, job) = completed_job().await;
        assert!(
            manager
                .capture_snapshots(None)
                .await
                .unwrap()
                .records
                .is_empty()
        );
        manager.checkpoint().await.unwrap();
        assert!(
            manager
                .capture_snapshots(None)
                .await
                .unwrap()
                .records
                .is_empty()
        );
        let id = job.record.read().await.id;
        manager.delete(id).await.unwrap();
        manager.checkpoint().await.unwrap();
        assert!(manager.0.database.load().await.unwrap().is_empty());
        assert!(
            manager
                .capture_snapshots(None)
                .await
                .unwrap()
                .records
                .is_empty()
        );
        manager.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn mutation_during_checkpoint_keeps_its_new_generation() {
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
    async fn failed_checkpoint_preserves_dirty_generation_and_dropped_receiver_does_not_cancel_commit()
     {
        let (_root, manager, job) = completed_job().await;
        job.record.write().await.error = Some("must be committed".into());
        let external = rusqlite::Connection::open(manager.database_path()).unwrap();
        external.execute_batch("CREATE TRIGGER reject_cache BEFORE UPDATE ON downloads BEGIN SELECT RAISE(ABORT, 'injected journal failure'); END;").unwrap();
        assert!(manager.checkpoint().await.is_err());
        assert!(manager.0.changes.has_pending());
        assert_eq!(manager.0.database.load().await.unwrap()[0].error, None);
        external.execute_batch("DROP TRIGGER reject_cache").unwrap();
        let (entered, release) = gate(&manager).await;
        let worker = manager.clone();
        let pending = tokio::spawn(async move { worker.checkpoint().await });
        entered.await.unwrap();
        pending.abort();
        assert!(pending.await.unwrap_err().is_cancelled());
        assert!(manager.0.changes.has_pending());
        assert_eq!(manager.0.database.load().await.unwrap()[0].error, None);
        release.send(()).unwrap();
        manager.checkpoint().await.unwrap();
        assert_eq!(
            manager.0.database.load().await.unwrap()[0].error.as_deref(),
            Some("must be committed")
        );
        manager.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn receipt_and_generation_acknowledgement_wait_for_journal_directory() {
        let (_root, manager, job) = completed_job().await;
        job.record.write().await.error = Some("directory barrier".into());
        let (entered, release) = manager
            .0
            .journal_trace
            .gate(barriers::Phase::BeforeDirectory);
        let worker = manager.clone();
        let mut pending = tokio::spawn(async move { worker.checkpoint().await });
        entered.await.unwrap();
        assert_eq!(
            manager.0.database.load().await.unwrap()[0].error.as_deref(),
            Some("directory barrier")
        );
        assert!(
            matches!(futures_util::poll!(&mut pending), std::task::Poll::Pending),
            "receipt escaped before journal-directory sync"
        );
        assert!(
            manager.0.changes.has_pending(),
            "generation acknowledged before journal-directory sync"
        );
        release.send(()).unwrap();
        pending.await.unwrap().unwrap();
        assert!(!manager.0.changes.has_pending());
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
    async fn clean_completed_credentials_still_check_journal_permissions() {
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
        std::fs::set_permissions(
            manager.database_path(),
            std::fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        assert!(manager.checkpoint().await.is_err());
        assert!(!manager.0.changes.has_pending());
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

    mod grouping;
    mod incremental;
}
