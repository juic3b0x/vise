use super::Manager;
use crate::{
    error::{Error, Result},
    model::{Record, Status},
    paths,
};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
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
        let jobs: Vec<_> = self.0.jobs.read().await.values().cloned().collect();
        let mut downloads = Vec::with_capacity(jobs.len());
        for job in &jobs {
            downloads.push(job.record.read().await.clone());
        }
        // Capture offsets BEFORE syncing files: every saved offset refers to durable bytes.
        let (downloads, directories) =
            tokio::task::spawn_blocking(move || sync_partials(downloads))
                .await
                .map_err(|error| Error::Worker(error.to_string()))??;
        for directory in directories {
            // Missing removable output volumes must not block unrelated tasks.
            match sync_directory(directory).await {
                Err(crate::error::Error::Io(error))
                    if error.kind() == std::io::ErrorKind::NotFound => {}
                result => result?,
            }
        }
        self.0.database.persist(downloads).await?;
        sync_directory(super::storage::parent(self.0.database.path())?.to_path_buf()).await?;
        Ok(())
    }
}

fn sync_partials(
    downloads: Vec<Record>,
) -> Result<(Vec<Record>, std::collections::BTreeSet<PathBuf>)> {
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
        assert!(sync_directory(root.join("..")).await.is_err());
        assert!(sync_directory(PathBuf::from("relative")).await.is_err());
    }
}
