use super::{
    Config, Inner, Job, Manager,
    journal::sync_directory,
    layout::{normalize_chunks, validate_record},
};
use crate::{
    checksum,
    database::{Database, FILE_NAME},
    error::{Error, Result},
    model::{Record, Status, timestamp_millis},
    net,
    rate_limiter::{RateLimit, RateLimiter},
};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicBool},
};
use tokio::{
    fs,
    sync::{Mutex, RwLock, Semaphore, broadcast, watch},
};
use tokio_util::sync::CancellationToken;

impl Manager {
    pub async fn open(mut config: Config) -> Result<Self> {
        validate_config(&config)?;
        config.request.resolve(&Default::default())?;
        if let Some(path) = config.cookies_file.take() {
            config.cookies_file = Some(net::cookies::load(path).await?.path);
        }
        let (state_dir, lock) = super::storage::prepare(&mut config).await?;
        let mut output_locks =
            super::storage::lock_outputs(std::collections::BTreeSet::from([config
                .output_dir
                .clone()]))
            .await?;
        let database = Database::open(state_dir.join(FILE_NAME)).await?;
        super::migration::import_selected(&database, &config.output_dir).await?;
        let directories = database
            .load()
            .await?
            .iter()
            .map(|record| super::storage::parent(&record.output_path).map(Path::to_path_buf))
            .collect::<Result<std::collections::BTreeSet<_>>>()?;
        output_locks.extend(
            super::storage::lock_outputs(
                directories
                    .into_iter()
                    .filter(|path| path != &config.output_dir)
                    .collect(),
            )
            .await?,
        );
        let rate = restore_rate(&database, config.limit_rate).await?;
        let Recovered {
            jobs,
            completed_parts,
            corrupt_targets,
        } = restore_jobs(&database, &config).await?;
        let (events, _) = watch::channel(0);
        let (activity, _) = broadcast::channel(2048);
        let manager = Self(Arc::new(Inner {
            pool: Semaphore::new(config.concurrent_downloads),
            config,
            database,
            rate_limiter: RateLimiter::new(rate),
            settings: Mutex::new(()),
            jobs: RwLock::new(jobs),
            scheduled: RwLock::new(std::collections::BTreeSet::new()),
            checkpoint: Mutex::new(()),
            admission: Mutex::new(()),
            events,
            activity,
            stopping: AtomicBool::new(false),
            _global_lock: lock,
            _output_locks: output_locks,
        }));
        manager.checkpoint().await?;
        // Persist recovery before removing hard-link evidence or corrupt targets.
        cleanup_recovered(completed_parts, corrupt_targets).await?;
        Ok(manager)
    }
}

async fn recover_record(
    mut record: Record,
    completed_parts: &mut Vec<PathBuf>,
    corrupt_targets: &mut Vec<PathBuf>,
) -> Result<Record> {
    validate_record(&record)?;
    let part = record.part_path.clone();
    let target = record.output_path.clone();
    if !super::storage::parent(&target)?.is_dir() {
        if !matches!(record.status, Status::Completed | Status::Corrupt) {
            record.status = Status::Failed;
            record.error =
                Some("destination directory is unavailable; saved offsets retained".into());
        }
        return Ok(record);
    }
    restore_publication_evidence(&part, completed_parts).await?;
    recover_publication(&mut record, &part, &target, corrupt_targets).await?;
    if record.status == Status::Completed {
        recover_completed(&mut record, &part, &target, completed_parts);
    } else {
        recover_partial(&mut record, &part).await?;
    }
    normalize_chunks(&mut record);
    Ok(record)
}

async fn restore_publication_evidence(part: &Path, cleanup: &mut Vec<PathBuf>) -> Result<()> {
    let evidence = super::journal::publication_path(part);
    // Restore the original staging name for the existing same-file and checksum
    // recovery checks. Keep evidence until the recovered state commits to SQLite.
    match fs::hard_link(&evidence, part).await {
        Ok(()) => (),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if !evidence.try_exists()? {
                return Ok(());
            }
            if !same_file::is_same_file(&evidence, part)? {
                return Err(Error::Conflict(format!(
                    "publication recovery link differs from staging file: {}",
                    part.display()
                )));
            }
        }
        Err(error) => return Err(error.into()),
    }
    cleanup.push(evidence);
    Ok(())
}

async fn recover_publication(
    record: &mut Record,
    part: &Path,
    target: &Path,
    corrupt_targets: &mut Vec<PathBuf>,
) -> Result<()> {
    if record.status == Status::Completed || !target.exists() || !part.exists() {
        return Ok(());
    }
    if !same_file::is_same_file(target, part)? {
        return Ok(());
    }
    verify_recovered(record, part).await?;
    if record.status == Status::Corrupt {
        corrupt_targets.push(target.to_path_buf());
    } else {
        record.status = Status::Completed;
    }
    Ok(())
}

async fn verify_recovered(record: &mut Record, part: &Path) -> Result<()> {
    if record.status == Status::Corrupt || record.checksums.expected().is_empty() {
        return Ok(());
    }
    record.checksums = checksum::hash_file(
        part.to_path_buf(),
        record.checksums.expected(),
        CancellationToken::new(),
    )
    .await?;
    if let Some(details) = record.checksums.mismatch() {
        record.status = Status::Corrupt;
        record.error = Some(Error::ChecksumMismatch(details).to_string());
        tracing::error!(id=%record.id, error=?record.error, "recovered file failed checksum verification");
    } else {
        record.checksums.verified_at = Some(timestamp_millis());
    }
    Ok(())
}

fn recover_completed(
    record: &mut Record,
    part: &Path,
    target: &Path,
    completed_parts: &mut Vec<PathBuf>,
) {
    if !target.is_file()
        || record
            .total_bytes
            .is_some_and(|n| target.metadata().map(|m| m.len() != n).unwrap_or(true))
    {
        record.status = Status::Failed;
        record.chunks.clear();
        record.error = Some("completed output is missing or its length has changed".into());
    } else if part.exists() {
        completed_parts.push(part.to_path_buf());
    }
}

async fn recover_partial(record: &mut Record, part: &Path) -> Result<()> {
    if record.status.active() {
        record.status = Status::Paused;
    }
    match fs::metadata(part).await {
        Ok(info) => {
            if record
                .chunks
                .iter()
                .any(|c| c.written > 0 && c.start.saturating_add(c.written) > info.len())
            {
                return Err(Error::Invalid(format!(
                    "partial file for {} is shorter than its saved progress",
                    record.id
                )));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => record.chunks.clear(),
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn validate_config(config: &Config) -> Result<()> {
    if !(1..=64).contains(&config.concurrent_downloads)
        || !(1..=64).contains(&config.connections_per_download)
        || config.retries > 20
        || config.read_timeout.is_zero()
    {
        return Err(Error::Invalid(
            "jobs/connections must be 1–64, retries 0–20, and timeout positive".into(),
        ));
    }
    Ok(())
}

async fn restore_rate(database: &Database, configured: Option<RateLimit>) -> Result<RateLimit> {
    match configured {
        Some(rate) => {
            database.save_rate_limit(rate).await?;
            Ok(rate)
        }
        None => database.load_rate_limit().await,
    }
}

struct Recovered {
    jobs: BTreeMap<uuid::Uuid, Arc<Job>>,
    completed_parts: Vec<PathBuf>,
    corrupt_targets: Vec<PathBuf>,
}

async fn restore_jobs(database: &Database, config: &Config) -> Result<Recovered> {
    let mut recovered = Recovered {
        jobs: BTreeMap::new(),
        completed_parts: Vec::new(),
        corrupt_targets: Vec::new(),
    };
    for record in database.load().await? {
        let record = recover_record(
            record,
            &mut recovered.completed_parts,
            &mut recovered.corrupt_targets,
        )
        .await?;
        // Load saved cookie files when the task runs. A removed/expired session
        // must not prevent unrelated tasks or the controller from recovering.
        let (client, _) = net::client(config.read_timeout, record.proxy_url.as_ref(), None).await?;
        recovered
            .jobs
            .insert(record.id, Arc::new(Job::new(record, client)));
    }
    Ok(recovered)
}

async fn cleanup_recovered(
    completed_parts: Vec<PathBuf>,
    corrupt_targets: Vec<PathBuf>,
) -> Result<()> {
    for part in completed_parts {
        fs::remove_file(&part).await?;
        sync_directory(super::storage::parent(&part)?.to_path_buf()).await?;
    }
    if !corrupt_targets.is_empty() {
        for target in corrupt_targets {
            fs::remove_file(&target).await?;
            sync_directory(super::storage::parent(&target)?.to_path_buf()).await?;
        }
    }
    Ok(())
}
