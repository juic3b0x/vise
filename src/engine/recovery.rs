use super::{
    Config, Inner, Job, Manager,
    journal::sync_directory,
    layout::{normalize_chunks, validate_record},
};
use crate::{
    checksum,
    database::{Database, FILE_NAME},
    error::{Error, Result},
    model::{LegacyJournal, Record, Status, timestamp_millis},
    proxy,
    rate_limiter::{RateLimit, RateLimiter},
};
use fs2::FileExt;
use std::{
    collections::BTreeMap,
    fs::OpenOptions,
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
        let (state_dir, lock) = prepare_directory(&mut config).await?;
        let database = Database::open(state_dir.join(FILE_NAME)).await?;
        let rate = restore_rate(&database, config.limit_rate).await?;
        import_legacy(&database, &state_dir).await?;
        let Recovered {
            jobs,
            completed_parts,
            corrupt_targets,
        } = restore_jobs(&database, &state_dir, &config).await?;
        let (events, _) = watch::channel(0);
        let (activity, _) = broadcast::channel(2048);
        let manager = Self(Arc::new(Inner {
            pool: Semaphore::new(config.concurrent_downloads),
            config,
            database,
            rate_limiter: RateLimiter::new(rate),
            settings: Mutex::new(()),
            jobs: RwLock::new(jobs),
            checkpoint: Mutex::new(()),
            admission: Mutex::new(()),
            events,
            activity,
            stopping: AtomicBool::new(false),
            _directory_lock: lock,
        }));
        manager.checkpoint().await?;
        // Persist recovery before removing hard-link evidence or corrupt targets.
        cleanup_recovered(
            &manager.0.config.output_dir,
            completed_parts,
            corrupt_targets,
        )
        .await?;
        Ok(manager)
    }
}

async fn import_legacy(database: &Database, state_dir: &Path) -> Result<()> {
    if database.needs_legacy_import().await? {
        let legacy_path = state_dir.join("state.json");
        let legacy = match fs::read(&legacy_path).await {
            Ok(bytes) => {
                let journal: LegacyJournal = serde_json::from_slice(&bytes)?;
                if journal.version != 1 {
                    return Err(Error::Invalid("unsupported legacy journal version".into()));
                }
                for record in &journal.downloads {
                    validate_record(record)?;
                }
                Some(journal.downloads)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        let imported = legacy.is_some();
        database.import_legacy(legacy.unwrap_or_default()).await?;
        // The SQL marker commits with the import, so a crash during backup
        // cannot cause stale JSON to be imported again after task deletion.
        let backup = state_dir.join("state.json.migrated");
        if imported && !backup.exists() {
            fs::rename(&legacy_path, backup).await?;
        }
    }
    Ok(())
}

async fn recover_record(
    mut record: Record,
    state_dir: &Path,
    output_dir: &Path,
    completed_parts: &mut Vec<PathBuf>,
    corrupt_targets: &mut Vec<PathBuf>,
) -> Result<Record> {
    validate_record(&record)?;
    let part = state_dir.join(format!("{}.part", record.id));
    let target = output_dir.join(&record.filename);
    recover_publication(&mut record, &part, &target, corrupt_targets).await?;
    if record.status == Status::Completed {
        recover_completed(&mut record, &part, &target, completed_parts);
    } else {
        recover_partial(&mut record, &part).await?;
    }
    normalize_chunks(&mut record);
    Ok(record)
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

async fn prepare_directory(config: &mut Config) -> Result<(PathBuf, std::fs::File)> {
    fs::create_dir_all(&config.output_dir).await?;
    config.output_dir = fs::canonicalize(&config.output_dir).await?;
    let state_dir = config.output_dir.join(".vise");
    fs::create_dir_all(&state_dir).await?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(state_dir.join("lock"))?;
    lock.try_lock_exclusive().map_err(|error| {
        Error::Conflict(format!(
            "cannot lock output directory (another Vise process may be using it): {error}"
        ))
    })?;
    Ok((state_dir, lock))
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

async fn restore_jobs(database: &Database, state_dir: &Path, config: &Config) -> Result<Recovered> {
    let mut recovered = Recovered {
        jobs: BTreeMap::new(),
        completed_parts: Vec::new(),
        corrupt_targets: Vec::new(),
    };
    for record in database.load().await? {
        let record = recover_record(
            record,
            state_dir,
            &config.output_dir,
            &mut recovered.completed_parts,
            &mut recovered.corrupt_targets,
        )
        .await?;
        let client = proxy::client(config.read_timeout, record.proxy_url.as_ref())?;
        recovered
            .jobs
            .insert(record.id, Arc::new(Job::new(record, client)));
    }
    Ok(recovered)
}

async fn cleanup_recovered(
    output_dir: &Path,
    completed_parts: Vec<PathBuf>,
    corrupt_targets: Vec<PathBuf>,
) -> Result<()> {
    for part in completed_parts {
        fs::remove_file(part).await?;
    }
    if !corrupt_targets.is_empty() {
        for target in corrupt_targets {
            fs::remove_file(target).await?;
        }
        sync_directory(output_dir.to_path_buf()).await?;
    }
    Ok(())
}
