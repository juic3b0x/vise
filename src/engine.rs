use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::BytesMut;
use fs2::FileExt;
use futures_util::StreamExt;
use reqwest::{Client, Response, StatusCode, header};
use tokio::{
    fs,
    io::{AsyncSeekExt, AsyncWriteExt},
    sync::{Mutex, RwLock, Semaphore, broadcast, watch},
    task::{JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::{
    error::{Error, Result},
    events::{Event, EventKind},
    model::{Chunk, Download, Journal, Record, Status},
};

const BUFFER_SIZE: usize = 256 * 1024;
const MIN_CHUNK_SIZE: u64 = 1024 * 1024;

#[derive(Debug, Clone)]
pub struct Config {
    pub output_dir: PathBuf,
    pub concurrent_downloads: usize,
    pub connections_per_download: usize,
    pub retries: u32,
    pub read_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            output_dir: "downloads".into(),
            concurrent_downloads: 3,
            connections_per_download: 8,
            retries: 5,
            read_timeout: Duration::from_secs(30),
        }
    }
}

struct Run {
    cancel: CancellationToken,
    handle: JoinHandle<()>,
}
struct Job {
    record: RwLock<Record>,
    control: Mutex<()>,
    run: Mutex<Option<Run>>,
    removed: AtomicBool,
}
impl Job {
    fn new(record: Record) -> Self {
        Self {
            record: RwLock::new(record),
            control: Mutex::new(()),
            run: Mutex::new(None),
            removed: AtomicBool::new(false),
        }
    }
}

struct Inner {
    config: Config,
    client: Client,
    jobs: RwLock<BTreeMap<Uuid, Arc<Job>>>,
    pool: Semaphore,
    checkpoint: Mutex<()>,
    admission: Mutex<()>,
    events: watch::Sender<u64>,
    activity: broadcast::Sender<Event>,
    stopping: AtomicBool,
    _directory_lock: std::fs::File,
}

/// Cloneable controller shared by CLI, REST handlers, and WebSocket subscribers.
#[derive(Clone)]
pub struct Manager(Arc<Inner>);

struct Metadata {
    total: Option<u64>,
    ranged: bool,
    validator: Option<String>,
}

impl Manager {
    pub async fn open(mut config: Config) -> Result<Self> {
        if !(1..=64).contains(&config.concurrent_downloads)
            || !(1..=64).contains(&config.connections_per_download)
            || config.retries > 20
            || config.read_timeout.is_zero()
        {
            return Err(Error::Invalid(
                "jobs/connections must be 1–64, retries 0–20, and timeout positive".into(),
            ));
        }
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
        lock.try_lock_exclusive().map_err(|e| {
            Error::Conflict(format!(
                "cannot lock output directory (another Vise process may be using it): {e}"
            ))
        })?;
        let client = Client::builder()
            .user_agent(concat!("Vise/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(Duration::from_secs(15))
            .read_timeout(config.read_timeout)
            .redirect(reqwest::redirect::Policy::limited(10))
            .pool_max_idle_per_host(64)
            .build()?;
        let mut jobs = BTreeMap::new();
        match fs::read(state_dir.join("state.json")).await {
            Ok(data) => {
                let journal: Journal = serde_json::from_slice(&data)?;
                if journal.version != 1 {
                    return Err(Error::Invalid("unsupported state-file version".into()));
                }
                let mut names = std::collections::HashSet::new();
                for mut record in journal.downloads {
                    validate_record(&record)?;
                    if !names.insert(record.filename.clone()) || jobs.contains_key(&record.id) {
                        return Err(Error::Invalid(
                            "duplicate filename or ID in state file".into(),
                        ));
                    }
                    let part = state_dir.join(format!("{}.part", record.id));
                    let target = config.output_dir.join(&record.filename);
                    // Recover a crash between publishing a hard link and saving completion.
                    if record.status != Status::Completed
                        && target.exists()
                        && part.exists()
                        && same_file::is_same_file(&target, &part)?
                    {
                        record.status = Status::Completed;
                    }
                    if record.status == Status::Completed {
                        if !target.is_file()
                            || record.total_bytes.is_some_and(|n| {
                                target.metadata().map(|m| m.len() != n).unwrap_or(true)
                            })
                        {
                            record.status = Status::Failed;
                            record.chunks.clear();
                            record.error = Some(
                                "completed output is missing or its length has changed".into(),
                            );
                        } else if part.exists() {
                            fs::remove_file(&part).await?;
                        }
                    } else {
                        if record.status.active() {
                            record.status = Status::Paused;
                        }
                        match fs::metadata(&part).await {
                            Ok(info) => {
                                if record.chunks.iter().any(|c| {
                                    c.written > 0 && c.start.saturating_add(c.written) > info.len()
                                }) {
                                    return Err(Error::Invalid(format!(
                                        "partial file for {} is shorter than its saved progress",
                                        record.id
                                    )));
                                }
                            }
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                                record.chunks.clear()
                            }
                            Err(e) => return Err(e.into()),
                        }
                    }
                    jobs.insert(record.id, Arc::new(Job::new(record)));
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            Err(e) => return Err(e.into()),
        }
        let (events, _) = watch::channel(0);
        let (activity, _) = broadcast::channel(2048);
        let manager = Self(Arc::new(Inner {
            pool: Semaphore::new(config.concurrent_downloads),
            config,
            client,
            jobs: RwLock::new(jobs),
            checkpoint: Mutex::new(()),
            admission: Mutex::new(()),
            events,
            activity,
            stopping: AtomicBool::new(false),
            _directory_lock: lock,
        }));
        manager.checkpoint().await?;
        Ok(manager)
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.0.events.subscribe()
    }
    pub fn subscribe_events(&self) -> broadcast::Receiver<Event> {
        self.0.activity.subscribe()
    }
    pub(crate) fn publish(&self, kind: EventKind, download: Option<Download>) {
        // Sending is synchronous and never waits for terminal I/O or a subscriber.
        let _ = self.0.activity.send(Event::new(kind, download));
    }
    async fn publish_job(&self, kind: EventKind, job: &Job) {
        self.publish(kind, Some(job.record.read().await.snapshot()));
    }
    async fn transition(&self, job: &Job, to: Status) {
        let mut record = job.record.write().await;
        let from = record.status;
        record.status = to;
        let snapshot = record.snapshot();
        drop(record);
        if from != to {
            self.publish(EventKind::StateChanged { from, to }, Some(snapshot));
        }
        self.notify();
    }
    pub fn server_started(&self, address: String) {
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
    fn notify(&self) {
        self.0.events.send_modify(|v| *v = v.wrapping_add(1));
    }
    fn part_path(&self, id: Uuid) -> PathBuf {
        self.0
            .config
            .output_dir
            .join(".vise")
            .join(format!("{id}.part"))
    }

    pub async fn list(&self) -> Vec<Download> {
        let jobs: Vec<_> = self.0.jobs.read().await.values().cloned().collect();
        let mut result = Vec::with_capacity(jobs.len());
        for job in jobs {
            result.push(job.record.read().await.snapshot());
        }
        result
    }
    async fn job(&self, id: Uuid) -> Result<Arc<Job>> {
        self.0
            .jobs
            .read()
            .await
            .get(&id)
            .cloned()
            .ok_or(Error::NotFound(id))
    }
    pub async fn get(&self, id: Uuid) -> Result<Download> {
        Ok(self.job(id).await?.record.read().await.snapshot())
    }

    pub async fn add(&self, url: &str, filename: Option<&str>) -> Result<Download> {
        let url = validate_url(url)?;
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
        let mut jobs = self.0.jobs.write().await;
        let used: Vec<_> = jobs.values().cloned().collect();
        let mut names = Vec::new();
        for job in used {
            names.push(job.record.read().await.filename.clone());
        }
        let mut name = base.clone();
        let mut suffix = 1;
        while self.0.config.output_dir.join(&name).exists() || names.contains(&name) {
            if filename.is_some() {
                return Err(Error::Conflict(format!(
                    "output filename already in use: {name}"
                )));
            }
            name = numbered_filename(&base, suffix);
            suffix += 1;
        }
        let id = Uuid::new_v4();
        jobs.insert(
            id,
            Arc::new(Job::new(Record {
                id,
                url: url.into(),
                filename: name,
                status: Status::Paused,
                total_bytes: None,
                ranged: false,
                validator: None,
                chunks: vec![],
                error: None,
                speed: 0.0,
            })),
        );
        drop(jobs);
        if let Err(error) = self.checkpoint().await {
            self.0.jobs.write().await.remove(&id);
            return Err(error);
        }
        self.start(id, EventKind::Added).await?;
        self.get(id).await
    }

    pub async fn resume(&self, id: Uuid) -> Result<()> {
        self.start(id, EventKind::Resumed).await
    }

    async fn start(&self, id: Uuid, action: EventKind) -> Result<()> {
        let job = self.job(id).await?;
        let _control = job.control.lock().await;
        if job.removed.load(Ordering::Acquire) {
            return Err(Error::NotFound(id));
        }
        if self.0.stopping.load(Ordering::Acquire) {
            return Err(Error::Conflict("manager is shutting down".into()));
        }
        let status = job.record.read().await.status;
        if status.active() {
            self.publish_job(action, &job).await;
            return Ok(());
        }
        if status == Status::Completed {
            return Err(Error::Conflict("download is already completed".into()));
        }
        if let Some(run) = job.run.lock().await.take() {
            run.handle.await.map_err(|e| Error::Worker(e.to_string()))?;
        }
        {
            let mut record = job.record.write().await;
            record.status = Status::Queued;
            record.error = None;
            record.speed = 0.0;
        }
        // Publish before spawning so even an empty file logs ADDED before DONE.
        self.publish_job(action, &job).await;
        let cancel = CancellationToken::new();
        let manager = self.clone();
        let worker_job = job.clone();
        let worker_cancel = cancel.clone();
        let handle = tokio::spawn(async move {
            let result = manager
                .execute(worker_job.clone(), worker_cancel.clone())
                .await;
            let event = {
                let mut record = worker_job.record.write().await;
                let kind = match result {
                    Ok(()) => {
                        record.status = Status::Completed;
                        EventKind::Completed
                    }
                    Err(Error::Cancelled) => {
                        record.status = Status::Paused;
                        EventKind::Paused
                    }
                    Err(error) => {
                        record.status = Status::Failed;
                        record.error = Some(error.to_string());
                        EventKind::Failed
                    }
                };
                record.speed = 0.0;
                (kind, record.snapshot())
            };
            manager.publish(event.0, Some(event.1));
            manager.notify();
            if let Err(error) = manager.checkpoint().await {
                tracing::error!(%id, %error, "cannot persist download state");
                worker_job.record.write().await.error =
                    Some(format!("cannot persist state: {error}"));
                manager
                    .publish_job(
                        EventKind::Warning {
                            message: format!("cannot persist state: {error}"),
                        },
                        &worker_job,
                    )
                    .await;
            } else if worker_job.record.read().await.status == Status::Completed {
                if let Err(error) = fs::remove_file(manager.part_path(id)).await {
                    tracing::warn!(%id, %error, "cannot remove completed partial file");
                    manager
                        .publish_job(
                            EventKind::Warning {
                                message: format!("cannot remove completed partial file: {error}"),
                            },
                            &worker_job,
                        )
                        .await;
                }
            }
            manager.notify();
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
        if job.record.read().await.status == Status::Completed {
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
        if record.status != Status::Completed {
            record.status = Status::Paused;
        }
        record.speed = 0.0;
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
        let snapshot = job.record.read().await.snapshot();
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
        match fs::remove_file(self.part_path(id)).await {
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
        self.0.stopping.store(true, Ordering::Release);
        // Cancel all transfers first so a large queue does not delay shutdown.
        let jobs: Vec<_> = self.0.jobs.read().await.values().cloned().collect();
        for job in &jobs {
            if let Some(run) = job.run.lock().await.as_ref() {
                run.cancel.cancel();
            }
        }
        let mut failure = None;
        for item in self.list().await {
            if item.status.active() {
                if let Err(error) = self.pause(item.id).await {
                    if failure.is_none() {
                        failure = Some(error);
                    }
                }
            }
        }
        for job in jobs {
            let _control = job.control.lock().await;
            if let Some(run) = job.run.lock().await.take() {
                if let Err(error) = run.handle.await {
                    if failure.is_none() {
                        failure = Some(Error::Worker(error.to_string()));
                    }
                }
            }
        }
        if let Err(error) = self.checkpoint().await {
            if failure.is_none() {
                failure = Some(error);
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Drive progress samples and crash-safe state checkpoints until `stop` fires.
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

    async fn checkpoint(&self) -> Result<()> {
        let _guard = self.0.checkpoint.lock().await;
        self.checkpoint_locked().await
    }

    // Caller holds the checkpoint mutex, including around destructive resets.
    async fn checkpoint_locked(&self) -> Result<()> {
        let jobs: Vec<_> = self.0.jobs.read().await.values().cloned().collect();
        let mut downloads = Vec::with_capacity(jobs.len());
        for job in jobs {
            downloads.push(job.record.read().await.clone());
        }
        // Capture offsets BEFORE syncing files: every saved offset refers to durable bytes.
        for record in &downloads {
            match fs::OpenOptions::new()
                .write(true)
                .open(self.part_path(record.id))
                .await
            {
                Ok(file) => file.sync_data().await?,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
                Err(e) => return Err(e.into()),
            }
        }
        let bytes = serde_json::to_vec(&Journal {
            version: 1,
            downloads,
        })?;
        let dir = self.0.config.output_dir.join(".vise");
        let temp = dir.join("state.json.tmp");
        let mut file = fs::File::create(&temp).await?;
        file.write_all(&bytes).await?;
        file.sync_all().await?;
        drop(file);
        fs::rename(temp, dir.join("state.json")).await?;
        sync_directory(dir).await?;
        Ok(())
    }

    async fn execute(&self, job: Arc<Job>, cancel: CancellationToken) -> Result<()> {
        let _permit = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(Error::Cancelled),
            permit = self.0.pool.acquire() => permit.map_err(|e| Error::Worker(e.to_string()))?,
        };
        self.transition(&job, Status::Probing).await;
        let url = job.record.read().await.url.clone();
        let metadata = self.probe(&url, &cancel).await?;
        let id = job.record.read().await.id;
        let path = self.part_path(id);
        let mut file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .await?;
        {
            let _checkpoint = self.0.checkpoint.lock().await;
            let mut record = job.record.write().await;
            let reusable = metadata.ranged
                && metadata.validator.is_some()
                && record.validator == metadata.validator
                && record.total_bytes == metadata.total
                && record.ranged
                && !record.chunks.is_empty();
            if !reusable {
                record.chunks = make_chunks(
                    metadata.total,
                    metadata.ranged,
                    self.0.config.connections_per_download,
                );
            }
            record.total_bytes = metadata.total;
            record.ranged = metadata.ranged;
            record.validator = metadata.validator;
            drop(record);
            if !reusable {
                // Invalidate old durable offsets BEFORE destroying old data. A
                // crash here restores zero progress, never offsets into a reset file.
                self.checkpoint_locked().await?;
                file.set_len(0).await?;
            }
            if let Some(total) = metadata.total {
                file.set_len(total).await?;
            }
        }
        self.transition(&job, Status::Downloading).await;
        let mut workers = JoinSet::new();
        let worker_cancel = cancel.child_token();
        let count = job.record.read().await.chunks.len();
        for index in 0..count {
            let manager = self.clone();
            let job = job.clone();
            let cancel = worker_cancel.clone();
            workers.spawn(async move { manager.transfer_chunk(job, index, &cancel).await });
        }
        let mut failure = None;
        while let Some(result) = workers.join_next().await {
            let result = result.unwrap_or_else(|e| Err(Error::Worker(e.to_string())));
            if let Err(error) = result {
                if failure.is_none() {
                    failure = Some(error);
                    worker_cancel.cancel();
                }
            }
        }
        // Do not abort tasks: cancellation waits for file writes and offset updates.
        if let Some(error) = failure {
            return Err(error);
        }
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let record = job.record.read().await;
        let downloaded = record.downloaded();
        if record.total_bytes.is_some_and(|total| total != downloaded) {
            return Err(Error::Protocol(
                "download length does not match expected length".into(),
            ));
        }
        let target = self.0.config.output_dir.join(&record.filename);
        drop(record);
        job.record.write().await.total_bytes = Some(downloaded);
        file.set_len(downloaded).await?;
        file.flush().await?;
        file.sync_all().await?;
        // Save all offsets before publishing, making an interrupted commit recoverable.
        self.checkpoint().await?;
        fs::hard_link(&path, &target).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                Error::Conflict(format!("output already exists: {}", target.display()))
            } else {
                e.into()
            }
        })?;
        sync_directory(self.0.config.output_dir.clone()).await?;
        Ok(())
    }

    async fn probe(&self, url: &str, cancel: &CancellationToken) -> Result<Metadata> {
        let mut attempt = 0;
        loop {
            let result = async {
                let response = self.request(url, Some("bytes=0-0"), None, cancel).await?;
                let status = response.status();
                if status == StatusCode::RANGE_NOT_SATISFIABLE
                    && response
                        .headers()
                        .get(header::CONTENT_RANGE)
                        .and_then(|v| v.to_str().ok())
                        == Some("bytes */0")
                {
                    return Ok(Metadata {
                        total: Some(0),
                        ranged: false,
                        validator: None,
                    });
                }
                check_response(&response)?;
                let validator = validator(&response);
                if status == StatusCode::PARTIAL_CONTENT {
                    let (start, end, total) = content_range(&response)?;
                    if start != 0 || end != 0 || total == 0 {
                        return Err(Error::Protocol("range probe did not return byte 0".into()));
                    }
                    // Parallel/resumed ranges need an identity validator. Without
                    // one a changing resource could silently splice two versions.
                    Ok(Metadata {
                        total: Some(total),
                        ranged: validator.is_some(),
                        validator,
                    })
                } else if status == StatusCode::OK {
                    Ok(Metadata {
                        total: response.content_length(),
                        ranged: false,
                        validator,
                    })
                } else {
                    Err(Error::Protocol(format!(
                        "unexpected probe status: {status}"
                    )))
                }
            }
            .await;
            match result {
                Ok(value) => return Ok(value),
                Err(error) if error.retryable() && attempt < self.0.config.retries => {
                    self.backoff(attempt, &error, cancel).await?;
                    attempt += 1;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn request(
        &self,
        url: &str,
        range: Option<&str>,
        validator: Option<&str>,
        cancel: &CancellationToken,
    ) -> Result<Response> {
        let mut request = self
            .0
            .client
            .get(url)
            .header(header::ACCEPT_ENCODING, "identity");
        if let Some(range) = range {
            request = request.header(header::RANGE, range);
        }
        if let Some(validator) = validator {
            request = request.header(header::IF_RANGE, validator);
        }
        tokio::select! { biased; _ = cancel.cancelled() => Err(Error::Cancelled), response = request.send() => Ok(response?) }
    }

    async fn transfer_chunk(
        &self,
        job: Arc<Job>,
        index: usize,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let mut attempt = 0;
        loop {
            if cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }
            let result = self.transfer_once(&job, index, cancel).await;
            match result {
                Ok(()) => return Ok(()),
                Err(error) if error.retryable() && attempt < self.0.config.retries => {
                    tracing::warn!(chunk = index, retry = attempt + 1, %error, "retrying transfer");
                    self.publish_job(
                        EventKind::Retrying {
                            chunk: index,
                            attempt: attempt + 1,
                            error: error.to_string(),
                        },
                        &job,
                    )
                    .await;
                    self.backoff(attempt, &error, cancel).await?;
                    attempt += 1;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn transfer_once(
        &self,
        job: &Arc<Job>,
        index: usize,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let (id, url, ranged, validator, chunk) = {
            let record = job.record.read().await;
            (
                record.id,
                record.url.clone(),
                record.ranged,
                record.validator.clone(),
                record.chunks[index].clone(),
            )
        };
        if chunk.end == Some(0)
            || (ranged
                && chunk
                    .end
                    .is_some_and(|end| chunk.start + chunk.written == end))
        {
            return Ok(());
        }
        let offset = if ranged {
            chunk.start + chunk.written
        } else {
            0
        };
        let range = if ranged {
            Some(format!(
                "bytes={offset}-{}",
                chunk
                    .end
                    .ok_or_else(|| Error::Protocol("range has no end".into()))?
                    - 1
            ))
        } else {
            None
        };
        let response = self
            .request(
                &url,
                range.as_deref(),
                if ranged { validator.as_deref() } else { None },
                cancel,
            )
            .await?;
        check_response(&response)?;
        if ranged {
            if response.status() != StatusCode::PARTIAL_CONTENT {
                return Err(Error::Protocol(
                    "server ignored range or the remote file changed; resume to reprobe".into(),
                ));
            }
            let (start, end, total) = content_range(&response)?;
            let expected = job.record.read().await.total_bytes;
            if start != offset || Some(end + 1) != chunk.end || Some(total) != expected {
                return Err(Error::Protocol(
                    "Content-Range does not match the requested segment".into(),
                ));
            }
        } else if response.status() != StatusCode::OK {
            return Err(Error::Protocol(format!(
                "expected HTTP 200 for stream, got {}",
                response.status()
            )));
        }
        if let (Some(expected), Some(actual)) =
            (validator.as_deref(), crate::engine::validator(&response))
        {
            if expected != actual {
                return Err(Error::Protocol(
                    "remote file validator changed; resume to reprobe".into(),
                ));
            }
        }
        if !ranged {
            let _checkpoint = self.0.checkpoint.lock().await;
            let mut record = job.record.write().await;
            record.total_bytes = response.content_length();
            record.chunks[index].end = response.content_length();
            record.chunks[index].written = 0;
            drop(record);
            self.checkpoint_locked().await?;
            fs::OpenOptions::new()
                .write(true)
                .open(self.part_path(id))
                .await?
                .set_len(0)
                .await?;
        }
        let expected = job.record.read().await.chunks[index]
            .end
            .map(|end| end - offset);
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(self.part_path(id))
            .await?;
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        let mut stream = response.bytes_stream();
        let mut pending = BytesMut::with_capacity(BUFFER_SIZE);
        let mut received = 0u64;
        loop {
            let next = tokio::select! { biased; _ = cancel.cancelled() => return Err(Error::Cancelled), value = stream.next() => value };
            match next {
                Some(Ok(bytes)) => {
                    received = received
                        .checked_add(bytes.len() as u64)
                        .ok_or_else(|| Error::Protocol("response is too large".into()))?;
                    if expected.is_some_and(|length| received > length) {
                        return Err(Error::Protocol("response exceeds requested length".into()));
                    }
                    // Bound the write buffer even when a peer sends very large frames.
                    let mut remaining = bytes.as_ref();
                    while !remaining.is_empty() {
                        let size = remaining.len().min(BUFFER_SIZE - pending.len());
                        pending.extend_from_slice(&remaining[..size]);
                        remaining = &remaining[size..];
                        if pending.len() == BUFFER_SIZE {
                            write_pending(&mut file, job, index, &mut pending).await?;
                        }
                    }
                }
                Some(Err(error)) => {
                    write_pending(&mut file, job, index, &mut pending).await?;
                    return Err(error.into());
                }
                None => break,
            }
        }
        write_pending(&mut file, job, index, &mut pending).await?;
        if expected.is_some_and(|length| received != length) {
            return Err(Error::Protocol("unexpected end of response body".into()));
        }
        Ok(())
    }

    async fn backoff(&self, attempt: u32, error: &Error, cancel: &CancellationToken) -> Result<()> {
        let seconds = match error {
            Error::Remote {
                retry_after: Some(seconds),
                ..
            } => (*seconds).clamp(1, 60),
            _ => (1u64 << attempt.min(5)).min(30),
        };
        tokio::select! { biased; _ = cancel.cancelled() => Err(Error::Cancelled), _ = tokio::time::sleep(Duration::from_secs(seconds)) => Ok(()) }
    }
}

async fn write_pending(
    file: &mut fs::File,
    job: &Job,
    index: usize,
    buffer: &mut BytesMut,
) -> Result<()> {
    if !buffer.is_empty() {
        file.write_all(buffer).await?;
        // Tokio files buffer the blocking write; flush ensures it finished before accounting.
        file.flush().await?;
        job.record.write().await.chunks[index].written += buffer.len() as u64;
        buffer.clear();
    }
    Ok(())
}

fn check_response(response: &Response) -> Result<()> {
    if !response.status().is_success() {
        return Err(Error::Remote {
            status: response.status(),
            retry_after: response
                .headers()
                .get(header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse().ok()),
        });
    }
    if response
        .headers()
        .get(header::CONTENT_ENCODING)
        .is_some_and(|v| v.as_bytes() != b"identity")
    {
        return Err(Error::Protocol(
            "server returned encoded bytes despite Accept-Encoding: identity".into(),
        ));
    }
    Ok(())
}

fn validator(response: &Response) -> Option<String> {
    response
        .headers()
        .get(header::ETAG)
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.starts_with("W/"))
        .or_else(|| {
            response
                .headers()
                .get(header::LAST_MODIFIED)
                .and_then(|v| v.to_str().ok())
        })
        .map(str::to_owned)
}

fn content_range(response: &Response) -> Result<(u64, u64, u64)> {
    let value = response
        .headers()
        .get(header::CONTENT_RANGE)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| Error::Protocol("missing Content-Range".into()))?;
    let parsed = (|| {
        let (span, total) = value.strip_prefix("bytes ")?.split_once('/')?;
        let (start, end) = span.split_once('-')?;
        let (start, end, total) = (
            start.parse::<u64>().ok()?,
            end.parse::<u64>().ok()?,
            total.parse::<u64>().ok()?,
        );
        (start <= end && end < total).then_some((start, end, total))
    })();
    parsed.ok_or_else(|| Error::Protocol(format!("malformed Content-Range: {value}")))
}

fn make_chunks(total: Option<u64>, ranged: bool, connections: usize) -> Vec<Chunk> {
    if !ranged || total == Some(0) {
        return vec![Chunk {
            start: 0,
            end: total,
            written: 0,
        }];
    }
    let total = total.unwrap_or(0);
    let count = (connections as u64)
        .min(total.div_ceil(MIN_CHUNK_SIZE))
        .max(1);
    let size = total.div_ceil(count);
    (0..count)
        .filter_map(|i| {
            let start = i * size;
            (start < total).then_some(Chunk {
                start,
                end: Some(start.saturating_add(size).min(total)),
                written: 0,
            })
        })
        .collect()
}

fn validate_url(input: &str) -> Result<Url> {
    if input.len() > 8192 {
        return Err(Error::Invalid("URL is too long".into()));
    }
    let url = Url::parse(input).map_err(|e| Error::Invalid(format!("invalid URL: {e}")))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(Error::Invalid(
            "use an HTTP(S) URL without embedded credentials".into(),
        ));
    }
    Ok(url)
}

fn validate_filename(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 200
        || name == "."
        || name == ".."
        || name.starts_with('.')
        || name
            .chars()
            .any(|c| c.is_control() || "/\\:<>\"|?*".contains(c))
        || name.ends_with(['.', ' '])
    {
        return Err(Error::Invalid(
            "filename must be a plain, non-hidden filename of at most 200 bytes".into(),
        ));
    }
    let stem = name.split('.').next().unwrap_or("").to_ascii_uppercase();
    if [
        "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
        "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    ]
    .contains(&stem.as_str())
    {
        return Err(Error::Invalid("reserved filename".into()));
    }
    Ok(())
}

fn default_filename(url: &Url) -> String {
    let raw = url
        .path_segments()
        .and_then(|mut parts| parts.next_back())
        .unwrap_or("");
    let mut name: String = raw
        .chars()
        .map(|c| {
            if c.is_control() || "/\\:<>\"|?*%".contains(c) {
                '_'
            } else {
                c
            }
        })
        .take(120)
        .collect();
    // Leave room for a numeric collision suffix even with multi-byte filenames.
    while name.len() > 160 {
        name.pop();
    }
    if validate_filename(&name).is_ok() {
        name
    } else {
        "download.bin".into()
    }
}
fn numbered_filename(base: &str, number: usize) -> String {
    let path = Path::new(base);
    match (
        path.file_stem().and_then(|s| s.to_str()),
        path.extension().and_then(|s| s.to_str()),
    ) {
        (Some(stem), Some(ext)) => format!("{stem} ({number}).{ext}"),
        _ => format!("{base} ({number})"),
    }
}

fn validate_record(record: &Record) -> Result<()> {
    validate_url(&record.url)?;
    validate_filename(&record.filename)?;
    let mut offset = 0;
    for chunk in &record.chunks {
        if chunk.start != offset
            || chunk
                .end
                .is_some_and(|end| end < chunk.start || chunk.written > end - chunk.start)
            || chunk.start.checked_add(chunk.written).is_none()
        {
            return Err(Error::Invalid("invalid chunk offsets in state file".into()));
        }
        offset = chunk.end.unwrap_or(0);
    }
    if record.ranged
        && !record.chunks.is_empty()
        && (record.total_bytes != Some(offset) || record.chunks.iter().any(|c| c.end.is_none()))
    {
        return Err(Error::Invalid("invalid range layout in state file".into()));
    }
    if !record.ranged && record.chunks.len() > 1 {
        return Err(Error::Invalid("invalid stream layout in state file".into()));
    }
    Ok(())
}

async fn sync_directory(path: PathBuf) -> Result<()> {
    // Directory fsync makes renames/link creation durable on Unix. Windows does not
    // permit opening a directory as a normal File; individual files are still synced.
    #[cfg(unix)]
    tokio::task::spawn_blocking(move || std::fs::File::open(path)?.sync_all())
        .await
        .map_err(|e| Error::Worker(e.to_string()))??;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn chunks_cover_exactly_once() {
        for total in [
            1,
            MIN_CHUNK_SIZE,
            MIN_CHUNK_SIZE + 1,
            17 * MIN_CHUNK_SIZE + 7,
        ] {
            let chunks = make_chunks(Some(total), true, 8);
            let mut cursor = 0;
            for chunk in chunks {
                assert_eq!(chunk.start, cursor);
                cursor = chunk.end.unwrap();
            }
            assert_eq!(cursor, total);
        }
    }
    #[test]
    fn rejects_unsafe_paths_and_urls() {
        for name in ["../file", ".vise", "a/b", "a\\b", "CON.txt", "", "x\n"] {
            assert!(validate_filename(name).is_err());
        }
        assert!(validate_url("file:///etc/passwd").is_err());
        assert!(validate_url("https://user:pass@example.com/a").is_err());
        assert!(validate_url("https://example.com/a").is_ok());
    }
}
