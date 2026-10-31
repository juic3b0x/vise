//! Shared download manager and its configuration. Controllers use this API while
//! recovery, queue control, persistence, and transfer I/O remain separate modules.
use crate::net::{Transport, credentials::RequestOverrides};
use crate::{
    checksum::ExpectedChecksums,
    database::Database,
    events::Event,
    model::Record,
    proxy::ProxyUrl,
    rate_limiter::{RateLimit, RateLimiter},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};
use tokio::{
    sync::{Mutex, RwLock, Semaphore, broadcast, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

mod adaptive;
mod batch;
pub use batch::{Accepted, BatchResult, Rejected};

mod completion;
mod http;
mod input_batch;
mod journal;
mod layout;
mod migration;
mod queue;
mod recovery;
mod state;
mod storage;
mod transfer;

#[derive(Debug, Clone)]
pub struct Config {
    /// Embedded callers may inject an isolated home; None resolves the OS home.
    pub home_dir: Option<PathBuf>,
    pub output_dir: PathBuf,
    pub concurrent_downloads: usize,
    pub connections_per_download: usize,
    /// Minimum unfinished bytes per half; zero disables tail splitting.
    pub min_split_size: u64,
    pub retries: u32,
    pub read_timeout: Duration,
    /// None restores the saved setting; Some(UNLIMITED) explicitly disables it.
    pub limit_rate: Option<RateLimit>,
    /// Default for newly created tasks; recovered tasks retain their own route.
    pub proxy_url: Option<ProxyUrl>,
    /// Default Netscape file for new tasks; saved tasks retain their own file.
    pub cookies_file: Option<PathBuf>,
    pub request: RequestOverrides,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            home_dir: None,
            output_dir: "downloads".into(),
            concurrent_downloads: 3,
            connections_per_download: 8,
            min_split_size: 1024 * 1024,
            retries: 5,
            read_timeout: Duration::from_secs(30),
            limit_rate: None,
            proxy_url: None,
            cookies_file: None,
            request: RequestOverrides::default(),
        }
    }
}

struct Run {
    cancel: CancellationToken,
    handle: JoinHandle<()>,
}
struct Job {
    output_path: PathBuf,
    part_path: PathBuf,
    record: RwLock<Record>,
    // Reuse only successfully checkpointed immutable completed snapshots.
    // Capture still compares the live record, including mutations during commit.
    completed_snapshot: std::sync::Mutex<Option<Arc<Record>>>,
    // Readers must not observe provisional terminal state during its journal commit.
    settlement: RwLock<()>,
    client: RwLock<Transport>,
    control: Mutex<()>,
    run: Mutex<Option<Run>>,
    removed: AtomicBool,
    adaptive: adaptive::Adaptive,
    journal_blocked: AtomicBool,
}
impl Job {
    async fn snapshot(&self) -> crate::model::Download {
        let _settled = self.settlement.read().await;
        self.adaptive.decorate(self.record.read().await.snapshot())
    }

    fn new(record: Record, client: impl Into<Transport>) -> Self {
        let adaptive = adaptive::Adaptive::default();
        adaptive.reset(&record, RateLimit::UNLIMITED);
        Self {
            adaptive,
            journal_blocked: AtomicBool::new(false),
            output_path: record.output_path.clone(),
            part_path: record.part_path.clone(),
            record: RwLock::new(record),
            completed_snapshot: std::sync::Mutex::new(None),
            settlement: RwLock::new(()),
            client: RwLock::new(client.into()),
            control: Mutex::new(()),
            run: Mutex::new(None),
            removed: AtomicBool::new(false),
        }
    }
}

#[cfg(test)]
type CheckpointHook = (
    tokio::sync::oneshot::Sender<()>,
    tokio::sync::oneshot::Receiver<()>,
);

struct Inner {
    config: Config,
    database: Database,
    stateless_client: tokio::sync::OnceCell<Transport>,
    rate_limiter: RateLimiter,
    settings: Mutex<()>,
    jobs: RwLock<BTreeMap<Uuid, Arc<Job>>>,
    scheduled: RwLock<BTreeSet<Uuid>>,
    pool: Semaphore,
    checkpoint: Mutex<()>,
    journal_group: Arc<journal::group::Group>,
    admission: Mutex<()>,
    events: watch::Sender<u64>,
    activity: broadcast::Sender<Event>,
    stopping: AtomicBool,
    _global_lock: std::fs::File,
    _output_locks: Vec<std::fs::File>,
    #[cfg(test)]
    checkpoint_hook: Mutex<Option<CheckpointHook>>,
    #[cfg(test)]
    delete_hook: Mutex<Option<CheckpointHook>>,
    #[cfg(test)]
    input_hook: Mutex<Option<CheckpointHook>>,
    #[cfg(test)]
    journal_trace: Arc<journal::barriers::Trace>,
}

/// Cloneable controller shared by CLI, REST handlers, and WebSocket subscribers.
#[derive(Clone)]
pub struct Manager(Arc<Inner>);

struct Metadata {
    total: Option<u64>,
    ranged: bool,
    validator: Option<String>,
    expected: ExpectedChecksums,
}

#[derive(Debug, Clone, Default)]
pub struct TransferOptions {
    pub expected: ExpectedChecksums,
    pub proxy_url: Option<ProxyUrl>,
    pub cookies_file: Option<PathBuf>,
    pub request: RequestOverrides,
}
