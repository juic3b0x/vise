//! Shared download manager and its configuration. Controllers use this API while
//! recovery, queue control, persistence, and transfer I/O remain separate modules.
use crate::{
    checksum::ExpectedChecksums,
    database::Database,
    events::Event,
    model::Record,
    proxy::ProxyUrl,
    rate_limiter::{RateLimit, RateLimiter},
};
use reqwest::Client;
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

mod http;
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
    pub retries: u32,
    pub read_timeout: Duration,
    /// None restores the saved setting; Some(UNLIMITED) explicitly disables it.
    pub limit_rate: Option<RateLimit>,
    /// Default for newly created tasks; recovered tasks retain their own route.
    pub proxy_url: Option<ProxyUrl>,
    /// Default Netscape file for new tasks; saved tasks retain their own file.
    pub cookies_file: Option<PathBuf>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            home_dir: None,
            output_dir: "downloads".into(),
            concurrent_downloads: 3,
            connections_per_download: 8,
            retries: 5,
            read_timeout: Duration::from_secs(30),
            limit_rate: None,
            proxy_url: None,
            cookies_file: None,
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
    client: RwLock<Client>,
    control: Mutex<()>,
    run: Mutex<Option<Run>>,
    removed: AtomicBool,
}
impl Job {
    fn new(record: Record, client: Client) -> Self {
        Self {
            output_path: record.output_path.clone(),
            part_path: record.part_path.clone(),
            record: RwLock::new(record),
            client: RwLock::new(client),
            control: Mutex::new(()),
            run: Mutex::new(None),
            removed: AtomicBool::new(false),
        }
    }
}

struct Inner {
    config: Config,
    database: Database,
    rate_limiter: RateLimiter,
    settings: Mutex<()>,
    jobs: RwLock<BTreeMap<Uuid, Arc<Job>>>,
    scheduled: RwLock<BTreeSet<Uuid>>,
    pool: Semaphore,
    checkpoint: Mutex<()>,
    admission: Mutex<()>,
    events: watch::Sender<u64>,
    activity: broadcast::Sender<Event>,
    stopping: AtomicBool,
    _global_lock: std::fs::File,
    _output_locks: Vec<std::fs::File>,
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
}
