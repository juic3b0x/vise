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
    collections::BTreeMap,
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
mod queue;
mod recovery;
mod state;
mod transfer;

#[derive(Debug, Clone)]
pub struct Config {
    pub output_dir: PathBuf,
    pub concurrent_downloads: usize,
    pub connections_per_download: usize,
    pub retries: u32,
    pub read_timeout: Duration,
    /// None restores the saved setting; Some(UNLIMITED) explicitly disables it.
    pub limit_rate: Option<RateLimit>,
    /// Default for newly created tasks; recovered tasks retain their own route.
    pub proxy_url: Option<ProxyUrl>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            output_dir: "downloads".into(),
            concurrent_downloads: 3,
            connections_per_download: 8,
            retries: 5,
            read_timeout: Duration::from_secs(30),
            limit_rate: None,
            proxy_url: None,
        }
    }
}

struct Run {
    cancel: CancellationToken,
    handle: JoinHandle<()>,
}
struct Job {
    record: RwLock<Record>,
    client: Client,
    control: Mutex<()>,
    run: Mutex<Option<Run>>,
    removed: AtomicBool,
}
impl Job {
    fn new(record: Record, client: Client) -> Self {
        Self {
            record: RwLock::new(record),
            client,
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
    expected: ExpectedChecksums,
}

#[derive(Debug, Clone, Default)]
pub struct TransferOptions {
    pub expected: ExpectedChecksums,
    pub proxy_url: Option<ProxyUrl>,
}
