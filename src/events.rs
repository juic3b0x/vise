use std::time::SystemTime;

use crate::model::{Download, Status};

/// Discrete events are separate from the coalesced snapshot/progress channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventKind {
    Added,
    PauseRequested,
    Paused,
    Resumed,
    DeleteRequested,
    Deleted,
    Completed,
    Failed,
    Corrupt,
    Verified,
    StateChanged {
        from: Status,
        to: Status,
    },
    Retrying {
        chunk: usize,
        attempt: u32,
        error: String,
    },
    Warning {
        message: String,
    },
    ServerStarted {
        address: String,
    },
    ServerStopped,
    RateLimitChanged {
        bytes_per_second: Option<u64>,
    },
}

#[derive(Debug, Clone)]
pub struct Event {
    pub(crate) timestamp: SystemTime,
    pub kind: EventKind,
    pub download: Option<Download>,
}

impl Event {
    pub(crate) fn new(kind: EventKind, download: Option<Download>) -> Self {
        Self {
            timestamp: SystemTime::now(),
            kind,
            download,
        }
    }
}
