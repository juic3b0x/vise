use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Queued,
    Probing,
    Downloading,
    Paused,
    Completed,
    Failed,
}

impl Status {
    pub fn active(self) -> bool {
        matches!(self, Self::Queued | Self::Probing | Self::Downloading)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Chunk {
    pub start: u64,
    pub end: Option<u64>, // exclusive; None for a stream with unknown length
    pub written: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Record {
    pub id: Uuid,
    pub url: String,
    pub filename: String,
    pub status: Status,
    pub total_bytes: Option<u64>,
    pub ranged: bool,
    pub validator: Option<String>,
    pub chunks: Vec<Chunk>,
    pub error: Option<String>,
    #[serde(skip)]
    pub speed: f64,
}

impl Record {
    pub fn downloaded(&self) -> u64 {
        self.chunks.iter().map(|c| c.written).sum()
    }
    pub fn snapshot(&self) -> Download {
        let downloaded = self.downloaded();
        Download {
            id: self.id,
            url: self.url.clone(),
            filename: self.filename.clone(),
            status: self.status,
            total_bytes: self.total_bytes,
            downloaded_bytes: downloaded,
            bytes_per_second: if self.status == Status::Downloading {
                self.speed
            } else {
                0.0
            },
            eta_seconds: self.total_bytes.and_then(|total| {
                (self.status == Status::Downloading && self.speed > 0.0)
                    .then(|| total.saturating_sub(downloaded) as f64 / self.speed)
            }),
            error: self.error.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Download {
    pub id: Uuid,
    pub url: String,
    pub filename: String,
    pub status: Status,
    pub total_bytes: Option<u64>,
    pub downloaded_bytes: u64,
    pub bytes_per_second: f64,
    pub eta_seconds: Option<f64>,
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Journal {
    pub version: u32,
    pub downloads: Vec<Record>,
}
