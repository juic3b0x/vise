use serde::{Deserialize, Serialize};
use std::{str::FromStr, time::SystemTime};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Queued,
    Probing,
    Downloading,
    Verifying,
    Paused,
    Completed,
    Failed,
    Corrupt,
}

impl Status {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Probing => "probing",
            Self::Downloading => "downloading",
            Self::Verifying => "verifying",
            Self::Paused => "paused",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Corrupt => "corrupt",
        }
    }
    pub fn active(self) -> bool {
        matches!(
            self,
            Self::Queued | Self::Probing | Self::Downloading | Self::Verifying
        )
    }
}

impl FromStr for Status {
    type Err = crate::error::Error;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "queued" => Ok(Self::Queued),
            "probing" => Ok(Self::Probing),
            "downloading" => Ok(Self::Downloading),
            "verifying" => Ok(Self::Verifying),
            "paused" => Ok(Self::Paused),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "corrupt" => Ok(Self::Corrupt),
            _ => Err(crate::error::Error::Invalid(format!(
                "unknown journal download status: {value}"
            ))),
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChunkStatus {
    #[default]
    Queued,
    Downloading,
    Paused,
    Completed,
    Failed,
}
impl ChunkStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Downloading => "downloading",
            Self::Paused => "paused",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}
impl FromStr for ChunkStatus {
    type Err = crate::error::Error;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "queued" => Ok(Self::Queued),
            "downloading" => Ok(Self::Downloading),
            "paused" => Ok(Self::Paused),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            _ => Err(crate::error::Error::Invalid(format!(
                "unknown journal chunk status: {value}"
            ))),
        }
    }
}

pub(crate) fn timestamp_millis() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Chunk {
    pub start: u64,
    pub end: Option<u64>, // exclusive; None for a stream with unknown length
    pub written: u64,
    #[serde(default)]
    pub status: ChunkStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    #[serde(default, flatten)]
    pub checksums: crate::checksum::Checksums,
    #[serde(default = "timestamp_millis")]
    pub created_at: i64,
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
            created_at: self.created_at,
            checksums: self.checksums.clone(),
            chunks: self
                .chunks
                .iter()
                .enumerate()
                .map(|(index, chunk)| ChunkProgress {
                    index,
                    start_byte: chunk.start,
                    end_byte: chunk.end,
                    downloaded_bytes: chunk.written,
                    status: chunk.status,
                })
                .collect(),
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
    #[serde(default, flatten)]
    pub checksums: crate::checksum::Checksums,
    /// Unix epoch milliseconds (UTC).
    pub created_at: i64,
    /// Live segment state. End offsets are exclusive; null denotes a stream.
    #[serde(default)]
    pub chunks: Vec<ChunkProgress>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkProgress {
    pub index: usize,
    pub start_byte: u64,
    pub end_byte: Option<u64>,
    pub downloaded_bytes: u64,
    pub status: ChunkStatus,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct LegacyJournal {
    pub version: u32,
    pub downloads: Vec<Record>,
}
