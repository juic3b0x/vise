use std::io::{self, Write};

use anyhow::{Context, Result};
use serde::Serialize;
use tokio::sync::{mpsc, oneshot};
use vise::model::Download;

struct Line {
    bytes: Vec<u8>,
    done: oneshot::Sender<io::Result<()>>,
}

pub(super) struct Output {
    pub json: bool,
    sender: mpsc::Sender<Line>,
}

impl Output {
    pub fn new(json: bool) -> io::Result<Self> {
        let (sender, mut receiver) = mpsc::channel::<Line>(1);
        // A full OS pipe must not block Tokio or hold runtime shutdown open.
        // This bounded writer thread detaches on interruption, like CLI stdin.
        std::thread::Builder::new()
            .name("vise-client-stdout".into())
            .spawn(move || {
                let stdout = io::stdout();
                let mut writer = stdout.lock();
                while let Some(line) = receiver.blocking_recv() {
                    let result = writer.write_all(&line.bytes).and_then(|_| writer.flush());
                    let failed = result.is_err();
                    let _ = line.done.send(result);
                    if failed {
                        break;
                    }
                }
            })?;
        Ok(Self { json, sender })
    }

    pub async fn value(&self, value: &impl Serialize, text: String) -> Result<()> {
        let mut bytes = if self.json {
            serde_json::to_vec(value)?
        } else {
            text.into_bytes()
        };
        bytes.push(b'\n');
        let (done, received) = oneshot::channel();
        self.sender
            .send(Line { bytes, done })
            .await
            .context("stdout writer stopped")?;
        received.await.context("stdout writer stopped")??;
        Ok(())
    }
}

pub(super) fn download(item: &Download) -> String {
    let eta = item
        .eta_seconds
        .map(|eta| format!("{eta:.0}s"))
        .unwrap_or_else(|| "unknown".into());
    let total = item
        .total_bytes
        .map(|bytes| bytes.to_string())
        .unwrap_or_else(|| "unknown".into());
    let mut line = format!(
        "{} {} {} {}/{} bytes {:.0} B/s ETA {}",
        item.id,
        item.status.as_str(),
        item.filename.escape_debug(),
        item.downloaded_bytes,
        total,
        item.bytes_per_second,
        eta
    );
    if let Some(error) = &item.error {
        line.push_str(&format!(" — {}", error.escape_debug()));
    }
    line
}
