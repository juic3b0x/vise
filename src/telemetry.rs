use std::{
    io::IsTerminal,
    time::{Duration, SystemTime},
};

use chrono::{DateTime, Utc};
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    sync::broadcast,
};
use tokio_util::sync::CancellationToken;

use crate::{
    engine::Manager,
    events::{Event, EventKind},
    model::{Download, Status},
};

#[derive(Clone, Copy)]
pub struct Options {
    pub interval: Duration,
    pub color: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(1),
            color: std::io::stdout().is_terminal()
                && std::env::var_os("NO_COLOR").is_none()
                && std::env::var("TERM").as_deref() != Ok("dumb"),
        }
    }
}

/// Used only by main's --webui branch. One writer owns all stdout monitor output.
pub async fn run(
    manager: Manager,
    events: broadcast::Receiver<Event>,
    stop: CancellationToken,
) -> std::io::Result<()> {
    run_with_writer(
        manager,
        events,
        stop,
        tokio::io::stdout(),
        Options::default(),
    )
    .await
}

pub async fn run_with_writer<W: AsyncWrite + Unpin>(
    manager: Manager,
    mut events: broadcast::Receiver<Event>,
    stop: CancellationToken,
    mut writer: W,
    options: Options,
) -> std::io::Result<()> {
    if options.interval.is_zero() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "telemetry interval must be positive",
        ));
    }
    write_line(
        &mut writer,
        line(
            SystemTime::now(),
            "MONITOR",
            36,
            "Browser controls downloads · live terminal telemetry · Ctrl+C to stop",
            options.color,
        ),
    )
    .await?;
    let mut interval = tokio::time::interval(options.interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = stop.cancelled() => break,
            _ = interval.tick() => write_status(&manager, &mut writer, options.color).await?,
            event = events.recv() => {
                match event {
                    Ok(event) => write_line(&mut writer, format_event(&event, options.color)).await?,
                    Err(broadcast::error::RecvError::Lagged(count)) => {
                        write_lag(&mut writer, count, options.color).await?;
                        write_status(&manager, &mut writer, options.color).await?;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }
    // Main cancels this logger only after workers have stopped, so drain the final
    // PAUSED/DONE/STOPPED events rather than racing shutdown against the receiver.
    loop {
        match events.try_recv() {
            Ok(event) => write_line(&mut writer, format_event(&event, options.color)).await?,
            Err(broadcast::error::TryRecvError::Lagged(count)) => {
                write_lag(&mut writer, count, options.color).await?
            }
            Err(broadcast::error::TryRecvError::Empty | broadcast::error::TryRecvError::Closed) => {
                break;
            }
        }
    }
    writer.flush().await
}

async fn write_status<W: AsyncWrite + Unpin>(
    manager: &Manager,
    writer: &mut W,
    color: bool,
) -> std::io::Result<()> {
    // list() returns owned snapshots: no application locks survive terminal I/O.
    for download in manager
        .list()
        .await
        .into_iter()
        .filter(|d| d.status.active())
    {
        write_line(
            writer,
            line(
                SystemTime::now(),
                "STATUS",
                34,
                &format!("{} · {}", identity(&download), metrics(&download)),
                color,
            ),
        )
        .await?;
    }
    Ok(())
}

async fn write_lag<W: AsyncWrite + Unpin>(
    writer: &mut W,
    count: u64,
    color: bool,
) -> std::io::Result<()> {
    write_line(
        writer,
        line(
            SystemTime::now(),
            "WARN",
            33,
            &format!("terminal fell behind; {count} events missed · refreshing active status"),
            color,
        ),
    )
    .await
}

async fn write_line<W: AsyncWrite + Unpin>(writer: &mut W, text: String) -> std::io::Result<()> {
    writer.write_all(text.as_bytes()).await?;
    // Event lines remain immediate even when stdout is redirected to a file/pipe.
    writer.flush().await
}

fn format_event(event: &Event, color: bool) -> String {
    let identity = event.download.as_ref().map(identity).unwrap_or_default();
    let progress = event.download.as_ref().map(metrics).unwrap_or_default();
    let (label, code, message) = match &event.kind {
        EventKind::Added => ("ADDED", 36, format!("{identity} · queued")),
        EventKind::PauseRequested => ("PAUSE", 33, format!("{identity} · stopping workers")),
        EventKind::Paused => ("PAUSED", 33, format!("{identity} · {progress}")),
        EventKind::Resumed => ("RESUMED", 36, format!("{identity} · {progress}")),
        EventKind::DeleteRequested => (
            "DELETE",
            35,
            format!("{identity} · stopping workers and removing queue entry"),
        ),
        EventKind::Deleted => (
            "DELETED",
            35,
            format!("{identity} · removed from queue; completed output kept"),
        ),
        EventKind::Completed => ("DONE", 32, format!("{identity} · {progress}")),
        EventKind::Failed => (
            "FAILED",
            31,
            format!(
                "{identity} · {}",
                event
                    .download
                    .as_ref()
                    .and_then(|d| d.error.as_deref())
                    .map(clean)
                    .unwrap_or_else(|| "download failed".into())
            ),
        ),
        EventKind::StateChanged { from, to } => (
            "STATE",
            36,
            format!("{identity} · {} → {}", state(*from), state(*to)),
        ),
        EventKind::Retrying {
            chunk,
            attempt,
            error,
        } => (
            "RETRY",
            33,
            format!(
                "{identity} · segment {} attempt {attempt} · {}",
                chunk + 1,
                clean(error)
            ),
        ),
        EventKind::Warning { message } => ("WARN", 33, format!("{identity} · {}", clean(message))),
        EventKind::ServerStarted { address } => ("READY", 32, format!("WebUI: {}", clean(address))),
        EventKind::ServerStopped => (
            "STOPPED",
            33,
            "server stopped · transfer state saved".into(),
        ),
    };
    line(event.timestamp, label, code, &message, color)
}

fn line(timestamp: SystemTime, label: &str, code: u8, message: &str, color: bool) -> String {
    let timestamp: DateTime<Utc> = timestamp.into();
    let timestamp = timestamp.format("%Y-%m-%dT%H:%M:%S%.3fZ");
    if color {
        format!("\x1b[90m[{timestamp}]\x1b[0m \x1b[{code}m{label:<8}\x1b[0m {message}\n")
    } else {
        format!("[{timestamp}] {label:<8} {message}\n")
    }
}

fn identity(download: &Download) -> String {
    format!(
        "{} {}",
        &download.id.to_string()[..8],
        clean(&download.filename)
    )
}

fn metrics(download: &Download) -> String {
    let percentage = match download.total_bytes {
        Some(0) if download.status == Status::Completed => "100.0%".into(),
        Some(0) => "0.0%".into(),
        Some(total) => format!(
            "{:.1}%",
            (download.downloaded_bytes as f64 / total as f64 * 100.0).clamp(0.0, 100.0)
        ),
        None => "unknown%".into(),
    };
    let total = download
        .total_bytes
        .map(|n| bytes(n as f64))
        .unwrap_or_else(|| "unknown".into());
    let eta = download
        .eta_seconds
        .map(|n| format!("{:.0}s", n.ceil()))
        .unwrap_or_else(|| "—".into());
    format!(
        "{} · {percentage} · {}/{} · {}/s · ETA {eta}",
        state(download.status),
        bytes(download.downloaded_bytes as f64),
        total,
        bytes(download.bytes_per_second)
    )
}

fn state(status: Status) -> &'static str {
    match status {
        Status::Queued => "queued",
        Status::Probing => "probing",
        Status::Downloading => "downloading",
        Status::Paused => "paused",
        Status::Completed => "completed",
        Status::Failed => "failed",
    }
}

fn bytes(mut value: f64) -> String {
    let units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];
    let mut index = 0;
    while value >= 1024.0 && index < units.len() - 1 {
        value /= 1024.0;
        index += 1;
    }
    format!("{value:.1} {}", units[index])
}

fn clean(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    for c in value.chars() {
        if c.is_control() {
            result.extend(c.escape_default());
        } else {
            result.push(c);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn events_are_timestamped_colorized_and_terminal_safe() {
        let download = Download {
            id: Uuid::nil(),
            url: String::new(),
            filename: "test\n\x1b[31m.bin".into(),
            status: Status::Completed,
            total_bytes: Some(1024),
            downloaded_bytes: 1024,
            bytes_per_second: 0.0,
            eta_seconds: None,
            error: None,
        };
        let event = Event {
            timestamp: SystemTime::UNIX_EPOCH,
            kind: EventKind::Completed,
            download: Some(download),
        };
        let plain = format_event(&event, false);
        assert!(plain.starts_with("[1970-01-01T00:00:00.000Z] DONE"));
        assert!(plain.contains("100.0%"));
        assert_eq!(plain.lines().count(), 1);
        assert!(!plain.contains('\x1b'));
        assert!(format_event(&event, true).contains("\x1b[32mDONE"));
    }

    #[tokio::test]
    async fn shutdown_drains_pending_events() {
        let dir = tempfile::tempdir().unwrap();
        let manager = Manager::open(crate::engine::Config {
            output_dir: dir.path().into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let receiver = manager.subscribe_events();
        manager.server_started("http://127.0.0.1:8080".into());
        manager.server_stopped();
        let stop = CancellationToken::new();
        stop.cancel();
        let mut output = Vec::new();
        run_with_writer(
            manager,
            receiver,
            stop,
            &mut output,
            Options {
                interval: Duration::from_secs(1),
                color: false,
            },
        )
        .await
        .unwrap();
        let text = String::from_utf8(output).unwrap();
        assert!(text.contains("READY"));
        assert!(text.contains("STOPPED"));
        assert!(!text.contains("STATUS"));
    }
}
