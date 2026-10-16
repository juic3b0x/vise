use std::{
    collections::BTreeMap,
    io::{BufRead, IsTerminal},
    time::Duration,
};

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    engine::Manager,
    error::{Error, Result},
    model::{Download, Status},
};

pub async fn run(manager: Manager, interactive: bool, stop: CancellationToken) -> Result<bool> {
    let progress = MultiProgress::new();
    let mut bars = BTreeMap::<Uuid, ProgressBar>::new();
    let mut events = manager.subscribe();
    let (sender, mut input) = mpsc::channel(64);
    if interactive {
        // Tokio's stdin uses an uncancellable blocking-pool read, which can hold
        // runtime shutdown open. A dedicated detached OS thread exits with the
        // process and sends lines through a bounded, cancellable receiver.
        std::thread::Builder::new()
            .name("vise-stdin".into())
            .spawn(move || {
                let stdin = std::io::stdin();
                for line in stdin.lock().lines() {
                    let failed = line.is_err();
                    if sender.blocking_send(line).is_err() || failed {
                        break;
                    }
                }
            })?;
    } else {
        drop(sender);
    }
    let mut input_open = interactive;
    if interactive {
        progress.println("Vise • add URL [FILENAME] | pause ID | resume ID | retry ID | limit-rate RATE | list | quit\nPaste a URL to queue it. IDs may be shortened to a unique prefix.")?;
    }
    let mut interval = tokio::time::interval(Duration::from_millis(250));
    let mut cancelled = false;
    loop {
        let downloads = manager.list().await;
        render(&progress, &mut bars, &downloads)?;
        if !input_open && downloads.iter().all(|d| !d.status.active()) {
            break;
        }
        tokio::select! {
            _ = stop.cancelled() => { cancelled = true; break; }
            _ = interval.tick() => (),
            changed = events.changed() => {
                if changed.is_err() { return Err(Error::Worker("progress channel closed".into())); }
            }
            line = input.recv(), if input_open => {
                match line {
                    None => { input_open = false; },
                    Some(line) => {
                        let line = line?;
                        match command(&manager, &progress, &line).await {
                            Ok(true) => { cancelled = true; break; },
                            Ok(false) => (),
                            Err(error) => progress.println(format!("Error: {error}"))?,
                        }
                    }
                }
            }
        }
    }
    for bar in bars.values() {
        if !bar.is_finished() {
            bar.finish_and_clear();
        }
    }
    let downloads = manager.list().await;
    for item in &downloads {
        if item.status == Status::Failed {
            progress.println(format!(
                "{}: {}",
                item.filename,
                item.error.as_deref().unwrap_or("download failed")
            ))?;
        }
    }
    Ok(cancelled || downloads.iter().all(|d| d.status == Status::Completed))
}

fn render(
    progress: &MultiProgress,
    bars: &mut BTreeMap<Uuid, ProgressBar>,
    downloads: &[Download],
) -> Result<()> {
    for item in downloads {
        let bar = bars.entry(item.id).or_insert_with(|| {
            let bar = match item.total_bytes {
                Some(total) => ProgressBar::new(total),
                None => ProgressBar::new_spinner(),
            };
            progress.add(bar)
        });
        let template = if item.total_bytes.is_some() {
            "{prefix:.cyan} {msg} [{bar:28.cyan/blue}] {percent:>3}% {bytes}/{total_bytes}"
        } else {
            "{prefix:.cyan} {msg} {spinner:.cyan} {bytes}"
        };
        bar.set_style(
            ProgressStyle::with_template(template)
                .map_err(|e| Error::Invalid(e.to_string()))?
                .progress_chars("=>-"),
        );
        bar.set_prefix(format!("{} {}", &item.id.to_string()[..8], item.filename));
        if let Some(total) = item.total_bytes {
            bar.set_length(total);
        }
        bar.set_position(item.downloaded_bytes);
        let eta = item
            .eta_seconds
            .map(|s| format!("{}s", s.ceil() as u64))
            .unwrap_or_else(|| "—".into());
        bar.set_message(format!(
            "{:?} · {}/s · ETA {}",
            item.status,
            bytes(item.bytes_per_second),
            eta
        ));
        bar.tick();
        if matches!(
            item.status,
            Status::Completed | Status::Failed | Status::Paused
        ) {
            if !bar.is_finished() {
                bar.finish();
            }
        } else if bar.is_finished() {
            bar.reset();
            bar.set_position(item.downloaded_bytes);
        }
    }
    Ok(())
}

fn bytes(value: f64) -> String {
    if value >= 1024.0 * 1024.0 {
        format!("{:.1} MiB", value / (1024.0 * 1024.0))
    } else if value >= 1024.0 {
        format!("{:.1} KiB", value / 1024.0)
    } else {
        format!("{value:.0} B")
    }
}

async fn command(manager: &Manager, progress: &MultiProgress, input: &str) -> Result<bool> {
    let input = input.trim();
    if input.is_empty() {
        return Ok(false);
    }
    let (verb, rest) = input.split_once(char::is_whitespace).unwrap_or((input, ""));
    match verb {
        "quit" | "exit" => return Ok(true),
        "help" => progress
            .println("add URL [FILENAME] · pause ID · resume ID · retry ID · limit-rate RATE (or off) · list · quit")?,
        "limit-rate" => {
            let configuration = if rest.trim().is_empty() { manager.rate_configuration() }
                else { manager.set_rate_limit(rest.trim().parse()?).await? };
            progress.println(format!("Global bandwidth limit: {}", configuration.limit_rate))?;
        }
        "list" => {
            for item in manager.list().await {
                progress.println(format!(
                    "{} {:?} {} ({}/{}){}",
                    item.id,
                    item.status,
                    item.filename,
                    item.downloaded_bytes,
                    item.total_bytes
                        .map(|n| n.to_string())
                        .unwrap_or_else(|| "?".into()),
                    item.error.map(|e| format!(" — {e}")).unwrap_or_default()
                ))?;
            }
        }
        "pause" | "resume" | "retry" => {
            let id = resolve_id(manager, rest.trim()).await?;
            if verb == "pause" {
                manager.pause(id).await?;
            } else {
                manager.resume(id).await?;
            }
        }
        "add" => {
            let rest = rest.trim();
            let (url, filename) = rest
                .split_once(char::is_whitespace)
                .map(|(url, name)| (url, Some(name.trim())))
                .unwrap_or((rest, None));
            let item = manager.add(url, filename).await?;
            progress.println(format!("Queued {}: {}", item.id, item.filename))?;
        }
        _ if input.starts_with("http://") || input.starts_with("https://") => {
            let item = manager.add(input, None).await?;
            progress.println(format!("Queued {}: {}", item.id, item.filename))?;
        }
        _ => return Err(Error::Invalid("unknown command; type help".into())),
    }
    Ok(false)
}

async fn resolve_id(manager: &Manager, prefix: &str) -> Result<Uuid> {
    if prefix.len() < 4 {
        return Err(Error::Invalid(
            "use at least four characters of a download ID".into(),
        ));
    }
    let matches: Vec<_> = manager
        .list()
        .await
        .into_iter()
        .filter(|d| d.id.to_string().starts_with(prefix))
        .collect();
    match matches.as_slice() {
        [item] => Ok(item.id),
        [] => Err(Error::Invalid("no download matches that ID".into())),
        _ => Err(Error::Invalid("ambiguous download ID".into())),
    }
}

pub fn stdin_is_terminal() -> bool {
    std::io::stdin().is_terminal()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn runtime_rate_commands_update_and_clear_the_shared_limiter() {
        let dir = tempfile::tempdir().unwrap();
        let manager = Manager::open(crate::engine::Config {
            output_dir: dir.path().into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let progress = MultiProgress::with_draw_target(indicatif::ProgressDrawTarget::hidden());
        command(&manager, &progress, "limit-rate 1.5M")
            .await
            .unwrap();
        assert_eq!(
            manager.rate_configuration().limit_bytes_per_second,
            Some(1_572_864)
        );
        assert!(
            command(&manager, &progress, "limit-rate nonsense")
                .await
                .is_err()
        );
        assert_eq!(
            manager.rate_configuration().limit_bytes_per_second,
            Some(1_572_864)
        );
        command(&manager, &progress, "limit-rate off")
            .await
            .unwrap();
        assert_eq!(manager.rate_configuration().limit_bytes_per_second, None);
        manager.shutdown().await.unwrap();
    }
}
