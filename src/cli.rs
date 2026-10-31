use std::{
    io::{BufRead, IsTerminal},
    time::Duration,
};

use indicatif::{MultiProgress, ProgressDrawTarget};

mod progress;
pub use progress::ProgressDisplay;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    engine::Manager,
    error::{Error, Result},
    model::Status,
};

pub async fn run(manager: Manager, interactive: bool, stop: CancellationToken) -> Result<bool> {
    if !interactive && ProgressDrawTarget::stderr().is_hidden() {
        return run_hidden(manager, stop).await;
    }
    let mut display = ProgressDisplay::default();
    let progress = display.messages();
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
        display.render(&downloads)?;
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
    display.finish();
    session_result(&manager, &progress, cancelled).await
}

async fn run_hidden(manager: Manager, stop: CancellationToken) -> Result<bool> {
    // Subscribe before checking: watch coalescing cannot lose the last wakeup.
    let mut events = manager.subscribe();
    let mut cancelled = false;
    while manager.session_pending() {
        tokio::select! {
            _ = stop.cancelled() => { cancelled = true; break; }
            changed = events.changed() => {
                changed.map_err(|_| Error::Worker("progress channel closed".into()))?;
            }
        }
    }
    // Keep the existing redirected-message policy without constructing bars.
    let messages = MultiProgress::with_draw_target(ProgressDrawTarget::hidden());
    session_result(&manager, &messages, cancelled).await
}

async fn session_result(
    manager: &Manager,
    progress: &MultiProgress,
    cancelled: bool,
) -> Result<bool> {
    let downloads = manager.session_downloads().await;
    for item in &downloads {
        if matches!(item.status, Status::Failed | Status::Corrupt) {
            progress.println(format!(
                "{}: {}",
                item.filename,
                item.error.as_deref().unwrap_or("download failed")
            ))?;
        }
    }
    Ok(cancelled || downloads.iter().all(|d| d.status == Status::Completed))
}

async fn command(manager: &Manager, progress: &MultiProgress, input: &str) -> Result<bool> {
    let input = input.trim();
    if input.is_empty() {
        return Ok(false);
    }
    let (verb, rest) = input.split_once(char::is_whitespace).unwrap_or((input, ""));
    match verb {
        "quit" | "exit" => return Ok(true),
        "help" => progress.println("add URL [FILENAME] · pause ID · resume ID · retry ID · limit-rate RATE (or off) · list · quit")?,
        "limit-rate" => configure_rate(manager, progress, rest.trim()).await?,
        "list" => list_downloads(manager, progress).await?,
        "pause" | "resume" | "retry" => control_download(manager, verb, rest.trim()).await?,
        "add" => add_download(manager, progress, rest.trim()).await?,
        _ => add_bare_url(manager, progress, input).await?,
    }
    Ok(false)
}

async fn configure_rate(manager: &Manager, progress: &MultiProgress, rate: &str) -> Result<()> {
    let configuration = if rate.is_empty() {
        manager.rate_configuration()
    } else {
        manager.set_rate_limit(rate.parse()?).await?
    };
    progress.println(format!(
        "Global bandwidth limit: {}",
        configuration.limit_rate
    ))?;
    Ok(())
}

async fn list_downloads(manager: &Manager, progress: &MultiProgress) -> Result<()> {
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
    Ok(())
}

async fn control_download(manager: &Manager, verb: &str, prefix: &str) -> Result<()> {
    let id = resolve_id(manager, prefix).await?;
    match verb {
        "pause" => manager.pause(id).await,
        "retry" => manager.retry(id).await,
        _ => manager.resume(id).await,
    }
}

async fn add_download(manager: &Manager, progress: &MultiProgress, input: &str) -> Result<()> {
    let (url, filename) = input
        .split_once(char::is_whitespace)
        .map(|(url, name)| (url, Some(name.trim())))
        .unwrap_or((input, None));
    let item = manager.add(url, filename).await?;
    progress.println(format!("Queued {}: {}", item.id, item.filename))?;
    Ok(())
}

async fn add_bare_url(manager: &Manager, progress: &MultiProgress, input: &str) -> Result<()> {
    if !input.starts_with("http://") && !input.starts_with("https://") {
        return Err(Error::Invalid("unknown command; type help".into()));
    }
    let item = manager.add(input, None).await?;
    progress.println(format!("Queued {}: {}", item.id, item.filename))?;
    Ok(())
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
    async fn hidden_batch_skips_display_work_and_handles_coalesced_or_early_completion() {
        use axum::{Router, routing::get};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/ok", get(|| async { "payload" })),
            )
            .await
            .unwrap();
        });
        for early in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let manager = Manager::open(crate::engine::Config {
                home_dir: Some(root.path().join("home")),
                output_dir: root.path().join("out"),
                ..Default::default()
            })
            .await
            .unwrap();
            manager
                .add_input(
                    vec![
                        format!("http://{address}/ok"),
                        format!("http://{address}/missing"),
                    ],
                    None,
                    Default::default(),
                )
                .await
                .unwrap();
            if early {
                tokio::time::timeout(Duration::from_secs(10), async {
                    while manager.session_pending() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
            }
            let before = progress::display_work();
            let success = tokio::time::timeout(
                Duration::from_secs(10),
                run_hidden(manager.clone(), CancellationToken::new()),
            )
            .await
            .unwrap()
            .unwrap();
            assert!(!success, "a failed download must make batch mode fail");
            assert_eq!(
                progress::display_work(),
                before,
                "hidden batch constructed or rendered progress bars"
            );
            assert_eq!(
                std::fs::read(
                    manager
                        .session_downloads()
                        .await
                        .into_iter()
                        .find(|item| item.status == Status::Completed)
                        .unwrap()
                        .output_path
                )
                .unwrap(),
                b"payload"
            );
            manager.shutdown().await.unwrap();
        }
        server.abort();
        let _ = server.await;
    }
    #[tokio::test]
    async fn runtime_rate_commands_update_and_clear_the_shared_limiter() {
        let dir = tempfile::tempdir().unwrap();
        let manager = Manager::open(crate::engine::Config {
            output_dir: dir.path().into(),
            home_dir: Some(dir.path().join("test-home")),
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
