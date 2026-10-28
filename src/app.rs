//! Controller supervision and ordered shutdown for CLI, WebUI, and daemon sessions.
use super::Args;
use anyhow::Context;
use std::{error::Error, future::Future, net::SocketAddr};
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;
use vise::{checksum::ExpectedChecksums, cli, engine::Manager, telemetry, web};

pub(super) enum Controller {
    Cli,
    WebUi(SocketAddr),
    Daemon(web::DaemonConfig),
}

pub(super) async fn run_session(
    manager: Manager,
    args: &Args,
    interactive: bool,
    expected: ExpectedChecksums,
    controller: Controller,
) -> anyhow::Result<u8> {
    let mut background = Background::start(&manager, args.webui)?;
    let result = async {
        seed_queue(&manager, args, expected).await?;
        let controller = run_controller(&manager, interactive, background.stop.clone(), controller);
        background.supervise(controller).await
    }
    .await;
    let interrupted = background.stop.is_cancelled();
    background
        .shutdown(&manager, args.webui || args.daemon)
        .await?;
    let success = result?;
    Ok(if interrupted {
        130
    } else if success {
        0
    } else {
        1
    })
}

async fn seed_queue(
    manager: &Manager,
    args: &Args,
    expected: ExpectedChecksums,
) -> anyhow::Result<()> {
    manager
        .validate_request_batch(&args.urls, &Default::default())
        .await?;
    if args.resume_all {
        manager.resume_all().await?;
    }
    for url in &args.urls {
        manager
            .add_with_checksums(url, args.name.as_deref(), expected.clone())
            .await?;
    }
    Ok(())
}

async fn run_controller(
    manager: &Manager,
    interactive: bool,
    stop: CancellationToken,
    controller: Controller,
) -> anyhow::Result<bool> {
    match controller {
        Controller::Daemon(config) => web::run_daemon(manager.clone(), config, stop)
            .await
            .map(|_| true),
        Controller::WebUi(bind) => web::run(manager.clone(), bind, stop).await.map(|_| true),
        Controller::Cli => cli::run(manager.clone(), interactive, stop)
            .await
            .map_err(anyhow::Error::from),
    }
}

struct Background {
    stop: CancellationToken,
    monitor_stop: CancellationToken,
    telemetry_stop: CancellationToken,
    monitor: JoinHandle<vise::error::Result<()>>,
    terminal: Option<JoinHandle<std::io::Result<()>>>,
    signal: JoinHandle<()>,
    monitor_consumed: bool,
    terminal_consumed: bool,
}

impl Background {
    fn start(manager: &Manager, webui: bool) -> anyhow::Result<Self> {
        // Register OS listeners before a controller can accept commands. Merely
        // spawning an async registration task leaves a startup window in which
        // SIGTERM still invokes the operating system's default termination.
        let shutdown = shutdown_signal().context("cannot install shutdown signal handlers")?;
        let stop = CancellationToken::new();
        let telemetry_stop = CancellationToken::new();
        // Subscribe before seeding so even an immediately completed task is logged.
        // CLI and daemon modes never start a terminal telemetry task.
        let terminal = if webui {
            let events = manager.subscribe_events();
            let manager = manager.clone();
            let token = telemetry_stop.clone();
            Some(tokio::spawn(async move {
                telemetry::run(manager, events, token).await
            }))
        } else {
            None
        };
        let monitor_stop = CancellationToken::new();
        let monitor_manager = manager.clone();
        let token = monitor_stop.clone();
        let monitor = tokio::spawn(async move { monitor_manager.monitor(token).await });
        let signal_stop = stop.clone();
        let signal = tokio::spawn(async move {
            shutdown.await;
            signal_stop.cancel();
        });
        Ok(Self {
            stop,
            monitor_stop,
            telemetry_stop,
            monitor,
            terminal,
            signal,
            monitor_consumed: false,
            terminal_consumed: false,
        })
    }

    async fn supervise(
        &mut self,
        controller: impl Future<Output = anyhow::Result<bool>>,
    ) -> anyhow::Result<bool> {
        tokio::select! {
            result = controller => result,
            result = &mut self.monitor => {
                self.monitor_consumed = true;
                self.stop.cancel();
                Err(unexpected_exit(result, "state monitor"))
            }
            result = terminal_exit(&mut self.terminal) => {
                self.terminal_consumed = true;
                self.stop.cancel();
                Err(unexpected_exit(result, "terminal monitor"))
            }
        }
    }

    async fn shutdown(self, manager: &Manager, server: bool) -> anyhow::Result<()> {
        self.stop.cancel();
        // Stop periodic snapshots before the final quiescent checkpoint.
        self.monitor_stop.cancel();
        let monitor_result = join_if_pending(self.monitor, self.monitor_consumed).await;
        self.signal.abort();
        let shutdown = manager.shutdown().await;
        if server {
            publish_shutdown(manager, &shutdown);
        }
        // Keep telemetry alive through shutdown, then flush remaining events.
        self.telemetry_stop.cancel();
        let terminal_result = match self.terminal {
            Some(handle) => join_if_pending(handle, self.terminal_consumed).await,
            None => Ok(()),
        };
        // Await every task before propagating errors, preserving error priority.
        shutdown?;
        monitor_result?;
        terminal_result
    }
}

async fn terminal_exit(
    terminal: &mut Option<JoinHandle<std::io::Result<()>>>,
) -> Result<std::io::Result<()>, JoinError> {
    match terminal {
        Some(handle) => handle.await,
        None => std::future::pending().await,
    }
}

fn unexpected_exit<E: std::fmt::Display>(
    result: Result<Result<(), E>, JoinError>,
    name: &str,
) -> anyhow::Error {
    match result {
        Ok(Err(error)) => anyhow::anyhow!("{name} failed: {error}"),
        Err(error) => anyhow::anyhow!("{name} crashed: {error}"),
        Ok(Ok(())) => anyhow::anyhow!("{name} stopped unexpectedly"),
    }
}

async fn join_if_pending<E: Error + Send + Sync + 'static>(
    handle: JoinHandle<Result<(), E>>,
    consumed: bool,
) -> anyhow::Result<()> {
    if !consumed {
        handle.await??;
    }
    Ok(())
}

fn publish_shutdown(manager: &Manager, result: &vise::error::Result<()>) {
    match result {
        Ok(()) => manager.server_stopped(),
        Err(error) => manager.publish_shutdown_error(error.to_string()),
    }
}

#[cfg(unix)]
pub(super) fn shutdown_signal() -> std::io::Result<impl Future<Output = ()>> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    Ok(async move {
        tokio::select! { _ = interrupt.recv() => (), _ = terminate.recv() => () }
    })
}

#[cfg(windows)]
pub(super) fn shutdown_signal() -> std::io::Result<impl Future<Output = ()>> {
    let mut interrupt = tokio::signal::windows::ctrl_c()?;
    Ok(async move {
        interrupt.recv().await;
    })
}
