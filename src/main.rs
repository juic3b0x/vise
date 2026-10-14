use std::{net::SocketAddr, path::PathBuf, process::ExitCode, time::Duration};

use clap::Parser;
use tokio_util::sync::CancellationToken;
use vise::{
    cli,
    engine::{Config, Manager},
    telemetry, web,
};

#[derive(Debug, Parser)]
#[command(version, about = "Vise — concurrent, resumable downloads", long_about = None)]
struct Args {
    /// HTTP(S) URLs to enqueue
    urls: Vec<String>,
    /// Launch the local dashboard instead of the terminal controller
    #[arg(long)]
    webui: bool,
    /// Local WebUI listen address (loopback only)
    #[arg(long, default_value = "127.0.0.1:8080", requires = "webui")]
    bind: SocketAddr,
    /// Directory for downloaded files and the .vise journal
    #[arg(short, long, default_value = "downloads")]
    output: PathBuf,
    /// Number of downloads running at once
    #[arg(short = 'j', long, default_value_t = 3, value_parser = clap::value_parser!(u16).range(1..=64))]
    jobs: u16,
    /// Maximum parallel connections per download
    #[arg(short = 'c', long, default_value_t = 8, value_parser = clap::value_parser!(u16).range(1..=64))]
    connections: u16,
    /// Retries after the initial attempt, per segment
    #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u32).range(0..=20))]
    retries: u32,
    /// Idle read timeout, in seconds (not a total transfer limit)
    #[arg(long, default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..))]
    timeout: u64,
    /// Resume unfinished downloads saved in the output directory
    #[arg(long)]
    resume_all: bool,
    /// Keep the interactive command queue open even with URL arguments
    #[arg(short, long, conflicts_with = "webui")]
    interactive: bool,
    /// Run without reading terminal input
    #[arg(long, conflicts_with_all = ["interactive", "webui"])]
    batch: bool,
    /// Save a single URL under this filename
    #[arg(short = 'n', long)]
    name: Option<String>,
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "vise=warn".into()),
        )
        .init();
    match run(Args::parse()).await {
        Ok(code) => ExitCode::from(code),
        Err(error) => {
            eprintln!("Vise: {error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> anyhow::Result<u8> {
    anyhow::ensure!(
        args.name.is_none() || args.urls.len() == 1,
        "--name requires exactly one URL"
    );
    let interactive = !args.webui
        && !args.batch
        && (args.interactive
            || (args.urls.is_empty() && !args.resume_all && cli::stdin_is_terminal()));
    anyhow::ensure!(
        args.webui || interactive || !args.urls.is_empty() || args.resume_all,
        "provide a URL, use --resume-all, --interactive, or --webui"
    );
    if args.webui {
        anyhow::ensure!(
            args.bind.ip().is_loopback(),
            "--bind must use a loopback address"
        );
    }
    let manager = Manager::open(Config {
        output_dir: args.output,
        concurrent_downloads: args.jobs as usize,
        connections_per_download: args.connections as usize,
        retries: args.retries,
        read_timeout: Duration::from_secs(args.timeout),
    })
    .await?;
    let stop = CancellationToken::new();
    let telemetry_stop = CancellationToken::new();
    // Subscribe before seeding/resuming the queue so initial events cannot race
    // the logger startup. The default CLI never constructs a terminal logger.
    let mut terminal = if args.webui {
        let events = manager.subscribe_events();
        let terminal_manager = manager.clone();
        let token = telemetry_stop.clone();
        Some(tokio::spawn(async move {
            telemetry::run(terminal_manager, events, token).await
        }))
    } else {
        None
    };
    let monitor_stop = CancellationToken::new();
    let monitor_manager = manager.clone();
    let monitor_token = monitor_stop.clone();
    let mut monitor = tokio::spawn(async move { monitor_manager.monitor(monitor_token).await });
    let signal_stop = stop.clone();
    let signal = tokio::spawn(async move {
        match shutdown_signal().await {
            Ok(()) => signal_stop.cancel(),
            Err(error) => {
                tracing::error!(%error, "cannot install shutdown signal handler");
                signal_stop.cancel();
            }
        }
    });
    let mut monitor_consumed = false;
    let mut terminal_consumed = false;
    let result = async {
        if args.resume_all {
            manager.resume_all().await?;
        }
        for url in &args.urls {
            manager.add(url, args.name.as_deref()).await?;
        }
        let controller = async {
            if args.webui {
                web::run(manager.clone(), args.bind, stop.clone())
                    .await
                    .map(|_| true)
            } else {
                cli::run(manager.clone(), interactive, stop.clone())
                    .await
                    .map_err(anyhow::Error::from)
            }
        };
        tokio::select! {
            result = controller => result,
            result = &mut monitor => {
                monitor_consumed = true;
                stop.cancel();
                match result {
                    Ok(Err(error)) => Err(anyhow::anyhow!("state monitor failed: {error}")),
                    Err(error) => Err(anyhow::anyhow!("state monitor crashed: {error}")),
                    Ok(Ok(())) => Err(anyhow::anyhow!("state monitor stopped unexpectedly")),
                }
            }
            result = async {
                match terminal.as_mut() {
                    Some(handle) => handle.await,
                    None => std::future::pending().await,
                }
            } => {
                terminal_consumed = true;
                stop.cancel();
                match result {
                    Ok(Err(error)) => Err(anyhow::anyhow!("terminal monitor failed: {error}")),
                    Err(error) => Err(anyhow::anyhow!("terminal monitor crashed: {error}")),
                    Ok(Ok(())) => Err(anyhow::anyhow!("terminal monitor stopped unexpectedly")),
                }
            }
        }
    }
    .await;
    let interrupted = stop.is_cancelled();
    stop.cancel();
    // Stop periodic snapshots before the final quiescent checkpoint.
    monitor_stop.cancel();
    let monitor_result = if monitor_consumed {
        Ok(())
    } else {
        monitor
            .await
            .map_err(anyhow::Error::from)
            .and_then(|r| r.map_err(anyhow::Error::from))
    };
    signal.abort();
    let shutdown = manager.shutdown().await;
    if args.webui {
        if shutdown.is_ok() {
            manager.server_stopped();
        } else if let Err(error) = &shutdown {
            manager.publish_shutdown_error(error.to_string());
        }
    }
    // Keep the logger alive through shutdown, then flush its remaining events.
    telemetry_stop.cancel();
    let terminal_result = if terminal_consumed {
        Ok(())
    } else if let Some(handle) = terminal {
        handle
            .await
            .map_err(anyhow::Error::from)
            .and_then(|r| r.map_err(anyhow::Error::from))
    } else {
        Ok(())
    };
    shutdown?;
    monitor_result?;
    terminal_result?;
    let success = result?;
    Ok(if interrupted {
        130
    } else if success {
        0
    } else {
        1
    })
}

async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! { result = tokio::signal::ctrl_c() => result, _ = terminate.recv() => Ok(()) }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}
