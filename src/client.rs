//! Explicit daemon client. This module never opens a Manager or SQLite journal.
use clap::{Args as ClapArgs, Subcommand};
use vise::rate_limiter::RateLimit;

mod api;
mod commands;
mod output;
mod settings;

#[derive(ClapArgs)]
pub(super) struct Args {
    /// Daemon origin, e.g. http://127.0.0.1:8080 (HTTPS required off loopback)
    #[arg(long, global = true, value_name = "URL")]
    endpoint: Option<String>,
    /// Machine-readable JSON; watch emits one telemetry object per line
    #[arg(long, global = true)]
    json: bool,
    /// Timeout for each API request; does not limit the total --wait duration
    #[arg(long, global = true, default_value_t = 15, value_parser = clap::value_parser!(u64).range(1..=300))]
    request_timeout: u64,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Queue a URL in the daemon (paths and cookies use daemon-side defaults)
    Add(Add),
    /// List the daemon's saved and active downloads
    List,
    /// Inspect one download (full UUID or unique prefix of at least 4 characters)
    Get { id: String },
    /// Pause a download and preserve its partial data
    Pause { id: String },
    /// Resume a paused or failed download
    Resume { id: String },
    /// Retry a task; corrupt downloads restart all chunks
    Retry { id: String },
    /// Remove a task and its partial data; completed output files are kept
    Delete { id: String },
    /// Show or change the shared bandwidth cap (e.g. 5M or off)
    LimitRate { rate: Option<RateLimit> },
    /// Monitor telemetry until interrupted; exiting leaves the daemon running
    Watch,
}

#[derive(ClapArgs)]
struct Add {
    /// HTTP(S) source URL
    url: String,
    /// Plain output filename, not a path
    #[arg(short = 'n', long)]
    name: Option<String>,
    #[arg(long)]
    expected_sha256: Option<String>,
    #[arg(long)]
    expected_md5: Option<String>,
    /// Download proxy route; never used for the API connection
    #[arg(long)]
    proxy: Option<String>,
    /// Wait for verified publication; a failed, corrupt, or paused task exits 2
    #[arg(long)]
    wait: bool,
}

pub(super) async fn run(args: Args) -> anyhow::Result<u8> {
    let shutdown = crate::app::shutdown_signal()?;
    let result = tokio::select! {
        result = commands::execute(args) => result,
        _ = shutdown => return Ok(130),
    };
    match result {
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|e| e.kind() == std::io::ErrorKind::BrokenPipe) =>
        {
            Ok(0)
        }
        other => other,
    }
}
