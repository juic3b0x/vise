use std::{net::SocketAddr, path::PathBuf, process::ExitCode, time::Duration};

use clap::Parser;
use vise::{
    checksum::ExpectedChecksums,
    cli,
    engine::{Config, Manager},
    proxy::ProxyUrl,
    rate_limiter::RateLimit,
    web::DaemonConfig,
};

mod app;

#[derive(Parser)]
#[command(version, about = "Vise — concurrent, resumable downloads", long_about = None)]
#[command(group(clap::ArgGroup::new("server").args(["webui", "daemon"])))]
struct Args {
    /// HTTP(S) URLs to enqueue
    urls: Vec<String>,
    /// Launch the local dashboard instead of the terminal controller
    #[arg(long)]
    webui: bool,
    /// Run the API and dashboard quietly for a service manager (no terminal input)
    #[arg(short = 'd', long)]
    daemon: bool,
    /// Server listen address; non-loopback daemon binds require VISE_API_TOKEN
    #[arg(long, default_value = "127.0.0.1:8080", requires = "server")]
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
    #[arg(short, long, conflicts_with_all = ["webui", "daemon"])]
    interactive: bool,
    /// Run without reading terminal input
    #[arg(long, conflicts_with_all = ["interactive", "webui", "daemon"])]
    batch: bool,
    /// Save a single URL under this filename
    #[arg(short = 'n', long)]
    name: Option<String>,
    /// Global body-byte rate, e.g. 500K or 5M; off disables a saved limit
    #[arg(long)]
    limit_rate: Option<RateLimit>,
    /// Expected SHA-256 digest for exactly one URL (64 hexadecimal digits)
    #[arg(long)]
    expected_sha256: Option<String>,
    /// Expected MD5 digest for exactly one URL (32 hexadecimal digits)
    #[arg(long)]
    expected_md5: Option<String>,
    /// Default proxy for new downloads: http(s)://host:port or socks5(h)://host:port
    #[arg(long)]
    proxy: Option<String>,
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
            let _ = report_error(&mut std::io::stderr().lock(), &error);
            ExitCode::FAILURE
        }
    }
}

fn report_error(writer: &mut impl std::io::Write, error: &anyhow::Error) -> std::io::Result<()> {
    writeln!(writer, "Vise: {error:#}")
}

async fn run(args: Args) -> anyhow::Result<u8> {
    let proxy_url = args
        .proxy
        .as_deref()
        .map(str::parse::<ProxyUrl>)
        .transpose()?;
    let expected = ExpectedChecksums::new(
        args.expected_sha256.as_deref(),
        args.expected_md5.as_deref(),
    )?;
    anyhow::ensure!(
        expected.is_empty() || args.urls.len() == 1,
        "checksum flags require exactly one URL"
    );
    anyhow::ensure!(
        args.name.is_none() || args.urls.len() == 1,
        "--name requires exactly one URL"
    );
    let interactive = !args.webui
        && !args.daemon
        && !args.batch
        && (args.interactive
            || (args.urls.is_empty() && !args.resume_all && cli::stdin_is_terminal()));
    anyhow::ensure!(
        args.webui || args.daemon || interactive || !args.urls.is_empty() || args.resume_all,
        "provide a URL, use --resume-all, --interactive, --webui, or --daemon"
    );
    if args.webui {
        anyhow::ensure!(
            args.bind.ip().is_loopback(),
            "--bind must use a loopback address"
        );
    }
    let daemon = if args.daemon {
        let token = match std::env::var("VISE_API_TOKEN") {
            Ok(token) => Some(token),
            Err(std::env::VarError::NotPresent) => None,
            Err(_) => anyhow::bail!("VISE_API_TOKEN must contain valid Unicode"),
        };
        Some(DaemonConfig::new(args.bind, token)?)
    } else {
        None
    };
    let manager = Manager::open(Config {
        output_dir: args.output.clone(),
        concurrent_downloads: args.jobs as usize,
        connections_per_download: args.connections as usize,
        retries: args.retries,
        read_timeout: Duration::from_secs(args.timeout),
        limit_rate: args.limit_rate,
        proxy_url,
    })
    .await?;
    app::run_session(manager, &args, interactive, expected, daemon).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn daemon_arguments_are_exclusive_and_allow_a_remote_bind() {
        for flag in ["-d", "--daemon"] {
            let args = Args::try_parse_from(["vise", flag, "--bind", "0.0.0.0:8080"]).unwrap();
            assert!(args.daemon);
            assert!(!args.webui);
            for conflict in ["--webui", "--interactive", "--batch"] {
                assert!(Args::try_parse_from(["vise", flag, conflict]).is_err());
            }
        }
        assert!(Args::try_parse_from(["vise", "--bind", "127.0.0.1:8080"]).is_err());
        assert!(Args::try_parse_from(["vise", "--daemon"]).is_ok());
        assert!(Args::try_parse_from(["vise", "--webui"]).is_ok());
        assert!(Args::try_parse_from(["vise"]).is_ok());
    }

    #[test]
    fn error_reporting_preserves_context_and_returns_write_failures() {
        let error = anyhow::anyhow!("disk unavailable").context("cannot open journal");
        let mut output = Vec::new();
        report_error(&mut output, &error).unwrap();
        assert_eq!(output, b"Vise: cannot open journal: disk unavailable\n");
        assert_eq!(
            report_error(&mut std::io::Cursor::new([]), &error)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::WriteZero
        );
    }
}
