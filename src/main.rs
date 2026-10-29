use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    process::ExitCode,
};

use clap::{Parser, Subcommand};
use vise::{checksum::ExpectedChecksums, cli, engine::Manager, rate_limiter::RateLimit};

mod app;
mod client;
mod logging;
mod startup;

#[derive(Parser)]
#[command(version = env!("VISE_BUILD_VERSION"), about = "Vise — concurrent, resumable downloads", long_about = None)]
#[command(group(clap::ArgGroup::new("server").args(["webui", "daemon"])))]
#[command(args_conflicts_with_subcommands = true)]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,
    /// HTTP(S) URLs to enqueue
    urls: Vec<String>,
    /// Read HTTP(S) URLs from a UTF-8 text file (one per line; # comments)
    #[arg(long, value_name = "FILE")]
    input: Option<PathBuf>,
    /// Launch the local dashboard instead of the terminal controller
    #[arg(long)]
    webui: bool,
    /// Run the API and dashboard quietly for a service manager (no terminal input)
    #[arg(short = 'd', long)]
    daemon: bool,
    /// Override the configured server address (exclusive with --host/--port)
    #[arg(long, requires = "server", conflicts_with_all = ["host", "port"])]
    bind: Option<SocketAddr>,
    /// Override daemon.host from ~/.vise/config.toml
    #[arg(long, requires = "server")]
    host: Option<IpAddr>,
    /// Override daemon.port from ~/.vise/config.toml
    #[arg(long, requires = "server")]
    port: Option<u16>,
    /// Default destination for new downloads; global state stays in ~/.vise
    #[arg(short, long)]
    output: Option<PathBuf>,
    /// Number of downloads running at once
    #[arg(short = 'j', long, value_parser = clap::value_parser!(u16).range(1..=64))]
    jobs: Option<u16>,
    /// Maximum parallel connections per download
    #[arg(short = 'c', long, default_value_t = 8, value_parser = clap::value_parser!(u16).range(1..=64))]
    connections: u16,
    /// Retries after the initial attempt, per segment
    #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u32).range(0..=20))]
    retries: u32,
    /// Idle read timeout, in seconds (not a total transfer limit)
    #[arg(long, default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..))]
    timeout: u64,
    /// Resume unfinished global tasks at their saved destination paths
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
    /// Netscape cookies.txt file for new downloads; saved tasks retain their file
    #[arg(long, value_name = "FILE")]
    cookies: Option<PathBuf>,
    /// Custom request header for new downloads (repeatable; per-task API only in server modes)
    #[arg(long, value_name = "NAME: VALUE")]
    header: Vec<String>,
    /// Referer for new downloads (per-task API only in server modes)
    #[arg(long, value_name = "URL")]
    referer: Option<String>,
    /// User agent for new downloads; empty selects Vise's built-in value
    #[arg(long, value_name = "STRING")]
    user_agent: Option<String>,
}

#[derive(Subcommand)]
enum Command {
    /// Control an existing daemon without starting a local download engine
    Remote(client::Args),
}

#[tokio::main]
async fn main() -> ExitCode {
    logging::init();
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

async fn run(mut args: Args) -> anyhow::Result<u8> {
    if let Some(Command::Remote(remote)) = args.command {
        return client::run(remote).await;
    }
    anyhow::ensure!(
        !args.urls.iter().any(|url| url == "remote"),
        "put remote immediately after vise; local-engine options cannot be mixed with daemon client commands"
    );
    if args.input.is_some() || !args.urls.is_empty() {
        args.urls = vise::input::load(std::mem::take(&mut args.urls), args.input.take()).await?;
    }
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
    let startup = startup::load(&args).await?;
    let manager = Manager::open(startup.engine).await?;
    app::run_session(manager, &args, interactive, expected, startup.controller).await
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
        assert!(
            Args::try_parse_from([
                "vise",
                "--daemon",
                "--bind",
                "127.0.0.1:8080",
                "--host",
                "127.0.0.1"
            ])
            .is_err()
        );
        assert!(Args::try_parse_from(["vise", "--port", "8080"]).is_err());
    }

    #[test]
    fn remote_commands_are_explicit_and_preserve_direct_url_mode() {
        let direct = Args::try_parse_from(["vise", "https://example.org/file", "-c", "4"]).unwrap();
        assert!(direct.command.is_none());
        assert_eq!(direct.urls, ["https://example.org/file"]);
        assert_eq!(direct.connections, 4);
        for command in [
            vec!["vise", "remote", "list", "--json"],
            vec![
                "vise",
                "remote",
                "--endpoint",
                "http://127.0.0.1:8080",
                "add",
                "https://example.org/file",
                "--wait",
            ],
            vec!["vise", "remote", "limit-rate", "off"],
        ] {
            assert!(matches!(
                Args::try_parse_from(command).unwrap().command,
                Some(Command::Remote(_))
            ));
        }
        // Leading local arguments retain the positional-URL grammar and must
        // never silently become a remote client invocation.
        for command in [
            vec!["vise", "--daemon", "remote", "list"],
            vec!["vise", "--output", "downloads", "remote", "list"],
            vec!["vise", "https://example.org/file", "remote", "list"],
        ] {
            assert!(Args::try_parse_from(command).unwrap().command.is_none());
        }
        assert!(
            Args::try_parse_from([
                "vise",
                "remote",
                "add",
                "https://example.org/file",
                "--cookies",
                "cookies.txt"
            ])
            .is_err()
        );
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
