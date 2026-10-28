//! CLI precedence and validated settings for the selected controller.
use std::{net::SocketAddr, time::Duration};

use anyhow::Result;
use vise::{
    config::{self, LoadedConfig},
    engine::Config,
    rate_limiter::RateLimit,
    web::DaemonConfig,
};

use crate::{Args, app::Controller};

pub(super) struct Startup {
    pub engine: Config,
    pub controller: Controller,
}

pub(super) async fn load(args: &Args) -> Result<Startup> {
    let loaded = config::load().await?;
    let environment_token = if args.daemon {
        match std::env::var("VISE_API_TOKEN") {
            Ok(value) => Some(value),
            Err(std::env::VarError::NotPresent) => None,
            Err(_) => anyhow::bail!("VISE_API_TOKEN must contain valid Unicode"),
        }
    } else {
        None
    };
    resolve(args, &loaded, environment_token)
}

fn resolve(
    args: &Args,
    loaded: &LoadedConfig,
    environment_token: Option<String>,
) -> Result<Startup> {
    anyhow::ensure!(
        !(args.daemon || args.webui) || (args.header.is_empty() && args.referer.is_none()),
        "--header and --referer are per-task API settings in --daemon/--webui mode; process-level defaults are forbidden"
    );
    let file = &loaded.values;
    let request = vise::net::credentials::RequestOverrides::from_cli(
        &args.header,
        args.referer.as_deref(),
        args.user_agent.as_deref(),
    )?
    .with_user_agent_default(file.downloads.user_agent.as_deref())?;
    request.validate_urls(&Default::default(), &args.urls)?;
    let bind = args.bind.unwrap_or_else(|| {
        SocketAddr::new(
            args.host.unwrap_or(file.daemon.host),
            args.port.unwrap_or(file.daemon.port),
        )
    });
    let jobs = args.jobs.unwrap_or(file.downloads.max_concurrent);
    anyhow::ensure!(
        (1..=64).contains(&jobs),
        "downloads.max_concurrent must be between 1 and 64"
    );
    let proxy = args.proxy.as_deref().unwrap_or(&file.proxy.url);
    let proxy_url = if proxy.is_empty() {
        None
    } else {
        Some(proxy.parse()?)
    };
    let limit_rate = match args.limit_rate {
        Some(limit) => limit,
        None => configured_rate(file.downloads.global_speed_limit_kbps)?,
    };
    let engine = Config {
        home_dir: Some(loaded.home.clone()),
        output_dir: config::expand_home(
            args.output
                .as_deref()
                .unwrap_or(&file.downloads.default_dir),
            &loaded.home,
        )?,
        concurrent_downloads: jobs as usize,
        connections_per_download: args.connections as usize,
        retries: args.retries,
        read_timeout: Duration::from_secs(args.timeout),
        limit_rate: (args.limit_rate.is_some() || file.downloads.global_speed_limit_kbps != 0)
            .then_some(limit_rate),
        proxy_url,
        request,
        cookies_file: args
            .cookies
            .as_deref()
            .or(file.downloads.cookies_file.as_deref())
            .map(|path| config::expand_home(path, &loaded.home))
            .transpose()?,
    };
    let controller = controller(args, loaded, bind, environment_token)?;
    Ok(Startup { engine, controller })
}

fn configured_rate(kbps: u64) -> Result<RateLimit> {
    let bytes = kbps
        .checked_mul(1024)
        .ok_or_else(|| anyhow::anyhow!("downloads.global_speed_limit_kbps is too large"))?;
    Ok(RateLimit::from_bytes_per_second(Some(bytes)))
}

fn controller(
    args: &Args,
    loaded: &LoadedConfig,
    bind: SocketAddr,
    environment_token: Option<String>,
) -> Result<Controller> {
    if args.daemon {
        let token = environment_token.or_else(|| {
            let token = &loaded.values.daemon.api_token;
            (!token.is_empty()).then(|| token.clone())
        });
        return Ok(Controller::Daemon(DaemonConfig::new(bind, token)?));
    }
    if args.webui {
        anyhow::ensure!(
            bind.ip().is_loopback(),
            "--bind must use a loopback address"
        );
        return Ok(Controller::WebUi(bind));
    }
    Ok(Controller::Cli)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use vise::config::FileConfig;

    fn loaded(home: &std::path::Path) -> LoadedConfig {
        let mut values = FileConfig::default();
        values.daemon.host = "127.0.0.2".parse().unwrap();
        values.daemon.port = 9191;
        values.daemon.api_token = "file-token-0123456789abcdef0123456789".into();
        values.downloads.max_concurrent = 7;
        values.downloads.global_speed_limit_kbps = 256;
        values.proxy.url = "http://127.0.0.1:9080".into();
        LoadedConfig {
            home: home.into(),
            path: home.join(".vise/config.toml"),
            values,
        }
    }

    #[test]
    fn file_preferences_supply_all_startup_defaults() {
        let home = tempfile::tempdir().unwrap();
        let args = Args::try_parse_from(["vise", "--webui"]).unwrap();
        let settings = resolve(&args, &loaded(home.path()), None).unwrap();
        assert_eq!(
            settings.engine.output_dir,
            home.path().join("Downloads/Vise")
        );
        assert_eq!(settings.engine.concurrent_downloads, 7);
        assert_eq!(
            settings.engine.limit_rate.unwrap().bytes_per_second(),
            Some(256 * 1024)
        );
        assert_eq!(
            settings.engine.proxy_url.unwrap().to_string(),
            "http://127.0.0.1:9080/"
        );
        assert!(
            matches!(settings.controller, Controller::WebUi(bind) if bind.to_string()=="127.0.0.2:9191")
        );
    }

    #[test]
    fn explicit_cli_values_override_even_when_equal_to_the_old_defaults() {
        let home = tempfile::tempdir().unwrap();
        let args = Args::try_parse_from([
            "vise",
            "--webui",
            "--bind",
            "127.0.0.1:8080",
            "--output",
            "downloads",
            "--jobs",
            "3",
            "--limit-rate",
            "off",
            "--proxy",
            "",
        ])
        .unwrap();
        let settings = resolve(&args, &loaded(home.path()), None).unwrap();
        assert_eq!(
            settings.engine.output_dir,
            std::path::PathBuf::from("downloads")
        );
        assert_eq!(settings.engine.concurrent_downloads, 3);
        assert_eq!(settings.engine.limit_rate.unwrap(), RateLimit::UNLIMITED);
        assert!(settings.engine.proxy_url.is_none());
        assert!(
            matches!(settings.controller, Controller::WebUi(bind) if bind.to_string()=="127.0.0.1:8080")
        );
        let args =
            Args::try_parse_from(["vise", "--webui", "--host", "::1", "--port", "8282"]).unwrap();
        assert!(
            matches!(resolve(&args, &loaded(home.path()), None).unwrap().controller, Controller::WebUi(bind) if bind.to_string()=="[::1]:8282")
        );
    }

    #[test]
    fn cookies_flag_overrides_file_settings_and_expands_the_home_component() {
        let home = tempfile::tempdir().unwrap();
        let mut file = loaded(home.path());
        file.values.downloads.cookies_file = Some("~/saved-cookies.txt".into());
        let args = Args::try_parse_from(["vise"]).unwrap();
        let settings = resolve(&args, &file, None).unwrap();
        assert_eq!(
            settings.engine.cookies_file,
            Some(home.path().join("saved-cookies.txt"))
        );
        let args = Args::try_parse_from(["vise", "--cookies", "~/override.txt"]).unwrap();
        let settings = resolve(&args, &file, None).unwrap();
        assert_eq!(
            settings.engine.cookies_file,
            Some(home.path().join("override.txt"))
        );
    }

    #[test]
    fn effective_limits_are_checked_and_can_be_overridden() {
        let home = tempfile::tempdir().unwrap();
        let mut file = loaded(home.path());
        let args = Args::try_parse_from(["vise"]).unwrap();
        file.values.downloads.global_speed_limit_kbps = u64::MAX;
        assert!(resolve(&args, &file, None).is_err());
        let override_rate = Args::try_parse_from(["vise", "--limit-rate", "off"]).unwrap();
        assert!(resolve(&override_rate, &file, None).is_ok());
        file.values.downloads.global_speed_limit_kbps = 0;
        file.values.downloads.max_concurrent = 0;
        assert!(resolve(&args, &file, None).is_err());
        let override_jobs = Args::try_parse_from(["vise", "-j", "5"]).unwrap();
        assert!(resolve(&override_jobs, &file, None).is_ok());
    }
}

#[cfg(test)]
mod credential_tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn daemon_mode_rejects_process_level_header_and_referer() {
        let root = tempfile::tempdir().unwrap();
        let loaded = LoadedConfig {
            home: root.path().into(),
            path: root.path().join("config.toml"),
            values: Default::default(),
        };
        for mode in ["--daemon", "--webui"] {
            for (flag, value) in [
                ("--header", "X-Secret: private-sentinel"),
                ("--referer", "https://example.com/private-sentinel"),
            ] {
                let args = Args::try_parse_from(["vise", mode, flag, value]).unwrap();
                let error = resolve(&args, &loaded, None).err().unwrap().to_string();
                assert!(error.contains("per-task") && !error.contains("private-sentinel"));
            }
            let args = Args::try_parse_from(["vise", mode, "--user-agent", "allowed"]).unwrap();
            assert!(resolve(&args, &loaded, None).is_ok());
        }
    }

    #[test]
    fn cli_user_agent_overrides_toml_even_at_builtin_value() {
        let root = tempfile::tempdir().unwrap();
        let mut values = vise::config::FileConfig::default();
        values.downloads.user_agent = Some("configured-private-agent".into());
        let loaded = LoadedConfig {
            home: root.path().into(),
            path: root.path().join("config.toml"),
            values,
        };
        let args = Args::try_parse_from([
            "vise",
            "--user-agent",
            vise::net::credentials::BUILTIN_USER_AGENT,
        ])
        .unwrap();
        let startup = resolve(&args, &loaded, None).unwrap();
        let mut wire = serde_json::json!({});
        startup.engine.request.encode_admission(&mut wire).unwrap();
        assert_eq!(
            wire["user_agent"],
            vise::net::credentials::BUILTIN_USER_AGENT
        );
    }
}
