use std::{io::IsTerminal, time::Duration};

use anyhow::Result;
use serde::Serialize;
use vise::{
    checksum::ExpectedChecksums,
    cli::ProgressDisplay,
    model::{Download, Status},
    rate_limiter::RateConfiguration,
};

use super::{
    Add, Args, Command,
    api::{Api, Telemetry},
    output::{self, Output},
    settings::Settings,
};

const POLL: Duration = Duration::from_secs(1);

pub(super) async fn execute(args: Args) -> Result<u8> {
    let settings = Settings::load(args.endpoint).await?;
    let api = Api::new(settings, args.request_timeout)?;
    let output = Output::new(args.json)?;
    match args.command {
        Command::Add(add) => add_download(&api, &output, add).await,
        Command::List => list_downloads(&api, &output).await,
        Command::Get { id } => task(&api, &output, &id, None).await,
        Command::Pause { id } => task(&api, &output, &id, Some("pause")).await,
        Command::Resume { id } => task(&api, &output, &id, Some("resume")).await,
        Command::Retry { id } => task(&api, &output, &id, Some("retry")).await,
        Command::Delete { id } => delete_download(&api, &output, &id).await,
        Command::LimitRate { rate } => configure_limit(&api, &output, rate).await,
        Command::Watch => watch(&api, &output).await,
    }
}

async fn list_downloads(api: &Api, output: &Output) -> Result<u8> {
    let jobs: Vec<Download> = api.get("api/downloads").await?;
    let text = if output.json {
        String::new()
    } else if jobs.is_empty() {
        "No downloads.".into()
    } else {
        jobs.iter()
            .map(output::download)
            .collect::<Vec<_>>()
            .join("\n")
    };
    output.value(&jobs, text).await?;
    Ok(0)
}

async fn delete_download(api: &Api, output: &Output, input: &str) -> Result<u8> {
    let id = api.resolve_id(input).await?;
    api.delete(id).await?;
    output
        .value(
            &serde_json::json!({"id": id, "deleted": true}),
            format!("Deleted {id}; completed output kept."),
        )
        .await?;
    Ok(0)
}

async fn configure_limit(
    api: &Api,
    output: &Output,
    rate: Option<vise::rate_limiter::RateLimit>,
) -> Result<u8> {
    let configuration: RateConfiguration = match rate {
        Some(rate) => {
            api.post(
                "api/config",
                &serde_json::json!({"limit_rate": rate.to_string()}),
            )
            .await?
        }
        None => api.get("api/config").await?,
    };
    output
        .value(
            &configuration,
            format!(
                "Global bandwidth limit: {}",
                configuration.limit_rate.escape_debug()
            ),
        )
        .await?;
    Ok(0)
}

#[derive(Serialize)]
struct Admission<'a> {
    url: &'a str,
    filename: Option<&'a str>,
    expected_sha256: Option<String>,
    expected_md5: Option<String>,
    proxy_url: Option<&'a str>,
}

async fn add_download(api: &Api, output: &Output, add: Add) -> Result<u8> {
    let expected =
        ExpectedChecksums::new(add.expected_sha256.as_deref(), add.expected_md5.as_deref())?;
    let body = Admission {
        url: &add.url,
        filename: add.name.as_deref(),
        expected_sha256: expected.sha256,
        expected_md5: expected.md5,
        proxy_url: add.proxy.as_deref(),
    };
    let item: Download = api.post("api/downloads", &body).await?;
    if add.wait {
        return wait(api, output, item).await;
    }
    output
        .value(&item, format!("Queued {}", output::download(&item)))
        .await?;
    Ok(0)
}

async fn task(api: &Api, output: &Output, input: &str, action: Option<&str>) -> Result<u8> {
    let id = api.resolve_id(input).await?;
    let path = format!("api/downloads/{id}");
    let item: Download = match action {
        Some(action) => {
            api.post(&format!("{path}/{action}"), &serde_json::json!({}))
                .await?
        }
        None => api.get(&path).await?,
    };
    output.value(&item, output::download(&item)).await?;
    Ok(0)
}

fn progress_enabled(output: &Output) -> bool {
    !output.json && std::io::stderr().is_terminal() && std::io::stdout().is_terminal()
}

async fn wait(api: &Api, output: &Output, mut item: Download) -> Result<u8> {
    let mut display = ProgressDisplay::default();
    loop {
        if !item.status.active() {
            display.finish();
            output.value(&item, output::download(&item)).await?;
            return Ok(if item.status == Status::Completed {
                0
            } else {
                2
            });
        }
        if progress_enabled(output) {
            display.render(std::slice::from_ref(&item))?;
        }
        tokio::time::sleep(POLL).await;
        item = api.get(&format!("api/downloads/{}", item.id)).await?;
    }
}

async fn watch(api: &Api, output: &Output) -> Result<u8> {
    let mut display = ProgressDisplay::default();
    let mut announced = false;
    loop {
        let snapshot: Telemetry = api.get("api/telemetry").await?;
        if output.json {
            output.value(&snapshot, String::new()).await?;
        } else if progress_enabled(output) {
            if !announced {
                display
                    .messages()
                    .println("Connected to daemon. Ctrl+C detaches without stopping downloads.")?;
                announced = true;
            }
            display.render(&snapshot.downloads)?;
        } else {
            let mut text = format!(
                "{} — global limit: {}",
                snapshot.timestamp,
                snapshot.configuration.limit_rate.escape_debug()
            );
            for item in &snapshot.downloads {
                text.push('\n');
                text.push_str(&output::download(item));
            }
            output.value(&snapshot, text).await?;
        }
        tokio::time::sleep(POLL).await;
    }
}
