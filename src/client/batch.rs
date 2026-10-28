//! Remote bulk admission and completion never create client-side engine state.
use super::{
    api::Api,
    output::{self, Output},
};
use anyhow::Result;
use std::{collections::BTreeMap, fmt::Write, io::IsTerminal, time::Duration};
use vise::{
    engine::BatchResult,
    model::{Download, Status},
};

pub(super) async fn submit(
    api: &Api,
    output: &Output,
    urls: Vec<String>,
    proxy: Option<String>,
    request: vise::net::credentials::RequestOverrides,
    wait: bool,
) -> Result<u8> {
    let mut body = serde_json::json!({"urls":urls,"proxy_url":proxy});
    request.encode_admission(&mut body)?;
    let mut result: BatchResult = api.post("api/downloads/batch", &body).await?;
    if wait && !result.accepted.is_empty() {
        wait_for_all(api, output, &mut result).await?;
    }
    let failed = !result.rejected.is_empty()
        || (wait
            && result
                .accepted
                .iter()
                .any(|entry| entry.download.status != Status::Completed));
    let mut text = format!(
        "{} accepted; {} rejected.",
        result.accepted.len(),
        result.rejected.len()
    );
    for entry in &result.accepted {
        write!(
            text,
            "\n{}: {}",
            entry.index,
            output::download(&entry.download)
        )?;
    }
    for entry in &result.rejected {
        write!(
            text,
            "\nEntry {}: {}",
            entry.index,
            entry.error.escape_debug()
        )?;
    }
    if !result.rejected.is_empty() {
        text.push_str("\nInspect daemon history before retrying rejected entries.");
    }
    output.value(&result, text).await?;
    Ok(if failed { 2 } else { 0 })
}

async fn wait_for_all(api: &Api, output: &Output, result: &mut BatchResult) -> Result<()> {
    let mut display = vise::cli::ProgressDisplay::default();
    while result
        .accepted
        .iter()
        .any(|entry| entry.download.status.active())
    {
        if !output.json && std::io::stdout().is_terminal() && std::io::stderr().is_terminal() {
            display.render(
                &result
                    .accepted
                    .iter()
                    .map(|entry| entry.download.clone())
                    .collect::<Vec<_>>(),
            )?;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        let snapshot: Vec<Download> = api.get("api/downloads").await?;
        let mut snapshots: BTreeMap<_, _> =
            snapshot.into_iter().map(|item| (item.id, item)).collect();
        for entry in &mut result.accepted {
            entry.download = snapshots.remove(&entry.download.id).ok_or_else(|| {
                anyhow::anyhow!("a submitted task was removed; inspect daemon history")
            })?;
        }
    }
    display.finish();
    Ok(())
}
