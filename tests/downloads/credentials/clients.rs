use super::{
    api::Api,
    fixture::{Capture, SECRET},
};
use crate::fixture::Mode;
use std::{process::Stdio, time::Duration};
use vise::{engine::BatchResult, model::Status};

async fn output(mut command: std::process::Command) -> std::process::Output {
    tokio::task::spawn_blocking(move || {
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .unwrap()
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn remote_input_batch_applies_request_options_to_every_url() {
    let source = Capture::start(Mode::Ranges).await;
    let api = Api::start(Default::default()).await;
    let file = api.root.path().join("urls.txt");
    std::fs::write(&file, format!("# files\n{}\n{}\n", source.url, source.url)).unwrap();
    let mut command = api.command();
    command.args(["add", "--input"]).arg(&file).args([
        "--header",
        &format!("X-Secret: {SECRET}"),
        "--referer",
        "http://source.example/private",
        "--user-agent",
        SECRET,
        "--wait",
    ]);
    let result = output(command).await;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let batch: BatchResult = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(batch.accepted.len(), 2);
    assert!(
        batch
            .accepted
            .iter()
            .all(|a| a.download.status == Status::Completed)
    );
    assert!(!String::from_utf8_lossy(&result.stdout).contains(SECRET));
    assert!(!String::from_utf8_lossy(&result.stderr).contains(SECRET));
    for (_, headers) in source.requests.lock().unwrap().iter() {
        assert_eq!(headers.get("x-secret").unwrap(), SECRET);
        assert_eq!(headers.get("user-agent").unwrap(), SECRET);
    }
    api.close().await;
    source.close().await;
}

#[tokio::test]
async fn local_and_remote_multi_origin_batches_fail_before_admission() {
    for from_file in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let mut local = crate::fixture::command(root.path());
        local.args(["--header", &format!("X-Secret: {SECRET}")]);
        let file = root.path().join("urls.txt");
        std::fs::write(&file, "http://127.0.0.1:1/a\nhttp://127.0.0.1:2/b").unwrap();
        if from_file {
            local.arg("--input").arg(&file);
        } else {
            local.args(["http://127.0.0.1:1/a", "http://127.0.0.1:2/b"]);
        }
        let result = output(local).await;
        let error = String::from_utf8_lossy(&result.stderr);
        assert!(!result.status.success() && error.contains("split") && !error.contains(SECRET));
        assert!(!root.path().join("test-home/.vise/state.sqlite3").exists());
        let api = Api::start(Default::default()).await;
        let mut remote = api.command();
        remote.args(["add", "--header", &format!("X-Secret: {SECRET}")]);
        if from_file {
            remote.arg("--input").arg(&file);
        } else {
            remote.args(["http://127.0.0.1:1/a", "http://127.0.0.1:2/b"]);
        }
        let result = output(remote).await;
        assert!(!result.status.success());
        assert!(String::from_utf8_lossy(&result.stderr).contains("split"));
        assert!(api.manager.list().await.is_empty());
        api.close().await;
    }
}

#[tokio::test]
async fn credential_values_absent_from_vise_trace_and_cli_errors() {
    let source = Capture::start(Mode::Transient).await;
    let root = tempfile::tempdir().unwrap();
    let mut command = crate::fixture::command(root.path());
    command
        .env("RUST_LOG", "vise=trace")
        .args(["--batch", "--output"])
        .arg(root.path())
        .args([
            "--header",
            &format!("X-Secret: {SECRET}"),
            "--referer",
            &format!("http://source.example/{SECRET}"),
            "--user-agent",
            SECRET,
            &source.url,
        ]);
    let result = output(command).await;
    assert!(result.status.success());
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(stderr.contains("retrying transfer"));
    assert!(!stderr.contains(SECRET) && !String::from_utf8_lossy(&result.stdout).contains(SECRET));
    for flag in ["--daemon", "--webui"] {
        let mut command = crate::fixture::command(root.path());
        command.args([flag, "--header", &format!("X-Secret: {SECRET}")]);
        let result = output(command).await;
        assert!(!result.status.success());
        let error = String::from_utf8_lossy(&result.stderr);
        assert!(error.contains("per-task") && !error.contains(SECRET));
    }
    source.close().await;
}

pub(super) async fn checkpoint(path: std::path::PathBuf) -> (uuid::Uuid, u64) {
    tokio::time::timeout(Duration::from_secs(10),async move {
        loop {
            let path=path.clone();
            let saved=tokio::task::spawn_blocking(move || {
                let connection=rusqlite::Connection::open_with_flags(path,rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).ok()?;
                connection.busy_timeout(Duration::from_millis(100)).ok()?;
                let (id,bytes)=connection.query_row("SELECT d.id,COALESCE(SUM(c.downloaded_bytes),0) FROM downloads d LEFT JOIN chunks c ON c.download_id=d.id GROUP BY d.id",[],|r|Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?))).ok()?;
                Some((id.parse::<uuid::Uuid>().ok()?,u64::try_from(bytes).ok()?))
            }).await.unwrap();
            if let Some((id,bytes))=saved && bytes>0 {break (id,bytes);}
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap()
}

async fn seeded_journal(url: String) -> (tempfile::TempDir, uuid::Uuid) {
    tokio::task::spawn_blocking(move || {
        let root = tempfile::tempdir().unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let id = runtime.block_on(async {
            let owner = vise::engine::Manager::open(vise::engine::Config {
                output_dir: root.path().join("downloads"),
                home_dir: Some(root.path().join("test-home")),
                concurrent_downloads: 1,
                connections_per_download: 2,
                retries: 2,
                ..Default::default()
            })
            .await
            .unwrap();
            owner.set_rate_limit("64M".parse().unwrap()).await.unwrap();
            let existing = owner.add(&url, None).await.unwrap();
            assert_eq!(
                crate::fixture::terminal(&owner, existing.id).await.status,
                Status::Completed
            );
            owner.shutdown().await.unwrap();
            drop(owner);
            existing.id
        });
        // Database::Shared::drop schedules connection close on this runtime's
        // blocking pool. Dropping the runtime joins that cleanup; awaiting this
        // outer task also leaves the HTTP fixtures' runtime free to serve data.
        drop(runtime);
        (root, id)
    })
    .await
    .unwrap()
}

fn output_entries(root: &std::path::Path) -> std::collections::BTreeSet<std::path::PathBuf> {
    fn visit(root: &std::path::Path, entries: &mut std::collections::BTreeSet<std::path::PathBuf>) {
        for entry in std::fs::read_dir(root).unwrap() {
            let entry = entry.unwrap();
            entries.insert(entry.path());
            if entry.file_type().unwrap().is_dir() {
                visit(&entry.path(), entries);
            }
        }
    }
    let mut entries = Default::default();
    visit(root, &mut entries);
    entries
}

#[tokio::test]
async fn rejected_batch_preserves_existing_journal_and_sends_no_requests() {
    use super::journal::snapshot;
    let first = Capture::start(Mode::Ranges).await;
    let second = Capture::start(Mode::Ranges).await;
    let (root, existing_id) = seeded_journal(first.url.clone()).await;
    // Ignore seeding traffic; every request from a rejected invocation is a bug.
    first.requests.lock().unwrap().clear();
    second.requests.lock().unwrap().clear();
    let output_dir = root.path().join("downloads");
    let path = root.path().join("test-home/.vise/state.sqlite3");
    for from_file in [false, true] {
        let before = snapshot(&path);
        let files = output_entries(&output_dir);
        assert_eq!(before.download_ids, [existing_id.to_string()]);
        assert!(
            before
                .row_counts()
                .get("chunks")
                .is_some_and(|count| *count > 0)
        );
        let mut command = crate::fixture::command(root.path());
        command.args([
            "--batch",
            "--resume-all",
            "--header",
            &format!("X-Secret: {SECRET}"),
        ]);
        command.arg("--output").arg(&output_dir);
        if from_file {
            let input = root.path().join("urls.txt");
            std::fs::write(&input, format!("{}\n{}\n", first.url, second.url)).unwrap();
            command.arg("--input").arg(input);
        } else {
            command.args([&first.url, &second.url]);
        }
        let result = output(command).await;
        assert_eq!(result.status.code(), Some(1));
        assert!(String::from_utf8_lossy(&result.stderr).contains("split"));
        // output() has joined the child after process exit. Compare SQLite's
        // logical contents, never main-file bytes or WAL/SHM physical state.
        let after = snapshot(&path);
        assert_eq!(
            after.download_ids, before.download_ids,
            "download IDs changed"
        );
        assert_eq!(
            after.row_counts(),
            before.row_counts(),
            "journal row counts changed"
        );
        assert_eq!(
            after, before,
            "journal contents changed after rejected admission"
        );
        assert_eq!(output_entries(&output_dir), files, "output entries changed");
        assert_eq!(
            first.requests.lock().unwrap().len() + second.requests.lock().unwrap().len(),
            0,
            "rejected batch sent a download request"
        );
    }
    first.close().await;
    second.close().await;
}
