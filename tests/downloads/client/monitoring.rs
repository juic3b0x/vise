use super::fixture::{Daemon, Process, decoded, run};
use crate::fixture::{Fixture, Mode};
use std::{io::BufRead, process::Stdio, time::Duration};
use vise::model::{Download, Status};

async fn signal_and_wait(child: &mut Process) {
    let status = std::process::Command::new("kill")
        .args(["-TERM", &child.0.as_ref().unwrap().id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    let status = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(status) = child.0.as_mut().unwrap().try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("remote monitor held shutdown open");
    assert_eq!(status.code(), Some(130));
}

#[tokio::test]
async fn watch_streams_ndjson_and_signal_detaches_without_stopping_the_daemon() {
    let daemon = Daemon::start().await;
    let origin = Fixture::start(Mode::SlowRanges).await;
    daemon
        .manager
        .set_rate_limit("8K".parse().unwrap())
        .await
        .unwrap();
    let running = daemon.manager.add(&origin.url, None).await.unwrap();
    let mut child = Process(Some(
        daemon
            .command()
            .args(["watch", "--json"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    ));
    let stdout = child.0.as_mut().unwrap().stdout.take().unwrap();
    let (sent, received) = tokio::sync::oneshot::channel();
    let reader = std::thread::spawn(move || {
        let mut reader = std::io::BufReader::new(stdout);
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        sent.send(line).unwrap();
        // Keep the read end alive until the child exits.
        let mut rest = String::new();
        std::io::Read::read_to_string(&mut reader, &mut rest).unwrap();
    });
    let line = tokio::time::timeout(Duration::from_secs(5), received)
        .await
        .unwrap()
        .unwrap();
    let snapshot: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert!(snapshot.get("downloads").unwrap().is_array());
    assert!(snapshot.get("timestamp").unwrap().is_number());
    assert!(snapshot.get("configuration").is_some());
    signal_and_wait(&mut child).await;
    reader.join().unwrap();
    let jobs: Vec<Download> = decoded(&daemon.run(&["list", "--json"]).await, 0);
    assert!(
        jobs.iter()
            .any(|job| job.id == running.id && job.status.active())
    );
    daemon.close().await;
    origin.close().await;
}

#[tokio::test]
async fn blocked_stdout_does_not_hold_an_interrupted_client_runtime_open() {
    use std::io::Read;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let router = axum::Router::new().route(
        "/api/telemetry",
        axum::routing::get(|| async {
            axum::Json(serde_json::json!({
                "timestamp": 0,
                "configuration": {"limit_rate": "unlimited", "limit_bytes_per_second": null},
                "downloads": [{
                    "id": "ba8d340a-6d57-4991-9f94-398e585ead15",
                    "url": "https://example.org/file", "filename": "x".repeat(8 * 1024 * 1024),
                    "status": "queued", "total_bytes": null, "downloaded_bytes": 0,
                    "bytes_per_second": 0.0, "eta_seconds": null, "error": null, "created_at": 0
                }]
            }))
        }),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let root = tempfile::tempdir().unwrap();
    let mut command = crate::fixture::command(root.path());
    let mut child = Process(Some(
        command
            .args(["remote", "--endpoint", &endpoint, "watch", "--json"])
            .env("VISE_API_TOKEN", super::fixture::SECRET)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    ));
    let mut stdout = child.0.as_mut().unwrap().stdout.take().unwrap();
    let (sent, received) = tokio::sync::oneshot::channel();
    let (release, hold) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        stdout.read_exact(&mut [0u8; 1]).unwrap();
        let _ = sent.send(());
        // Deliberately leave the rest of a response larger than the pipe buffer
        // unread. EOF must not be necessary for the client's signal shutdown.
        let _ = hold.recv_timeout(Duration::from_secs(15));
        drop(stdout);
    });
    tokio::time::timeout(Duration::from_secs(10), received)
        .await
        .unwrap()
        .unwrap();
    signal_and_wait(&mut child).await;
    release.send(()).unwrap();
    reader.join().unwrap();
    server.abort();
}

#[tokio::test]
async fn wait_reports_a_paused_task_as_unsuccessful_without_resuming_it() {
    let daemon = Daemon::start().await;
    let origin = Fixture::start(Mode::SlowRanges).await;
    let output = daemon.run(&["limit-rate", "1K"]).await;
    assert!(output.status.success());
    let mut command = daemon.command();
    command.args(["add", &origin.url, "--wait", "--json"]);
    let client = tokio::spawn(run(command));
    let id = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            // Admission initially journals a paused record before starting it.
            // Wait until start holds/has released the task-control lock so the
            // pause cannot race ahead of that initial start operation.
            if let Some(item) = daemon
                .manager
                .list()
                .await
                .iter()
                .find(|item| item.status.active())
            {
                break item.id;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    daemon.manager.pause(id).await.unwrap();
    let paused: Download = decoded(&client.await.unwrap(), 2);
    assert_eq!(paused.status, Status::Paused);
    assert_eq!(daemon.manager.get(id).await.unwrap().status, Status::Paused);
    daemon.close().await;
    origin.close().await;
}
