use super::fixture::{Daemon, SECRET, decoded};
use crate::fixture::{Fixture, Mode, terminal};
use reqwest::{Client, StatusCode};
use serde_json::json;
use vise::{engine::BatchResult, input::MAX_BATCH_BODY_BYTES, model::Status};

async fn post(daemon: &Daemon, body: serde_json::Value) -> reqwest::Response {
    Client::new()
        .post(format!("{}/api/downloads/batch", daemon.endpoint))
        .bearer_auth(SECRET)
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn daemon_batches_prevalidate_authenticate_and_keep_single_request_limits() {
    let daemon = Daemon::start().await;
    for body in [
        json!({}),
        json!({"urls":[]}),
        json!({"content":"# empty\n"}),
        json!({"urls":["https://example.com/a", "bad"]}),
        json!({"content":"https://example.com/a\nhttps://user:secret@example.com/a"}),
        json!({"urls":["https://example.com/a"],"content":"https://example.com/b"}),
        json!({"urls":["https://example.com/a"],"filename":"one.bin"}),
        json!({"urls":["https://example.com/a"],"proxy_url":"not-a-proxy"}),
    ] {
        let response = post(&daemon, body).await;
        assert!(response.status().is_client_error());
        assert!(!response.text().await.unwrap().contains("secret"));
        assert!(daemon.manager.list().await.is_empty());
    }
    let unauthenticated = Client::new()
        .post(format!("{}/api/downloads/batch", daemon.endpoint))
        .header("content-type", "application/json")
        .body(r#"{"urls":["https://example.com/a"]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);
    let oversized = Client::new()
        .post(format!("{}/api/downloads/batch", daemon.endpoint))
        .bearer_auth(SECRET)
        .header("content-type", "application/json")
        .body(" ".repeat(MAX_BATCH_BODY_BYTES + 1))
        .send()
        .await
        .unwrap();
    assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let single = Client::new()
        .post(format!("{}/api/downloads", daemon.endpoint))
        .bearer_auth(SECRET)
        .header("content-type", "application/json")
        .body(
            json!({"url": "a".repeat(vise::net::credentials::ADMISSION_BODY_BYTES + 1)})
                .to_string(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(single.status(), StatusCode::PAYLOAD_TOO_LARGE);
    daemon.close().await;
}

#[tokio::test]
async fn daemon_accepts_array_and_text_batches_and_journals_them_in_the_same_queue() {
    let source = Fixture::start(Mode::SlowRanges).await;
    let daemon = Daemon::start().await;
    let response = post(&daemon, json!({"urls":[source.url, source.url]})).await;
    assert_eq!(response.status(), StatusCode::OK);
    let first: BatchResult = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(first.accepted.len(), 2);
    assert!(first.rejected.is_empty());
    assert_eq!(
        first
            .accepted
            .iter()
            .map(|entry| entry.index)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    // Route-specific body limit accepts a useful text upload above 16 KiB.
    let response = post(
        &daemon,
        json!({"content":format!("\u{feff}# {}\r\n\n {} \r\n", "x".repeat(20000), source.url)}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let second: BatchResult = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(second.accepted.len(), 1);
    assert_eq!(daemon.manager.list().await.len(), 3);
    assert!(
        daemon
            .manager
            .list()
            .await
            .iter()
            .any(|item| item.status == Status::Queued)
    );
    for accepted in first.accepted.into_iter().chain(second.accepted) {
        let done = terminal(&daemon.manager, accepted.download.id).await;
        assert_eq!(done.status, Status::Completed, "{:?}", done.error);
        assert_eq!(std::fs::read(&done.output_path).unwrap(), source.state.data);
    }
    assert!(source.state.peak.load(std::sync::atomic::Ordering::SeqCst) <= 2);
    let database = vise::database::Database::open(daemon.manager.database_path())
        .await
        .unwrap();
    assert_eq!(database.list().await.unwrap().len(), 3);
    daemon.close().await;
    source.close().await;
}

#[tokio::test]
async fn remote_input_reads_client_file_and_waits_for_every_submitted_task() {
    let source = Fixture::start(Mode::Ranges).await;
    let daemon = Daemon::start().await;
    let path = daemon.root.path().join("client-urls.txt");
    std::fs::write(
        &path,
        format!("# client file\n{}\n{}\n", source.url, source.url),
    )
    .unwrap();
    let mut command = daemon.command();
    command
        .args(["add", "--input"])
        .arg(path)
        .args(["--wait", "--json"]);
    let output = super::fixture::run(command).await;
    let batch: BatchResult = decoded(&output, 0);
    assert_eq!(batch.accepted.len(), 2);
    assert!(batch.rejected.is_empty());
    assert!(
        batch
            .accepted
            .iter()
            .all(|entry| entry.download.status == Status::Completed)
    );
    assert!(
        !daemon
            .root
            .path()
            .join("test-home/unused-client-default")
            .exists()
    );
    daemon.close().await;
    source.close().await;
}

#[tokio::test]
async fn rejected_admissions_return_indexed_results_and_remote_exit_two() {
    let daemon = Daemon::start().await;
    daemon.manager.shutdown().await.unwrap();
    let result = post(
        &daemon,
        json!({"urls":["https://example.com/a", "https://example.com/b"]}),
    )
    .await;
    assert_eq!(result.status(), StatusCode::OK);
    let result: BatchResult = serde_json::from_slice(&result.bytes().await.unwrap()).unwrap();
    assert!(result.accepted.is_empty());
    assert_eq!(
        result
            .rejected
            .iter()
            .map(|entry| entry.index)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert!(
        result
            .rejected
            .iter()
            .all(|entry| entry.error.contains("shutting down"))
    );
    let output = daemon
        .run(&[
            "add",
            "https://example.com/a",
            "https://example.com/b",
            "--json",
        ])
        .await;
    let result: BatchResult = decoded(&output, 2);
    assert_eq!(result.rejected.len(), 2);
    assert!(daemon.manager.list().await.is_empty());
    daemon.close().await;
}

#[tokio::test]
async fn admitted_batch_finishes_queueing_when_the_caller_disconnects() {
    let source = Fixture::start(Mode::SlowRanges).await;
    let daemon = Daemon::start().await;
    let mut events = daemon.manager.subscribe_events();
    let worker = daemon.manager.clone();
    let urls = vec![source.url.clone(); 4];
    let caller = tokio::spawn(async move { worker.add_batch(urls, None).await });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !matches!(
            events.recv().await.unwrap().kind,
            vise::events::EventKind::Added
        ) {}
    })
    .await
    .unwrap();
    caller.abort();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let list = daemon.manager.list().await;
            if list.len() == 4 && list.iter().all(|item| item.status.active()) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let database = vise::database::Database::open(daemon.manager.database_path())
        .await
        .unwrap();
    assert_eq!(database.list().await.unwrap().len(), 4);
    daemon.close().await;
    source.close().await;
}
