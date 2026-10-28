use super::{
    clients::checkpoint,
    fixture::{Capture, SECRET, options},
};
use crate::fixture::{Mode, terminal};
use std::{
    process::{Child, Stdio},
    time::Duration,
};
use vise::{
    engine::{Config, Manager},
    model::Status,
};

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn abrupt_restart_restores_saved_request_credentials() {
    let source = Capture::start(Mode::SlowRanges).await;
    let root = tempfile::tempdir().unwrap();
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let bind = reservation.local_addr().unwrap();
    drop(reservation);
    let token = "crash-fixture-api-token-0123456789abcdef";
    let mut command = crate::fixture::command(root.path());
    command
        .env("VISE_API_TOKEN", token)
        .args([
            "--daemon",
            "--bind",
            &bind.to_string(),
            "--connections",
            "1",
            "--limit-rate",
            "1M",
            "--output",
        ])
        .arg(root.path());
    let mut child = Process(
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let base = format!("http://{bind}");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if client
                .get(format!("{base}/api/config"))
                .bearer_auth(token)
                .send()
                .await
                .is_ok()
            {
                break;
            }
            assert!(child.0.try_wait().unwrap().is_none());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let mut body = serde_json::json!({"url":source.url});
    options().request.encode_admission(&mut body).unwrap();
    let response = client
        .post(format!("{base}/api/downloads"))
        .bearer_auth(token)
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    let path = root.path().join("test-home/.vise/state.sqlite3");
    let (id, _) = checkpoint(path.clone()).await;
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    drop(child);
    // Read after death: another atomic checkpoint may have committed between
    // observing progress and killing the child. Never assert against a stale row.
    let (restored_id, saved) = checkpoint(path.clone()).await;
    assert_eq!(restored_id, id);
    assert!(saved < source.origin.state.data.len() as u64);
    let before = source.requests.lock().unwrap().len();
    let request = vise::net::credentials::RequestOverrides::from_cli(
        &[],
        None,
        Some("different-process-default"),
    )
    .unwrap();
    let manager = Manager::open(Config {
        home_dir: Some(root.path().join("test-home")),
        output_dir: root.path().into(),
        request,
        connections_per_download: 1,
        limit_rate: Some(vise::rate_limiter::RateLimit::UNLIMITED),
        ..Default::default()
    })
    .await
    .unwrap();
    assert_eq!(manager.get(id).await.unwrap().status, Status::Paused);
    assert_eq!(manager.get(id).await.unwrap().downloaded_bytes, saved);
    manager.resume(id).await.unwrap();
    let done = terminal(&manager, id).await;
    assert_eq!(done.status, Status::Completed);
    assert_eq!(
        std::fs::read(&done.output_path).unwrap(),
        source.origin.state.data
    );
    for (_, headers) in source.requests.lock().unwrap().iter().skip(before) {
        let range = headers.get("range").unwrap().to_str().unwrap();
        assert!(
            range == "bytes=0-0"
                || range == format!("bytes={saved}-{}", source.origin.state.data.len() - 1)
        );
        assert_eq!(headers.get("x-secret").unwrap(), SECRET);
        assert_eq!(headers.get("user-agent").unwrap(), SECRET);
        assert!(
            headers
                .get("cookie")
                .unwrap()
                .to_str()
                .unwrap()
                .contains(SECRET)
        );
        assert!(
            headers
                .get("referer")
                .unwrap()
                .to_str()
                .unwrap()
                .contains(SECRET)
        );
    }
    let connection = rusqlite::Connection::open(&path).unwrap();
    let saved: String = connection
        .query_row("SELECT request_credentials FROM downloads", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert!(!saved.contains(token));
    drop(connection);
    manager.shutdown().await.unwrap();
    source.close().await;
}
