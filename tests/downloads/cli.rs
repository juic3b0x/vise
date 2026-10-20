use super::fixture::{Fixture, Mode, expected_hashes};
use super::proxy_support;
#[cfg(unix)]
use axum::http::{StatusCode, header};
#[cfg(unix)]
use std::time::Duration;

#[tokio::test]
async fn cli_batch_download_is_successful_and_failure_exits_nonzero() {
    for (mode, expected) in [(Mode::Ranges, 0), (Mode::NotFound, 1)] {
        let fixture = Fixture::start(mode).await;
        let dir = tempfile::tempdir().unwrap();
        let url = fixture.url.clone();
        let output_dir = dir.path().to_owned();
        let output = tokio::task::spawn_blocking(move || {
            super::fixture::command(&output_dir)
                .args(["--batch", "--output"])
                .arg(output_dir)
                .arg(url)
                .output()
                .unwrap()
        })
        .await
        .unwrap();
        assert_eq!(
            output.status.code(),
            Some(expected),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!String::from_utf8_lossy(&output.stdout).contains("MONITOR"));
        assert!(!String::from_utf8_lossy(&output.stdout).contains("STATUS"));
        if expected == 0 {
            assert_eq!(
                tokio::fs::read(dir.path().join("file.bin")).await.unwrap(),
                fixture.state.data
            );
        }
        fixture.close().await;
    }
}

#[cfg(unix)]
#[tokio::test]
async fn interactive_sigterm_exits_without_waiting_for_stdin() {
    let dir = tempfile::tempdir().unwrap();
    let mut process = super::fixture::command(dir.path())
        .arg("--interactive")
        .arg("--output")
        .arg(dir.path())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !dir.path().join(".vise/state.sqlite3").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let status = std::process::Command::new("kill")
        .args(["-TERM", &process.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    let exited = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(status) = process.try_wait().unwrap() {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    if exited.is_err() {
        process.kill().unwrap();
        process.wait().unwrap();
    }
    assert_eq!(exited.unwrap().code(), Some(130));
}

#[cfg(unix)]
#[tokio::test]
async fn webui_binary_logs_actions_progress_and_shutdown_to_stdout() {
    let fixture = Fixture::start(Mode::SlowRanges).await;
    let dir = tempfile::tempdir().unwrap();
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let bind = socket.local_addr().unwrap();
    drop(socket);
    let mut process = super::fixture::command(dir.path())
        .args(["--webui", "--bind"])
        .arg(bind.to_string())
        .args(["--connections", "1", "--output"])
        .arg(dir.path())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    // Drain the OS pipe on a dedicated thread so the child never blocks on logs.
    let stdout = process.stdout.take().unwrap();
    let (sender, mut logs) = tokio::sync::mpsc::unbounded_channel();
    let reader = std::thread::spawn(move || {
        use std::io::BufRead;
        for line in std::io::BufReader::new(stdout).lines() {
            if sender.send(line.unwrap()).is_err() {
                break;
            }
        }
    });
    let mut captured = Vec::new();
    async fn wait_for(
        logs: &mut tokio::sync::mpsc::UnboundedReceiver<String>,
        captured: &mut Vec<String>,
        label: &str,
    ) {
        tokio::time::timeout(Duration::from_secs(6), async {
            loop {
                let line = logs.recv().await.expect("monitor output closed");
                let found = line.contains(&format!("] {label}"));
                captured.push(line);
                if found {
                    break;
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("missing {label}: {captured:?}"));
    }
    // Keep a kill guard so a failing assertion cannot leave a test server running.
    struct ChildGuard(std::process::Child);
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let mut child = ChildGuard(process);
    wait_for(&mut logs, &mut captured, "READY").await;
    let base = format!("http://{bind}");
    let client = reqwest::Client::new();
    let added = client
        .post(format!("{base}/api/downloads"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(serde_json::json!({"url":fixture.url}).to_string())
        .send()
        .await
        .unwrap();
    let item: vise::model::Download =
        serde_json::from_slice(&added.bytes().await.unwrap()).unwrap();
    wait_for(&mut logs, &mut captured, "ADDED").await;
    wait_for(&mut logs, &mut captured, "STATUS").await;
    let status_line = captured.last().unwrap();
    assert!(
        status_line.contains('%')
            && status_line.contains("/s")
            && status_line.contains("downloading")
    );
    assert!(
        status_line.starts_with('[') && status_line.contains('T') && status_line.contains("Z]")
    );
    for (action, label) in [("pause", "PAUSED"), ("resume", "RESUMED")] {
        let response = client
            .post(format!("{base}/api/downloads/{}/{action}", item.id))
            .header(header::CONTENT_TYPE, "application/json")
            .body("{}")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        wait_for(&mut logs, &mut captured, label).await;
    }
    wait_for(&mut logs, &mut captured, "DONE").await;
    let deleted = client
        .delete(format!("{base}/api/downloads/{}", item.id))
        .header(header::CONTENT_TYPE, "application/json")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
    wait_for(&mut logs, &mut captured, "DELETED").await;
    assert!(
        std::process::Command::new("kill")
            .args(["-TERM", &child.0.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    wait_for(&mut logs, &mut captured, "STOPPED").await;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                assert_eq!(status.code(), Some(130));
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    reader.join().unwrap();
    assert!(dir.path().join(&item.filename).exists());
    fixture.close().await;
}

#[tokio::test]
async fn cli_checksum_flags_accept_uppercase_and_mismatches_exit_nonzero() {
    for mismatch in [false, true] {
        let fixture = Fixture::start(Mode::Ranges).await;
        let dir = tempfile::tempdir().unwrap();
        let expected = expected_hashes(&fixture.state.data);
        let mut command = super::fixture::command(dir.path());
        command
            .arg("--batch")
            .arg("--output")
            .arg(dir.path())
            .arg("--expected-sha256")
            .arg(expected.sha256.unwrap().to_uppercase())
            .arg("--expected-md5")
            .arg(if mismatch {
                "0".repeat(32)
            } else {
                expected.md5.unwrap()
            })
            .arg(&fixture.url);
        let output = tokio::task::spawn_blocking(move || command.output().unwrap())
            .await
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(if mismatch { 1 } else { 0 }),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(dir.path().join("file.bin").exists(), !mismatch);
        if mismatch {
            let output = format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(output.contains("MD5 expected"));
            assert!(output.contains("actual"));
        }
        fixture.close().await;
    }
}

#[tokio::test]
async fn cli_proxy_flag_routes_the_entire_batch_and_debug_logs_hide_credentials() {
    use proxy_support::{Kind, TestProxy};
    let fixture = Fixture::start(Mode::Ranges).await;
    let proxy = TestProxy::start(Kind::Http, true).await;
    let dir = tempfile::tempdir().unwrap();
    let mut command = super::fixture::command(dir.path());
    command
        .env("RUST_LOG", "reqwest=debug")
        .args(["--batch", "--proxy"])
        .arg(&proxy.url)
        .arg("--output")
        .arg(dir.path())
        .args([&fixture.url, &fixture.url]);
    let output = tokio::task::spawn_blocking(move || command.output().unwrap())
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let logs = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    for secret in ["alice", "p%40ssword", "p@ssword", "YWxpY2U6cEBzc3dvcmQ="] {
        assert!(!logs.contains(secret));
    }
    assert_eq!(
        proxy.requests.lock().unwrap().len(),
        fixture.state.ranges.lock().unwrap().len() + 2
    );
    for name in ["file.bin", "file (1).bin"] {
        assert_eq!(
            tokio::fs::read(dir.path().join(name)).await.unwrap(),
            fixture.state.data
        );
    }
    proxy.close().await;
    fixture.close().await;
}
