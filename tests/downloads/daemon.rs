use super::fixture::{Fixture, Mode, manager, progress};
use axum::http::{StatusCode, header};
use futures_util::StreamExt;
#[cfg(unix)]
use std::net::SocketAddr;
use std::time::Duration;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_util::sync::CancellationToken;
use vise::{
    model::{Download, Status},
    web::DaemonConfig,
};

const SECRET: &str = "synthetic-daemon-token-0123456789abcdef";

#[cfg(unix)]
#[tokio::test]
async fn daemon_loads_file_settings_and_environment_authentication_wins() {
    for environment_override in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let mut command = super::fixture::command(root.path());
        let home = root.path().join("test-home");
        let config_dir = home.join(".vise");
        std::fs::create_dir(&config_dir).unwrap();
        let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let bind = socket.local_addr().unwrap();
        drop(socket);
        let file_token = if environment_override {
            "overridden-file-token-0123456789abcdef"
        } else {
            SECRET
        };
        std::fs::write(
            config_dir.join("config.toml"),
            format!(
                r#"[daemon]
host = "127.0.0.1"
port = {}
VISE_API_TOKEN = "{file_token}"
[downloads]
default_dir = "~/Configured"
max_concurrent = 2
global_speed_limit_kbps = 8
"#,
                bind.port()
            ),
        )
        .unwrap();
        command.arg("--daemon");
        if environment_override {
            command.env("VISE_API_TOKEN", SECRET);
        }
        let child = Daemon(
            command
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap(),
        );
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap();
        let base = format!("http://{bind}");
        wait_ready(&client, &base).await;
        let response = client
            .get(format!("{base}/api/config"))
            .bearer_auth(SECRET)
            .send()
            .await
            .unwrap();
        let config: serde_json::Value =
            serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
        assert_eq!(config["limit_bytes_per_second"], 8 * 1024);
        if environment_override {
            let response = client
                .get(format!("{base}/api/config"))
                .bearer_auth(file_token)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }
        child.stop().await;
        assert!(home.join(".vise/state.sqlite3").is_file());
        assert!(!root.path().join("downloads").exists());
    }
}

#[tokio::test]
async fn unsafe_server_configuration_fails_before_creating_a_journal() {
    for (flag, token, message) in [
        (
            "--daemon",
            None,
            "non-loopback daemon binds require VISE_API_TOKEN",
        ),
        (
            "--daemon",
            Some("private-invalid-token"),
            "VISE_API_TOKEN must contain",
        ),
        ("--webui", None, "--bind must use a loopback address"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_owned();
        let output = tokio::task::spawn_blocking(move || {
            let mut command = super::fixture::command(&path);
            let home = path.join("test-home");
            std::fs::create_dir_all(home.join(".vise")).unwrap();
            std::fs::write(
                home.join(".vise/config.toml"),
                "[daemon]\nVISE_API_TOKEN = \"\"\n",
            )
            .unwrap();
            command
                .args([flag, "--bind", "0.0.0.0:8080", "--output"])
                .arg(path)
                .env_remove("VISE_API_TOKEN");
            if let Some(token) = token {
                command.env("VISE_API_TOKEN", token);
            }
            command.output().unwrap()
        })
        .await
        .unwrap();
        assert_eq!(output.status.code(), Some(1));
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(error.contains(message), "{error}");
        assert!(!error.contains("private-invalid-token"));
        assert!(!dir.path().join(".vise").exists());
    }
}

#[tokio::test]
async fn remote_daemon_authenticates_dashboard_api_and_websockets() {
    let fixture = Fixture::start(Mode::SlowRanges).await;
    let dir = tempfile::tempdir().unwrap();
    let manager = manager(&dir, 1).await;
    let socket = std::net::TcpListener::bind("0.0.0.0:0").unwrap();
    let bind = socket.local_addr().unwrap();
    drop(socket);
    let base = format!("http://127.0.0.1:{}", bind.port());
    let stop = CancellationToken::new();
    let server_manager = manager.clone();
    let token = stop.clone();
    let server = tokio::spawn(async move {
        vise::web::run_daemon(
            server_manager,
            DaemonConfig::new(bind, Some(SECRET.into())).unwrap(),
            token,
        )
        .await
        .unwrap();
    });
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    wait_ready(&client, &base).await;
    for path in ["/", "/api/downloads", "/ws/telemetry"] {
        let response = client.get(format!("{base}{path}")).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(response.headers().contains_key(header::WWW_AUTHENTICATE));
    }
    let dashboard = client
        .get(&base)
        .basic_auth("vise", Some(SECRET))
        .send()
        .await
        .unwrap();
    assert_eq!(dashboard.status(), StatusCode::OK);
    assert!(dashboard.text().await.unwrap().contains("Transfer console"));
    let response = client
        .post(format!("{base}/api/downloads"))
        .bearer_auth(SECRET)
        .header(header::CONTENT_TYPE, "application/json")
        .body(serde_json::json!({"url":fixture.url}).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let item: Download = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    progress(&manager, item.id).await;
    let paused = client
        .post(format!("{base}/api/downloads/{}/pause", item.id))
        .bearer_auth(SECRET)
        .header(header::CONTENT_TYPE, "application/json")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(paused.status(), StatusCode::OK);
    assert_eq!(manager.get(item.id).await.unwrap().status, Status::Paused);
    for auth in [format!("Bearer {SECRET}"), {
        use base64::Engine;
        format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(format!("vise:{SECRET}"))
        )
    }] {
        let mut request = format!("ws://127.0.0.1:{}/ws/telemetry", bind.port())
            .into_client_request()
            .unwrap();
        request
            .headers_mut()
            .insert(header::AUTHORIZATION, auth.parse().unwrap());
        let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let message = tokio::time::timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let snapshot: serde_json::Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
        assert_eq!(snapshot["downloads"][0]["id"], item.id.to_string());
        assert_eq!(snapshot["downloads"][0]["status"], "paused");
        socket.close(None).await.unwrap();
    }
    concurrent_reader_allows_api_commit(&client, &base, dir.path()).await;
    for origin in [
        "chrome-extension://abcdefghijklmnopabcdefghijklmnop",
        "moz-extension://d5f97730-863b-4f81-964b-61ab81541856",
    ] {
        let response = client
            .post(format!("{base}/api/config"))
            .bearer_auth(SECRET)
            .header(header::ORIGIN, origin)
            .header("sec-fetch-site", "cross-site")
            .header(header::CONTENT_TYPE, "application/json")
            .body(r#"{"limit_rate":"1M"}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            !response
                .headers()
                .contains_key(header::ACCESS_CONTROL_ALLOW_ORIGIN)
        );
    }
    let rejected = client
        .post(format!("{base}/api/config"))
        .bearer_auth(SECRET)
        .header(header::ORIGIN, "http://untrusted.example")
        .header(header::CONTENT_TYPE, "application/json")
        .body(r#"{"limit_rate":"off"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), StatusCode::FORBIDDEN);
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

async fn wait_ready(client: &reqwest::Client, base: &str) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if client
                .get(format!("{base}/api/downloads"))
                .bearer_auth(SECRET)
                .send()
                .await
                .is_ok_and(|r| r.status() == StatusCode::OK)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

async fn concurrent_reader_allows_api_commit(
    client: &reqwest::Client,
    base: &str,
    output: &std::path::Path,
) {
    let path = output.join("test-home/.vise/state.sqlite3");
    let (ready, started) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let reader = tokio::task::spawn_blocking(move || {
        let mut connection = rusqlite::Connection::open(path).unwrap();
        let mode: String = connection
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .unwrap();
        assert_eq!(mode, "wal");
        let transaction = connection.transaction().unwrap();
        let count: i64 = transaction
            .query_row("SELECT COUNT(*) FROM downloads", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
        ready.send(()).unwrap();
        wait.recv_timeout(Duration::from_secs(5)).unwrap();
        transaction.commit().unwrap();
    });
    started.await.unwrap();
    let response = client
        .post(format!("{base}/api/config"))
        .bearer_auth(SECRET)
        .header(header::CONTENT_TYPE, "application/json")
        .body(r#"{"limit_rate":"1M"}"#)
        .send()
        .await
        .unwrap();
    release.send(()).unwrap();
    reader.await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[cfg(unix)]
struct Daemon(std::process::Child);

#[cfg(unix)]
impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[cfg(unix)]
impl Daemon {
    fn start(flag: &str, bind: SocketAddr, output: &std::path::Path) -> Self {
        Self(
            super::fixture::command(output)
                .env("VISE_API_TOKEN", SECRET)
                .env_remove("RUST_LOG")
                .args([flag, "--bind", &bind.to_string(), "--output"])
                .arg(output)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap(),
        )
    }

    async fn stop(mut self) {
        assert!(
            std::process::Command::new("kill")
                .args(["-TERM", &self.0.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        let status = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(status) = self.0.try_wait().unwrap() {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        use std::io::Read;
        let mut stdout = Vec::new();
        self.0
            .stdout
            .take()
            .unwrap()
            .read_to_end(&mut stdout)
            .unwrap();
        let mut stderr = String::new();
        self.0
            .stderr
            .take()
            .unwrap()
            .read_to_string(&mut stderr)
            .unwrap();
        assert_eq!(status.code(), Some(130), "{stderr}");
        assert!(
            stdout.is_empty(),
            "daemon must not run the terminal controller or logger"
        );
        assert!(stderr.is_empty(), "{stderr}");
    }
}

#[cfg(unix)]
#[tokio::test]
async fn daemon_aliases_remain_quiet_and_restore_transfers_after_sigterm() {
    let fixture = Fixture::start(Mode::SlowRanges).await;
    let output = tempfile::tempdir().unwrap();
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let bind = socket.local_addr().unwrap();
    drop(socket);
    let base = format!("http://{bind}");
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    let first = Daemon::start("-d", bind, output.path());
    wait_ready(&client, &base).await;
    let response = client
        .post(format!("{base}/api/downloads"))
        .bearer_auth(SECRET)
        .header(header::CONTENT_TYPE, "application/json")
        .body(serde_json::json!({"url": fixture.url}).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let item: Download = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    let snapshot = tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let snapshot = get_download(&client, &base, item.id).await;
            if snapshot.downloaded_bytes > 0 {
                break snapshot;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(snapshot.downloaded_bytes < snapshot.total_bytes.unwrap());
    first.stop().await;
    let second = Daemon::start("--daemon", bind, output.path());
    wait_ready(&client, &base).await;
    let restored = get_download(&client, &base, item.id).await;
    assert_eq!(restored.status, Status::Paused);
    assert!(restored.downloaded_bytes >= snapshot.downloaded_bytes);
    let resumed = client
        .post(format!("{base}/api/downloads/{}/resume", item.id))
        .bearer_auth(SECRET)
        .header(header::CONTENT_TYPE, "application/json")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(resumed.status(), StatusCode::OK);
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let snapshot = get_download(&client, &base, item.id).await;
            if snapshot.status == Status::Completed {
                break;
            }
            assert_ne!(snapshot.status, Status::Failed, "{:?}", snapshot.error);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    second.stop().await;
    assert_eq!(
        tokio::fs::read(output.path().join("file.bin"))
            .await
            .unwrap(),
        fixture.state.data
    );
    fixture.close().await;
}

#[cfg(unix)]
async fn get_download(client: &reqwest::Client, base: &str, id: uuid::Uuid) -> Download {
    let response = client
        .get(format!("{base}/api/downloads/{id}"))
        .bearer_auth(SECRET)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(&response.bytes().await.unwrap()).unwrap()
}
