use super::fixture::{Fixture, Mode, expected_hashes, manager, progress, terminal};
use super::proxy_support;
use axum::http::StatusCode;
use axum::http::header;
use futures_util::StreamExt;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use vise::rate_limiter::RateConfiguration;
use vise::{engine::Manager, model::Status};

#[tokio::test]
async fn web_api_and_websocket_share_the_transfer_engine() {
    let fixture = Fixture::start(Mode::Ranges).await;
    let proxy = proxy_support::TestProxy::start(proxy_support::Kind::Http, true).await;
    let dir = tempfile::tempdir().unwrap();
    let manager = manager(&dir, 2).await;
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let bind = socket.local_addr().unwrap();
    drop(socket);
    let stop = CancellationToken::new();
    let web_manager = manager.clone();
    let token = stop.clone();
    let server = tokio::spawn(async move {
        vise::web::run(web_manager, bind, token).await.unwrap();
    });
    let client = reqwest::Client::new();
    let base = format!("http://{bind}");
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if client
                .get(format!("{base}/api/downloads"))
                .send()
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_rate_controls(&client, &base, &manager).await;
    assert_invalid_transfers(&client, &base, &manager, &fixture.url).await;
    let expected = expected_hashes(&fixture.state.data);
    let response = client
        .post(format!("{base}/api/downloads"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(serde_json::json!({"url":fixture.url, "expected_sha256":expected.sha256, "expected_md5":expected.md5, "proxy_url":proxy.url}).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let item: vise::model::Download =
        serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert!(item.proxy_authenticated);
    let public = serde_json::to_string(&item).unwrap();
    assert!(!public.contains("alice"));
    assert!(!public.contains("ssword"));
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{bind}/ws"))
        .await
        .unwrap();
    let message = tokio::time::timeout(Duration::from_secs(2), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let snapshot: Vec<vise::model::Download> =
        serde_json::from_str(message.to_text().unwrap()).unwrap();
    assert_eq!(snapshot[0].id, item.id);
    progress(&manager, item.id).await;
    let paused = client
        .post(format!("{base}/api/downloads/{}/pause", item.id))
        .header(header::CONTENT_TYPE, "application/json")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(paused.status(), StatusCode::OK);
    assert_eq!(manager.get(item.id).await.unwrap().status, Status::Paused);
    let mut telemetry_ws = paused_telemetry(&client, &base, bind, &item).await;
    let resumed = client
        .post(format!("{base}/api/downloads/{}/resume", item.id))
        .header(header::CONTENT_TYPE, "application/json")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(resumed.status(), StatusCode::OK);
    let changed = client
        .post(format!("{base}/api/config"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(r#"{"limit_rate":"off"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(changed.status(), StatusCode::OK);
    let configuration: RateConfiguration = serde_json::from_slice(
        &client
            .get(format!("{base}/api/config"))
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(configuration.limit_bytes_per_second, None);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let message = telemetry_ws.next().await.unwrap().unwrap();
            let update: serde_json::Value =
                serde_json::from_str(message.to_text().unwrap()).unwrap();
            if update["configuration"]["limit_rate"] == "unlimited" {
                break;
            }
        }
    })
    .await
    .expect("telemetry WebSocket must stream live configuration changes");
    let done = terminal(&manager, item.id).await;
    assert_eq!(done.status, Status::Completed, "{:?}", done.error);
    assert!(done.checksums.verified_at.is_some());
    assert_eq!(
        done.checksums.expected(),
        expected_hashes(&fixture.state.data)
    );
    assert!(
        done.chunks
            .iter()
            .all(|c| c.status == vise::model::ChunkStatus::Completed)
    );
    assert_eq!(
        tokio::fs::read(dir.path().join(&done.filename))
            .await
            .unwrap(),
        fixture.state.data
    );
    assert_removal_preserves_output(&client, &base, &manager, &dir, &done).await;
    ws.close(None).await.unwrap();
    telemetry_ws.close(None).await.unwrap();
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
    manager.shutdown().await.unwrap();
    assert!(!proxy.requests.lock().unwrap().is_empty());
    proxy.close().await;
    fixture.close().await;
}

async fn assert_rate_controls(client: &reqwest::Client, base: &str, manager: &Manager) {
    let configuration: RateConfiguration = serde_json::from_slice(
        &client
            .get(format!("{base}/api/config"))
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(configuration.limit_bytes_per_second, None);
    let mut events = manager.subscribe_events();
    let changed = client
        .post(format!("{base}/api/config"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(r#"{"limit_rate":"512K"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(changed.status(), StatusCode::OK);
    let configuration: RateConfiguration =
        serde_json::from_slice(&changed.bytes().await.unwrap()).unwrap();
    assert_eq!(configuration.limit_bytes_per_second, Some(512 * 1024));
    assert_eq!(
        events.recv().await.unwrap().kind,
        vise::events::EventKind::RateLimitChanged {
            bytes_per_second: Some(512 * 1024)
        }
    );
    for (body, origin, expected) in [
        (
            r#"{"limit_rate":"nonsense"}"#,
            base,
            StatusCode::BAD_REQUEST,
        ),
        (
            r#"{"limit_rate":"off"}"#,
            "https://untrusted.example",
            StatusCode::FORBIDDEN,
        ),
    ] {
        let response = client
            .post(format!("{base}/api/config"))
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::ORIGIN, origin)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
        assert_eq!(
            manager.rate_configuration().limit_bytes_per_second,
            Some(512 * 1024)
        );
    }
}

async fn assert_invalid_transfers(
    client: &reqwest::Client,
    base: &str,
    manager: &Manager,
    url: &str,
) {
    let invalid_hash = client
        .post(format!("{base}/api/downloads"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(serde_json::json!({"url": url, "expected_sha256": "bad"}).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(invalid_hash.status(), StatusCode::BAD_REQUEST);
    assert!(manager.list().await.is_empty());
    let invalid_proxy = client.post(format!("{base}/api/downloads"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(serde_json::json!({"url": url, "proxy_url": "socks4://private-user:private-password@proxy:1080"}).to_string())
        .send().await.unwrap();
    assert_eq!(invalid_proxy.status(), StatusCode::BAD_REQUEST);
    let error = invalid_proxy.text().await.unwrap();
    assert!(!error.contains("private-password"));
    assert!(!error.contains("private-user"));
    assert!(manager.list().await.is_empty());
}

type TelemetrySocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn paused_telemetry(
    client: &reqwest::Client,
    base: &str,
    bind: std::net::SocketAddr,
    item: &vise::model::Download,
) -> TelemetrySocket {
    let snapshot: serde_json::Value = serde_json::from_slice(
        &client
            .get(format!("{base}/api/telemetry"))
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(snapshot["timestamp"].as_i64().unwrap() > 0);
    assert_eq!(
        snapshot["configuration"]["limit_bytes_per_second"],
        512 * 1024
    );
    let chunked: vise::model::Download =
        serde_json::from_value(snapshot["downloads"][0].clone()).unwrap();
    assert_eq!(chunked.id, item.id);
    assert_eq!(chunked.chunks.len(), 2);
    assert_eq!(
        chunked
            .chunks
            .iter()
            .map(|c| c.downloaded_bytes)
            .sum::<u64>(),
        chunked.downloaded_bytes
    );
    assert_eq!(chunked.chunks[0].start_byte, 0);
    assert_eq!(
        chunked.chunks[0].end_byte,
        Some(chunked.chunks[1].start_byte)
    );
    assert_eq!(chunked.chunks[1].end_byte, chunked.total_bytes);
    assert!(chunked.chunks.iter().all(|c| matches!(
        c.status,
        vise::model::ChunkStatus::Paused | vise::model::ChunkStatus::Completed
    )));
    let (mut telemetry_ws, _) =
        tokio_tungstenite::connect_async(format!("ws://{bind}/ws/telemetry"))
            .await
            .unwrap();
    let initial = tokio::time::timeout(Duration::from_secs(2), telemetry_ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let initial: serde_json::Value = serde_json::from_str(initial.to_text().unwrap()).unwrap();
    assert_eq!(initial["downloads"][0]["status"], "paused");
    assert_eq!(
        initial["configuration"]["limit_bytes_per_second"],
        512 * 1024
    );
    telemetry_ws
}

async fn assert_removal_preserves_output(
    client: &reqwest::Client,
    base: &str,
    manager: &Manager,
    dir: &tempfile::TempDir,
    done: &vise::model::Download,
) {
    let deleted = client
        .delete(format!("{base}/api/downloads/{}", done.id))
        .header(header::CONTENT_TYPE, "application/json")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
    assert!(manager.list().await.is_empty());
    assert!(dir.path().join(&done.filename).exists());
    assert_eq!(
        client
            .get(format!("{base}/api/downloads/{}", done.id))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
}
