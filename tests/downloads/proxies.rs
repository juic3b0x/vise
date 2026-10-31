use super::fixture::{Fixture, Mode, expected_hashes, manager, progress, terminal};
use super::proxy_support;
use axum::http::StatusCode;
use std::sync::atomic::Ordering;
use tokio::net::TcpListener;
use vise::engine::Config;
use vise::engine::Manager;
use vise::engine::TransferOptions;
use vise::model::Status;

#[tokio::test]
async fn http_and_socks_proxy_routes_carry_probes_parallel_chunks_and_authentication() {
    use proxy_support::{Kind, TestProxy};
    for (kind, auth, remote_dns) in [
        (Kind::Http, false, true),
        (Kind::Http, true, true),
        (Kind::Socks5, false, false),
        (Kind::Socks5, true, false),
        (Kind::Socks5, true, true),
    ] {
        let fixture = Fixture::start(Mode::Ranges).await;
        let proxy = TestProxy::start(kind, auth).await;
        let proxy_url = if matches!(kind, Kind::Socks5) && remote_dns {
            proxy.url.replacen("socks5:", "socks5h:", 1)
        } else {
            proxy.url.clone()
        };
        let mut url = url::Url::parse(&fixture.url).unwrap();
        if remote_dns {
            url.set_host(Some("origin.invalid")).unwrap();
        }
        let dir = tempfile::tempdir().unwrap();
        let manager = manager(&dir, 4).await;
        let item = manager
            .add_with_options(
                url.as_str(),
                None,
                TransferOptions {
                    proxy_url: Some(proxy_url.parse().unwrap()),
                    expected: expected_hashes(&fixture.state.data),
                    ..TransferOptions::default()
                },
            )
            .await
            .unwrap();
        let done = terminal(&manager, item.id).await;
        assert_eq!(done.status, Status::Completed, "{:?}", done.error);
        assert_eq!(
            tokio::fs::read(dir.path().join(&done.filename))
                .await
                .unwrap(),
            fixture.state.data
        );
        assert!(done.checksums.verified_at.is_some());
        assert_eq!(done.proxy_authenticated, auth);
        let serialized = serde_json::to_string(&done).unwrap();
        assert!(!serialized.contains("alice"));
        assert!(!serialized.contains("ssword"));
        let captured = proxy.requests.lock().unwrap().clone();
        // A SOCKS tunnel can keep the probe's connection alive for one segment;
        // HTTP fixture responses close each connection after a request.
        assert!(captured.len() >= if matches!(kind, Kind::Http) { 5 } else { 4 });
        assert!(captured.iter().all(|request| request.domain == remote_dns));
        if matches!(kind, Kind::Http) {
            assert!(
                captured
                    .iter()
                    .any(|request| request.range.as_deref() == Some("bytes=0-0"))
            );
            assert!(captured.iter().all(|request| request.method == "GET"));
        }
        if remote_dns {
            assert!(
                captured
                    .iter()
                    .all(|request| request.authority.contains("origin.invalid"))
            );
        }
        assert!(!fixture.state.proxy_auth_leaked.load(Ordering::SeqCst));
        manager.shutdown().await.unwrap();
        proxy.close().await;
        fixture.close().await;
    }
}

#[tokio::test]
async fn proxy_route_and_credentials_survive_restart_and_override_a_new_default() {
    use proxy_support::{Kind, TestProxy};
    let fixture = Fixture::start(Mode::SlowRanges).await;
    let proxy = TestProxy::start(Kind::Socks5, true).await;
    let dir = tempfile::tempdir().unwrap();
    let original = Manager::open(Config {
        output_dir: dir.path().into(),
        home_dir: Some(dir.path().join("test-home")),
        connections_per_download: 2,
        proxy_url: Some(proxy.url.parse().unwrap()),
        ..Config::default()
    })
    .await
    .unwrap();
    let item = original.add(&fixture.url, None).await.unwrap();
    progress(&original, item.id).await;
    original.pause(item.id).await.unwrap();
    original.shutdown().await.unwrap();
    drop(original);
    let connection =
        rusqlite::Connection::open(dir.path().join("test-home/.vise/state.sqlite3")).unwrap();
    let saved: String = connection
        .query_row(
            "SELECT proxy_url FROM downloads WHERE id=?1",
            [item.id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert!(saved.contains("alice:p%40ssword@"));
    drop(connection);
    let restored = Manager::open(Config {
        output_dir: dir.path().into(),
        home_dir: Some(dir.path().join("test-home")),
        connections_per_download: 2,
        proxy_url: Some("http://127.0.0.1:1".parse().unwrap()),
        ..Config::default()
    })
    .await
    .unwrap();
    let before = proxy.requests.lock().unwrap().len();
    restored.resume(item.id).await.unwrap();
    let done = terminal(&restored, item.id).await;
    assert_eq!(done.status, Status::Completed, "{:?}", done.error);
    assert!(proxy.requests.lock().unwrap().len() > before);
    assert!(done.proxy_authenticated);
    restored.shutdown().await.unwrap();
    proxy.close().await;
    fixture.close().await;
}

#[tokio::test]
async fn explicit_proxy_failure_never_falls_back_to_a_reachable_origin() {
    let fixture = Fixture::start(Mode::Ranges).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let dir = tempfile::tempdir().unwrap();
    let manager = Manager::open(Config {
        output_dir: dir.path().into(),
        home_dir: Some(dir.path().join("test-home")),
        retries: 0,
        ..Config::default()
    })
    .await
    .unwrap();
    let warm = manager.add(&fixture.url, None).await.unwrap();
    assert_eq!(terminal(&manager, warm.id).await.status, Status::Completed);
    let before = fixture.state.ranges.lock().unwrap().len();
    let item = manager
        .add_with_options(
            &fixture.url,
            None,
            TransferOptions {
                proxy_url: Some(
                    format!("http://private-user:private-password@{address}")
                        .parse()
                        .unwrap(),
                ),
                ..TransferOptions::default()
            },
        )
        .await
        .unwrap();
    let failed = terminal(&manager, item.id).await;
    assert_eq!(failed.status, Status::Failed);
    assert_eq!(fixture.state.ranges.lock().unwrap().len(), before);
    let public = serde_json::to_string(&failed).unwrap();
    assert!(!public.contains("private-user"));
    assert!(!public.contains("private-password"));
    assert!(!dir.path().join(&failed.filename).exists());
    let next = manager.add(&fixture.url, None).await.unwrap();
    assert_eq!(terminal(&manager, next.id).await.status, Status::Completed);
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn http_connect_and_encrypted_https_proxies_support_tls_without_disabling_validation() {
    use proxy_support::{CERTIFICATE, HttpsOrigin, Kind, TestProxy};
    for kind in [Kind::Http, Kind::TlsHttp, Kind::Socks5] {
        let origin = HttpsOrigin::start().await;
        let proxy = TestProxy::start(kind, true).await;
        let route: vise::proxy::ProxyUrl = proxy.url.parse().unwrap();
        let rejected = route
            .apply(reqwest::Client::builder())
            .unwrap()
            .build()
            .unwrap()
            .get(&origin.url)
            .send()
            .await;
        assert!(
            rejected.is_err(),
            "self-signed test certificates must not be trusted implicitly"
        );
        let client = route
            .apply(
                reqwest::Client::builder()
                    .add_root_certificate(reqwest::Certificate::from_der(CERTIFICATE).unwrap()),
            )
            .unwrap()
            .build()
            .unwrap();
        let response = client.get(&origin.url).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.bytes().await.unwrap(), "abc");
        let plain_origin = Fixture::start(Mode::Ranges).await;
        assert_eq!(
            client
                .get(&plain_origin.url)
                .send()
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            plain_origin.state.data
        );
        let captured = proxy.requests.lock().unwrap().clone();
        let target = url::Url::parse(&origin.url).unwrap();
        let method = if matches!(kind, Kind::Socks5) {
            "SOCKS CONNECT"
        } else {
            "CONNECT"
        };
        assert!(captured.iter().any(|request| request.method == method
            && request.authority == format!("127.0.0.1:{}", target.port().unwrap())));
        drop(client);
        proxy.close().await;
        origin.close().await;
        plain_origin.close().await;
    }
}

#[tokio::test]
async fn concurrent_transfers_keep_default_and_override_proxy_clients_isolated() {
    use proxy_support::{Kind, TestProxy};
    let fixture = Fixture::start(Mode::Ranges).await;
    let http = TestProxy::start(Kind::Http, true).await;
    let socks = TestProxy::start(Kind::Socks5, true).await;
    let dir = tempfile::tempdir().unwrap();
    let manager = Manager::open(Config {
        output_dir: dir.path().into(),
        home_dir: Some(dir.path().join("test-home")),
        proxy_url: Some(http.url.parse().unwrap()),
        connections_per_download: 2,
        ..Config::default()
    })
    .await
    .unwrap();
    let first = manager.add(&fixture.url, None).await.unwrap();
    let second = manager
        .add_with_options(
            &fixture.url,
            None,
            TransferOptions {
                proxy_url: Some(socks.url.parse().unwrap()),
                ..TransferOptions::default()
            },
        )
        .await
        .unwrap();
    for (item, scheme) in [(first, "http://"), (second, "socks5://")] {
        let done = terminal(&manager, item.id).await;
        assert_eq!(done.status, Status::Completed, "{:?}", done.error);
        assert!(done.proxy_url.unwrap().starts_with(scheme));
        assert_eq!(
            tokio::fs::read(dir.path().join(&done.filename))
                .await
                .unwrap(),
            fixture.state.data
        );
    }
    assert!(http.requests.lock().unwrap().len() >= 3);
    assert!(socks.requests.lock().unwrap().len() >= 2);
    manager.shutdown().await.unwrap();
    http.close().await;
    socks.close().await;
    fixture.close().await;
}
