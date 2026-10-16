use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    body::Body,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::Response,
    routing::get,
};
use bytes::Bytes;
use sha2::Digest;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use vise::{
    checksum::ExpectedChecksums,
    engine::{Config, Manager, TransferOptions},
    model::Status,
    rate_limiter::{RateConfiguration, RateLimit},
};

#[path = "support/proxy.rs"]
mod proxy_support;

#[derive(Clone, Copy)]
enum Mode {
    Ranges,
    IgnoreRanges,
    UnknownLength,
    BadRange,
    Truncated,
    Transient,
    Empty,
    NotFound,
    Stalled,
    NoValidator,
    TruncatedNoValidator,
    SlowRanges,
    MetadataRepr,
    MetadataContent,
    PartialDigest,
}

struct Fixture {
    url: String,
    state: Arc<ServerState>,
    stop: CancellationToken,
    server: tokio::task::JoinHandle<()>,
}
struct ServerState {
    data: Vec<u8>,
    mode: Mode,
    fail_once: AtomicBool,
    version: AtomicUsize,
    ranges: Mutex<Vec<(usize, usize)>>,
    active: AtomicUsize,
    peak: AtomicUsize,
    proxy_auth_leaked: AtomicBool,
}
struct Active(Arc<ServerState>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Fixture {
    async fn start(mode: Mode) -> Self {
        let state = Arc::new(ServerState {
            data: (0..8 * 1024 * 1024 + 137)
                .map(|i| (i % 251) as u8)
                .collect(),
            mode,
            fail_once: AtomicBool::new(true),
            version: AtomicUsize::new(0),
            ranges: Mutex::new(vec![]),
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            proxy_auth_leaked: AtomicBool::new(false),
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/file.bin", listener.local_addr().unwrap());
        let router = Router::new()
            .route("/file.bin", get(serve))
            .with_state(state.clone());
        let stop = CancellationToken::new();
        let token = stop.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(token.cancelled_owned())
                .await
                .unwrap();
        });
        Self {
            url,
            state,
            stop,
            server,
        }
    }
    async fn close(self) {
        self.stop.cancel();
        self.server.await.unwrap();
    }
}

async fn serve(State(state): State<Arc<ServerState>>, headers: HeaderMap) -> Response {
    if headers.contains_key("proxy-authorization") {
        state.proxy_auth_leaked.store(true, Ordering::SeqCst);
    }
    if matches!(state.mode, Mode::NotFound) {
        return Response::builder().status(404).body(Body::empty()).unwrap();
    }
    if matches!(state.mode, Mode::Empty) {
        return Response::builder()
            .status(416)
            .header(header::CONTENT_RANGE, "bytes */0")
            .body(Body::empty())
            .unwrap();
    }
    let range = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("bytes="))
        .and_then(|v| v.split_once('-'))
        .map(|(start, end)| {
            (
                start.parse::<usize>().unwrap(),
                end.parse::<usize>().unwrap(),
            )
        });
    let probe = range == Some((0, 0));
    let honors_range = !matches!(
        state.mode,
        Mode::IgnoreRanges | Mode::UnknownLength | Mode::MetadataContent
    );
    let (start, end) = if honors_range {
        range.unwrap_or((0, state.data.len() - 1))
    } else {
        (0, state.data.len() - 1)
    };
    if start >= state.data.len() || end >= state.data.len() {
        return Response::builder().status(416).body(Body::empty()).unwrap();
    }
    if matches!(state.mode, Mode::Transient)
        && !probe
        && state.fail_once.swap(false, Ordering::SeqCst)
    {
        return Response::builder()
            .status(503)
            .header(header::RETRY_AFTER, "1")
            .body(Body::empty())
            .unwrap();
    }
    let mut builder = Response::builder().status(if honors_range && range.is_some() {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    });
    let version = state.version.load(Ordering::SeqCst);
    if matches!(state.mode, Mode::MetadataRepr | Mode::PartialDigest)
        || (matches!(state.mode, Mode::MetadataContent) && !probe)
    {
        use base64::Engine;
        let data = if matches!(state.mode, Mode::PartialDigest) {
            &state.data[start..=end]
        } else {
            &state.data[..]
        };
        let sha = base64::engine::general_purpose::STANDARD.encode(sha2::Sha256::digest(data));
        let md5 = base64::engine::general_purpose::STANDARD.encode(md5::Md5::digest(data));
        builder = builder.header(
            if matches!(state.mode, Mode::MetadataRepr) {
                "repr-digest"
            } else {
                "content-digest"
            },
            format!("sha-256=:{sha}:, md5=:{md5}:"),
        );
        if !matches!(state.mode, Mode::MetadataRepr) {
            builder = builder.header("content-md5", md5);
        }
    }
    if !matches!(state.mode, Mode::NoValidator | Mode::TruncatedNoValidator) {
        builder = builder.header(header::ETAG, format!("\"version-{version}\""));
    }
    if honors_range && range.is_some() {
        let wrong = usize::from(matches!(state.mode, Mode::BadRange) && !probe);
        builder = builder.header(
            header::CONTENT_RANGE,
            format!("bytes {}-{end}/{}", start + wrong, state.data.len()),
        );
    }
    if !matches!(state.mode, Mode::UnknownLength) {
        builder = builder.header(header::CONTENT_LENGTH, end - start + 1);
    }
    if probe && honors_range {
        return builder.body(Body::from(vec![state.data[0]])).unwrap();
    }
    state.ranges.lock().unwrap().push((start, end));
    let active = state.active.fetch_add(1, Ordering::SeqCst) + 1;
    state.peak.fetch_max(active, Ordering::SeqCst);
    let guard = Active(state.clone());
    let truncated = matches!(state.mode, Mode::Truncated | Mode::TruncatedNoValidator)
        && state.fail_once.swap(false, Ordering::SeqCst);
    let stream = futures_util::stream::unfold(
        (start, 0, guard),
        move |(offset, pieces, guard)| async move {
            if offset > end {
                return None;
            }
            if matches!(guard.0.mode, Mode::Stalled) {
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            tokio::time::sleep(Duration::from_millis(
                if matches!(guard.0.mode, Mode::SlowRanges) {
                    20
                } else {
                    6
                },
            ))
            .await;
            if truncated && pieces == 6 {
                return Some((
                    Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "intentional drop",
                    )),
                    (end + 1, pieces, guard),
                ));
            }
            let next = (offset + 64 * 1024).min(end + 1);
            let bytes: Vec<_> = guard.0.data[offset..next]
                .iter()
                .map(|b| b.wrapping_add(version as u8))
                .collect();
            Some((Ok(Bytes::from(bytes)), (next, pieces + 1, guard)))
        },
    );
    builder.body(Body::from_stream(stream)).unwrap()
}

async fn manager(dir: &tempfile::TempDir, connections: usize) -> Manager {
    Manager::open(Config {
        output_dir: dir.path().into(),
        concurrent_downloads: 1,
        connections_per_download: connections,
        retries: 2,
        ..Config::default()
    })
    .await
    .unwrap()
}
async fn terminal(manager: &Manager, id: Uuid) -> vise::model::Download {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let item = manager.get(id).await.unwrap();
            if matches!(
                item.status,
                Status::Completed | Status::Failed | Status::Corrupt
            ) {
                return item;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("download did not terminate")
}
async fn progress(manager: &Manager, id: Uuid) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if manager.get(id).await.unwrap().downloaded_bytes >= 256 * 1024 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("download did not make progress");
}

#[tokio::test]
async fn parallel_ranges_are_bounded_and_byte_exact() {
    let fixture = Fixture::start(Mode::Ranges).await;
    let dir = tempfile::tempdir().unwrap();
    let manager = manager(&dir, 4).await;
    let first = manager.add(&fixture.url, None).await.unwrap();
    let second = manager.add(&fixture.url, None).await.unwrap();
    assert_ne!(first.filename, second.filename);
    for id in [first.id, second.id] {
        let item = terminal(&manager, id).await;
        assert_eq!(item.status, Status::Completed, "{:?}", item.error);
        assert_eq!(
            tokio::fs::read(dir.path().join(item.filename))
                .await
                .unwrap(),
            fixture.state.data
        );
    }
    assert!(fixture.state.peak.load(Ordering::SeqCst) > 1);
    assert!(fixture.state.peak.load(Ordering::SeqCst) <= 4);
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn global_rate_budget_is_shared_by_parallel_files_and_chunks() {
    let fixture = Fixture::start(Mode::Ranges).await;
    let dir = tempfile::tempdir().unwrap();
    let manager = Manager::open(Config {
        output_dir: dir.path().into(),
        concurrent_downloads: 2,
        connections_per_download: 2,
        limit_rate: Some("4M".parse().unwrap()),
        ..Config::default()
    })
    .await
    .unwrap();
    let started = tokio::time::Instant::now();
    let first = manager.add(&fixture.url, None).await.unwrap();
    let second = manager.add(&fixture.url, None).await.unwrap();
    for id in [first.id, second.id] {
        let done = terminal(&manager, id).await;
        assert_eq!(done.status, Status::Completed, "{:?}", done.error);
        assert_eq!(
            tokio::fs::read(dir.path().join(done.filename))
                .await
                .unwrap(),
            fixture.state.data
        );
    }
    assert!(
        started.elapsed() >= Duration::from_millis(3900),
        "each file must share the global 4 MiB/s budget"
    );
    assert!(fixture.state.peak.load(Ordering::SeqCst) > 1);
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn live_limits_preserve_the_open_response_and_do_not_trigger_idle_timeout() {
    let fixture = Fixture::start(Mode::Ranges).await;
    let dir = tempfile::tempdir().unwrap();
    let manager = Manager::open(Config {
        output_dir: dir.path().into(),
        connections_per_download: 1,
        retries: 0,
        read_timeout: Duration::from_millis(200),
        limit_rate: Some("8K".parse().unwrap()),
        ..Config::default()
    })
    .await
    .unwrap();
    let item = manager.add(&fixture.url, None).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while manager.get(item.id).await.unwrap().downloaded_bytes == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    manager
        .set_rate_limit("256K".parse().unwrap())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;
    manager.set_rate_limit("1".parse().unwrap()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    manager.set_rate_limit(RateLimit::UNLIMITED).await.unwrap();
    let done = terminal(&manager, item.id).await;
    assert_eq!(done.status, Status::Completed, "{:?}", done.error);
    assert_eq!(
        fixture.state.ranges.lock().unwrap().len(),
        1,
        "rate changes must retain the existing HTTP response"
    );
    assert_eq!(
        tokio::fs::read(dir.path().join(done.filename))
            .await
            .unwrap(),
        fixture.state.data
    );
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn rate_changes_are_durable_and_failed_commits_preserve_the_live_limit() {
    let dir = tempfile::tempdir().unwrap();
    let original = manager(&dir, 1).await;
    let rate: RateLimit = "2M".parse().unwrap();
    original.set_rate_limit(rate).await.unwrap();
    let connection = rusqlite::Connection::open(dir.path().join(".vise/state.sqlite3")).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_rate BEFORE UPDATE ON metadata WHEN NEW.key = 'limit_rate' BEGIN SELECT RAISE(ABORT, 'intentional failure'); END;").unwrap();
    assert!(
        original
            .set_rate_limit("3M".parse().unwrap())
            .await
            .is_err()
    );
    assert_eq!(original.rate_configuration(), RateConfiguration::from(rate));
    connection
        .execute_batch("DROP TRIGGER reject_rate;")
        .unwrap();
    drop(connection);
    original.shutdown().await.unwrap();
    drop(original);
    let restored = manager(&dir, 1).await;
    assert_eq!(restored.rate_configuration(), RateConfiguration::from(rate));
    restored.shutdown().await.unwrap();
    drop(restored);
    let cleared = Manager::open(Config {
        output_dir: dir.path().into(),
        limit_rate: Some(RateLimit::UNLIMITED),
        ..Config::default()
    })
    .await
    .unwrap();
    assert_eq!(cleared.rate_configuration().limit_bytes_per_second, None);
    cleared.shutdown().await.unwrap();
    drop(cleared);
    let restored = manager(&dir, 1).await;
    assert_eq!(restored.rate_configuration().limit_bytes_per_second, None);
    restored.shutdown().await.unwrap();
}

#[tokio::test]
async fn pause_resume_survives_process_restart() {
    let fixture = Fixture::start(Mode::Ranges).await;
    let dir = tempfile::tempdir().unwrap();
    let first_manager = manager(&dir, 2).await;
    let item = first_manager.add(&fixture.url, None).await.unwrap();
    progress(&first_manager, item.id).await;
    first_manager.pause(item.id).await.unwrap();
    let paused = first_manager.get(item.id).await.unwrap();
    assert_eq!(paused.status, Status::Paused);
    assert!(paused.downloaded_bytes > 0);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        first_manager.get(item.id).await.unwrap().downloaded_bytes,
        paused.downloaded_bytes
    );
    let database = vise::database::Database::open(dir.path().join(".vise/state.sqlite3"))
        .await
        .unwrap();
    let durable = database.list().await.unwrap();
    assert_eq!(durable[0].downloaded_bytes, paused.downloaded_bytes);
    assert_eq!(durable[0].created_at, paused.created_at);
    assert_eq!(durable[0].status, Status::Paused);
    drop(database);
    let connection = rusqlite::Connection::open(dir.path().join(".vise/state.sqlite3")).unwrap();
    let expected: Vec<(usize, usize)> = connection.prepare("SELECT start_byte+downloaded_bytes,end_byte-1 FROM chunks WHERE downloaded_bytes < end_byte-start_byte ORDER BY chunk_index").unwrap()
        .query_map([], |row| Ok((row.get::<_, i64>(0)? as usize, row.get::<_, i64>(1)? as usize))).unwrap().collect::<Result<_, _>>().unwrap();
    drop(connection);
    first_manager.shutdown().await.unwrap();
    drop(first_manager);
    let manager = manager(&dir, 2).await;
    assert_eq!(
        manager.get(item.id).await.unwrap().downloaded_bytes,
        paused.downloaded_bytes
    );
    let before = fixture.state.ranges.lock().unwrap().len();
    manager.resume(item.id).await.unwrap();
    let done = terminal(&manager, item.id).await;
    assert_eq!(done.status, Status::Completed, "{:?}", done.error);
    assert_eq!(
        tokio::fs::read(dir.path().join(done.filename))
            .await
            .unwrap(),
        fixture.state.data
    );
    let mut actual = fixture.state.ranges.lock().unwrap()[before..].to_vec();
    let mut expected = expected;
    actual.sort_unstable();
    expected.sort_unstable();
    assert_eq!(
        actual, expected,
        "resume must not request already-journaled bytes"
    );
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn changed_validator_restarts_instead_of_splicing_versions() {
    let fixture = Fixture::start(Mode::Ranges).await;
    let dir = tempfile::tempdir().unwrap();
    let manager = manager(&dir, 2).await;
    let item = manager.add(&fixture.url, None).await.unwrap();
    progress(&manager, item.id).await;
    manager.pause(item.id).await.unwrap();
    fixture.state.version.store(1, Ordering::SeqCst);
    manager.resume(item.id).await.unwrap();
    let done = terminal(&manager, item.id).await;
    assert_eq!(done.status, Status::Completed, "{:?}", done.error);
    let expected: Vec<_> = fixture
        .state
        .data
        .iter()
        .map(|b| b.wrapping_add(1))
        .collect();
    assert_eq!(
        tokio::fs::read(dir.path().join(done.filename))
            .await
            .unwrap(),
        expected
    );
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn retries_dropped_ranges_from_written_offset() {
    let fixture = Fixture::start(Mode::Truncated).await;
    let dir = tempfile::tempdir().unwrap();
    let manager = manager(&dir, 1).await;
    let item = manager.add(&fixture.url, None).await.unwrap();
    let done = terminal(&manager, item.id).await;
    assert_eq!(done.status, Status::Completed, "{:?}", done.error);
    assert_eq!(
        tokio::fs::read(dir.path().join(done.filename))
            .await
            .unwrap(),
        fixture.state.data
    );
    assert!(
        fixture
            .state
            .ranges
            .lock()
            .unwrap()
            .iter()
            .any(|(start, _)| *start > 0)
    );
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn handles_ignored_ranges_chunked_streams_transient_errors_and_empty_files() {
    for mode in [
        Mode::IgnoreRanges,
        Mode::UnknownLength,
        Mode::Transient,
        Mode::Empty,
        Mode::NoValidator,
        Mode::TruncatedNoValidator,
    ] {
        let fixture = Fixture::start(mode).await;
        let dir = tempfile::tempdir().unwrap();
        let manager = manager(&dir, 4).await;
        let item = manager.add(&fixture.url, None).await.unwrap();
        let done = terminal(&manager, item.id).await;
        assert_eq!(done.status, Status::Completed, "{:?}", done.error);
        let output = tokio::fs::read(dir.path().join(done.filename))
            .await
            .unwrap();
        if matches!(mode, Mode::Empty) {
            assert!(output.is_empty());
        } else {
            assert_eq!(output, fixture.state.data);
        }
        manager.shutdown().await.unwrap();
        fixture.close().await;
    }
}

#[tokio::test]
async fn permanent_errors_and_invalid_ranges_do_not_publish_output() {
    for mode in [Mode::NotFound, Mode::BadRange] {
        let fixture = Fixture::start(mode).await;
        let dir = tempfile::tempdir().unwrap();
        let manager = manager(&dir, 2).await;
        let item = manager.add(&fixture.url, None).await.unwrap();
        let done = terminal(&manager, item.id).await;
        assert_eq!(done.status, Status::Failed);
        assert!(done.error.is_some());
        assert!(!dir.path().join(done.filename).exists());
        manager.shutdown().await.unwrap();
        fixture.close().await;
    }
}

#[tokio::test]
async fn queued_and_stalled_downloads_pause_promptly() {
    let fixture = Fixture::start(Mode::Stalled).await;
    let dir = tempfile::tempdir().unwrap();
    let manager = manager(&dir, 2).await;
    let first = manager.add(&fixture.url, None).await.unwrap();
    let second = manager.add(&fixture.url, None).await.unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    tokio::time::timeout(Duration::from_secs(1), manager.pause(second.id))
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), manager.pause(first.id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(manager.get(first.id).await.unwrap().status, Status::Paused);
    assert_eq!(manager.get(second.id).await.unwrap().status, Status::Paused);
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn refuses_existing_outputs_and_concurrent_managers() {
    let fixture = Fixture::start(Mode::Ranges).await;
    let dir = tempfile::tempdir().unwrap();
    let manager = manager(&dir, 2).await;
    assert!(
        Manager::open(Config {
            output_dir: dir.path().into(),
            ..Default::default()
        })
        .await
        .is_err()
    );
    tokio::fs::write(dir.path().join("keep.bin"), b"original")
        .await
        .unwrap();
    assert!(manager.add(&fixture.url, Some("keep.bin")).await.is_err());
    let item = manager.add(&fixture.url, Some("raced.bin")).await.unwrap();
    tokio::fs::write(dir.path().join("raced.bin"), b"external writer")
        .await
        .unwrap();
    let done = terminal(&manager, item.id).await;
    assert_eq!(done.status, Status::Failed);
    assert_eq!(
        tokio::fs::read(dir.path().join("raced.bin")).await.unwrap(),
        b"external writer"
    );
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn cli_batch_download_is_successful_and_failure_exits_nonzero() {
    for (mode, expected) in [(Mode::Ranges, 0), (Mode::NotFound, 1)] {
        let fixture = Fixture::start(mode).await;
        let dir = tempfile::tempdir().unwrap();
        let url = fixture.url.clone();
        let output_dir = dir.path().to_owned();
        let output = tokio::task::spawn_blocking(move || {
            std::process::Command::new(env!("CARGO_BIN_EXE_vise"))
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

#[tokio::test]
async fn web_api_and_websocket_share_the_transfer_engine() {
    use futures_util::StreamExt;
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
            base.as_str(),
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
    let invalid_hash = client
        .post(format!("{base}/api/downloads"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(serde_json::json!({"url": fixture.url, "expected_sha256": "bad"}).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(invalid_hash.status(), StatusCode::BAD_REQUEST);
    assert!(manager.list().await.is_empty());
    let invalid_proxy = client.post(format!("{base}/api/downloads"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(serde_json::json!({"url": fixture.url, "proxy_url": "socks4://private-user:private-password@proxy:1080"}).to_string())
        .send().await.unwrap();
    assert_eq!(invalid_proxy.status(), StatusCode::BAD_REQUEST);
    let error = invalid_proxy.text().await.unwrap();
    assert!(!error.contains("private-password"));
    assert!(!error.contains("private-user"));
    assert!(manager.list().await.is_empty());
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
    let deleted = client
        .delete(format!("{base}/api/downloads/{}", item.id))
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
            .get(format!("{base}/api/downloads/{}", item.id))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
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

#[tokio::test]
async fn stalled_reads_exhaust_a_bounded_retry_budget() {
    let fixture = Fixture::start(Mode::Stalled).await;
    let dir = tempfile::tempdir().unwrap();
    let manager = Manager::open(Config {
        output_dir: dir.path().into(),
        concurrent_downloads: 1,
        connections_per_download: 1,
        retries: 1,
        read_timeout: Duration::from_millis(100),
        limit_rate: None,
        proxy_url: None,
    })
    .await
    .unwrap();
    let item = manager.add(&fixture.url, None).await.unwrap();
    let done = terminal(&manager, item.id).await;
    assert_eq!(done.status, Status::Failed);
    assert_eq!(fixture.state.ranges.lock().unwrap().len(), 2);
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn missing_completed_file_is_redownloaded_without_reusing_stale_offsets() {
    let fixture = Fixture::start(Mode::Ranges).await;
    let dir = tempfile::tempdir().unwrap();
    let original = manager(&dir, 2).await;
    let item = original.add(&fixture.url, None).await.unwrap();
    let done = terminal(&original, item.id).await;
    assert_eq!(done.status, Status::Completed);
    original.shutdown().await.unwrap();
    drop(original);
    tokio::fs::remove_file(dir.path().join(&done.filename))
        .await
        .unwrap();
    let manager = manager(&dir, 2).await;
    assert_eq!(manager.get(item.id).await.unwrap().status, Status::Failed);
    manager.resume(item.id).await.unwrap();
    let done = terminal(&manager, item.id).await;
    assert_eq!(done.status, Status::Completed, "{:?}", done.error);
    assert_eq!(
        tokio::fs::read(dir.path().join(done.filename))
            .await
            .unwrap(),
        fixture.state.data
    );
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[cfg(unix)]
#[tokio::test]
async fn interactive_sigterm_exits_without_waiting_for_stdin() {
    let dir = tempfile::tempdir().unwrap();
    let mut process = std::process::Command::new(env!("CARGO_BIN_EXE_vise"))
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

#[tokio::test]
async fn abrupt_process_death_restores_durable_offsets_and_finishes_exactly() {
    let fixture = Fixture::start(Mode::SlowRanges).await;
    let dir = tempfile::tempdir().unwrap();
    let mut process = std::process::Command::new(env!("CARGO_BIN_EXE_vise"))
        .args(["--batch", "--connections", "1", "--output"])
        .arg(dir.path())
        .arg(&fixture.url)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let saved = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let path = dir.path().join(".vise/state.sqlite3");
            let saved = tokio::task::spawn_blocking(move || -> Option<(Uuid, u64)> {
                let connection = rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).ok()?;
                connection.busy_timeout(Duration::from_millis(100)).ok()?;
                connection.query_row("SELECT d.id, COALESCE(SUM(c.downloaded_bytes),0) FROM downloads d LEFT JOIN chunks c ON c.download_id=d.id GROUP BY d.id", [], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                }).ok().and_then(|(id, bytes)| id.parse().ok().and_then(|id| u64::try_from(bytes).ok().map(|bytes| (id, bytes))))
            }).await.unwrap();
            if let Some((id, written)) = saved {
                if written > 0 && written < fixture.state.data.len() as u64 { return (id, written); }
            }
            if process.try_wait().unwrap().is_some() {
                panic!("child exited before a progress checkpoint");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    process.kill().unwrap();
    process.wait().unwrap();
    let (id, written) = saved.unwrap();
    let manager = manager(&dir, 1).await;
    let recovered = manager.get(id).await.unwrap();
    assert_eq!(recovered.status, Status::Paused);
    assert!(recovered.downloaded_bytes >= written);
    manager.resume(id).await.unwrap();
    let done = terminal(&manager, id).await;
    assert_eq!(done.status, Status::Completed, "{:?}", done.error);
    assert_eq!(
        tokio::fs::read(dir.path().join(done.filename))
            .await
            .unwrap(),
        fixture.state.data
    );
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn lifecycle_events_are_ordered_and_deletion_is_durable() {
    use vise::events::EventKind;
    let fixture = Fixture::start(Mode::SlowRanges).await;
    let dir = tempfile::tempdir().unwrap();
    let manager = manager(&dir, 2).await;
    let mut events = manager.subscribe_events();
    let item = manager.add(&fixture.url, None).await.unwrap();
    assert_eq!(events.recv().await.unwrap().kind, EventKind::Added);
    progress(&manager, item.id).await;
    manager.pause(item.id).await.unwrap();
    manager.resume(item.id).await.unwrap();
    let done = terminal(&manager, item.id).await;
    assert_eq!(done.status, Status::Completed);
    manager.delete(item.id).await.unwrap();
    let mut kinds = Vec::new();
    while let Ok(event) = events.try_recv() {
        kinds.push(event.kind);
    }
    for kind in [
        EventKind::PauseRequested,
        EventKind::Paused,
        EventKind::Resumed,
        EventKind::Completed,
        EventKind::DeleteRequested,
        EventKind::Deleted,
    ] {
        assert!(kinds.contains(&kind), "missing {kind:?}: {kinds:?}");
    }
    let completed = kinds
        .iter()
        .position(|kind| *kind == EventKind::Completed)
        .unwrap();
    let deleted = kinds
        .iter()
        .position(|kind| *kind == EventKind::Deleted)
        .unwrap();
    assert!(completed < deleted);
    assert!(dir.path().join(&done.filename).exists());
    let active = manager.add(&fixture.url, None).await.unwrap();
    progress(&manager, active.id).await;
    manager.pause(active.id).await.unwrap();
    let (resumed, removed) = tokio::join!(manager.resume(active.id), manager.delete(active.id));
    assert!(resumed.is_ok() || matches!(resumed, Err(vise::error::Error::NotFound(_))));
    removed.unwrap();
    assert!(manager.list().await.is_empty());
    assert!(
        !dir.path()
            .join(".vise")
            .join(format!("{}.part", active.id))
            .exists()
    );
    assert!(!dir.path().join(&active.filename).exists());
    manager.shutdown().await.unwrap();
    drop(manager);
    let manager = self::manager(&dir, 2).await;
    assert!(manager.list().await.is_empty());
    assert!(dir.path().join(done.filename).exists());
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[cfg(unix)]
#[tokio::test]
async fn webui_binary_logs_actions_progress_and_shutdown_to_stdout() {
    let fixture = Fixture::start(Mode::SlowRanges).await;
    let dir = tempfile::tempdir().unwrap();
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let bind = socket.local_addr().unwrap();
    drop(socket);
    let mut process = std::process::Command::new(env!("CARGO_BIN_EXE_vise"))
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
async fn shutdown_waits_for_all_workers_even_when_checkpointing_fails() {
    let fixture = Fixture::start(Mode::SlowRanges).await;
    let dir = tempfile::tempdir().unwrap();
    let manager = manager(&dir, 1).await;
    let active = manager.add(&fixture.url, None).await.unwrap();
    let queued = manager.add(&fixture.url, None).await.unwrap();
    progress(&manager, active.id).await;
    // Abort real SQLite updates to exercise transaction failure during shutdown.
    let connection = rusqlite::Connection::open(dir.path().join(".vise/state.sqlite3")).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_updates BEFORE UPDATE ON downloads BEGIN SELECT RAISE(ABORT, 'test checkpoint failure'); END;").unwrap();
    assert!(manager.shutdown().await.is_err());
    let snapshot = manager.get(active.id).await.unwrap();
    assert_eq!(snapshot.status, Status::Paused);
    assert_eq!(manager.get(queued.id).await.unwrap().status, Status::Paused);
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(
        manager.get(active.id).await.unwrap().downloaded_bytes,
        snapshot.downloaded_bytes
    );
    connection
        .execute_batch("DROP TRIGGER reject_updates")
        .unwrap();
    drop(connection);
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn legacy_json_is_imported_once_and_resumes_without_redownloading_saved_bytes() {
    let fixture = Fixture::start(Mode::Ranges).await;
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join(".vise");
    tokio::fs::create_dir(&state_dir).await.unwrap();
    let id = Uuid::new_v4();
    let written = 256 * 1024;
    let part = state_dir.join(format!("{id}.part"));
    tokio::fs::write(&part, &fixture.state.data[..written])
        .await
        .unwrap();
    tokio::fs::OpenOptions::new()
        .write(true)
        .open(&part)
        .await
        .unwrap()
        .set_len(fixture.state.data.len() as u64)
        .await
        .unwrap();
    let legacy = serde_json::json!({"version":1,"downloads":[{
        "id":id,"url":fixture.url,"filename":"legacy.bin","status":"downloading",
        "total_bytes":fixture.state.data.len(),"ranged":true,"validator":"\"version-0\"",
        "chunks":[{"start":0,"end":fixture.state.data.len(),"written":written}],"error":null
    }]});
    tokio::fs::write(
        state_dir.join("state.json"),
        serde_json::to_vec(&legacy).unwrap(),
    )
    .await
    .unwrap();
    let manager = manager(&dir, 2).await;
    assert!(state_dir.join("state.json.migrated").exists());
    assert!(!state_dir.join("state.json").exists());
    let restored = manager.get(id).await.unwrap();
    assert_eq!(restored.status, Status::Paused);
    assert_eq!(restored.downloaded_bytes, written as u64);
    assert!(restored.created_at > 0);
    manager.resume(id).await.unwrap();
    let done = terminal(&manager, id).await;
    assert_eq!(done.status, Status::Completed, "{:?}", done.error);
    assert_eq!(fixture.state.ranges.lock().unwrap()[0].0, written);
    assert_eq!(
        tokio::fs::read(dir.path().join(&done.filename))
            .await
            .unwrap(),
        fixture.state.data
    );
    manager.delete(id).await.unwrap();
    manager.shutdown().await.unwrap();
    drop(manager);
    // Simulate a crash after import commit but before the JSON backup rename.
    tokio::fs::copy(
        state_dir.join("state.json.migrated"),
        state_dir.join("state.json"),
    )
    .await
    .unwrap();
    let manager = self::manager(&dir, 2).await;
    assert!(
        manager.list().await.is_empty(),
        "stale JSON must never resurrect deleted rows"
    );
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn invalid_legacy_json_is_preserved_and_can_be_repaired() {
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join(".vise");
    tokio::fs::create_dir(&state_dir).await.unwrap();
    tokio::fs::write(state_dir.join("state.json"), b"invalid JSON")
        .await
        .unwrap();
    assert!(
        Manager::open(Config {
            output_dir: dir.path().into(),
            ..Default::default()
        })
        .await
        .is_err()
    );
    assert_eq!(
        tokio::fs::read(state_dir.join("state.json")).await.unwrap(),
        b"invalid JSON"
    );
    tokio::fs::write(
        state_dir.join("state.json"),
        br#"{"version":1,"downloads":[]}"#,
    )
    .await
    .unwrap();
    let manager = manager(&dir, 1).await;
    assert!(manager.list().await.is_empty());
    assert!(state_dir.join("state.json.migrated").exists());
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn recovers_a_published_file_before_sql_completion_without_any_http_transfer() {
    let fixture = Fixture::start(Mode::Ranges).await;
    let dir = tempfile::tempdir().unwrap();
    let first = manager(&dir, 2).await;
    let item = first.add(&fixture.url, None).await.unwrap();
    let done = terminal(&first, item.id).await;
    assert_eq!(done.status, Status::Completed);
    first.shutdown().await.unwrap();
    drop(first);
    let part = dir.path().join(".vise").join(format!("{}.part", item.id));
    tokio::fs::hard_link(dir.path().join(&item.filename), &part)
        .await
        .unwrap();
    let connection = rusqlite::Connection::open(dir.path().join(".vise/state.sqlite3")).unwrap();
    connection
        .execute(
            "UPDATE downloads SET status='downloading' WHERE id=?1",
            [item.id.to_string()],
        )
        .unwrap();
    drop(connection);
    let requests = fixture.state.ranges.lock().unwrap().len();
    let manager = manager(&dir, 2).await;
    assert_eq!(
        manager.get(item.id).await.unwrap().status,
        Status::Completed
    );
    assert!(!part.exists());
    let database = vise::database::Database::open(dir.path().join(".vise/state.sqlite3"))
        .await
        .unwrap();
    assert_eq!(database.list().await.unwrap()[0].status, Status::Completed);
    assert_eq!(requests, fixture.state.ranges.lock().unwrap().len());
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

fn expected_hashes(bytes: &[u8]) -> ExpectedChecksums {
    ExpectedChecksums::new(
        Some(&format!("{:x}", sha2::Sha256::digest(bytes))),
        Some(&format!("{:x}", md5::Md5::digest(bytes))),
    )
    .unwrap()
}

#[tokio::test]
async fn checksum_success_is_journaled_before_publication_for_ranges_streams_and_empty_files() {
    for mode in [Mode::Ranges, Mode::UnknownLength, Mode::Empty] {
        let fixture = Fixture::start(mode).await;
        let dir = tempfile::tempdir().unwrap();
        let manager = manager(&dir, 4).await;
        let expected = expected_hashes(if matches!(mode, Mode::Empty) {
            &[]
        } else {
            &fixture.state.data
        });
        let mut events = manager.subscribe_events();
        let item = manager
            .add_with_checksums(&fixture.url, None, expected.clone())
            .await
            .unwrap();
        let done = terminal(&manager, item.id).await;
        assert_eq!(done.status, Status::Completed, "{:?}", done.error);
        assert_eq!(done.checksums.actual_sha256, expected.sha256);
        assert_eq!(done.checksums.actual_md5, expected.md5);
        assert!(done.checksums.verified_at.is_some());
        manager.shutdown().await.unwrap();
        assert!(dir.path().join(&done.filename).exists());
        assert!(!dir.path().join(format!(".vise/{}.part", item.id)).exists());
        let mut kinds = Vec::new();
        while let Ok(event) = events.try_recv() {
            kinds.push(event.kind);
        }
        let verifying = kinds
            .iter()
            .position(|kind| {
                matches!(
                    kind,
                    vise::events::EventKind::StateChanged {
                        to: Status::Verifying,
                        ..
                    }
                )
            })
            .unwrap();
        let verified = kinds
            .iter()
            .position(|kind| *kind == vise::events::EventKind::Verified)
            .unwrap();
        let completed = kinds
            .iter()
            .position(|kind| *kind == vise::events::EventKind::Completed)
            .unwrap();
        assert!(verifying < verified && verified < completed);
        drop(manager);
        let restored = manager_for_checksum(&dir).await;
        let saved = restored.get(item.id).await.unwrap();
        assert_eq!(saved.checksums, done.checksums);
        restored.shutdown().await.unwrap();
        fixture.close().await;
    }
}
async fn manager_for_checksum(dir: &tempfile::TempDir) -> Manager {
    manager(dir, 4).await
}

#[tokio::test]
async fn mismatch_retains_staging_and_survives_restart_then_retry_redownloads_every_chunk() {
    let fixture = Fixture::start(Mode::Ranges).await;
    fixture.state.version.store(1, Ordering::SeqCst);
    let dir = tempfile::tempdir().unwrap();
    let original = manager(&dir, 4).await;
    let expected = expected_hashes(&fixture.state.data);
    let item = original
        .add_with_checksums(&fixture.url, None, expected.clone())
        .await
        .unwrap();
    let bad = terminal(&original, item.id).await;
    assert_eq!(bad.status, Status::Corrupt);
    let actual = expected_hashes(
        &fixture
            .state
            .data
            .iter()
            .map(|b| b.wrapping_add(1))
            .collect::<Vec<_>>(),
    );
    for value in [
        expected.sha256.as_ref().unwrap(),
        expected.md5.as_ref().unwrap(),
        actual.sha256.as_ref().unwrap(),
        actual.md5.as_ref().unwrap(),
    ] {
        assert!(bad.error.as_ref().unwrap().contains(value));
    }
    assert!(bad.checksums.verified_at.is_none());
    assert!(!dir.path().join(&bad.filename).exists());
    assert!(dir.path().join(format!(".vise/{}.part", item.id)).exists());
    original.shutdown().await.unwrap();
    drop(original);
    let restored = manager(&dir, 4).await;
    assert_eq!(restored.get(item.id).await.unwrap().status, Status::Corrupt);
    assert!(restored.resume(item.id).await.is_err());
    restored.resume_all().await.unwrap();
    assert_eq!(restored.get(item.id).await.unwrap().status, Status::Corrupt);
    fixture.state.version.store(0, Ordering::SeqCst);
    let before = fixture.state.ranges.lock().unwrap().len();
    restored.retry(item.id).await.unwrap();
    let done = terminal(&restored, item.id).await;
    assert_eq!(done.status, Status::Completed, "{:?}", done.error);
    let ranges = fixture.state.ranges.lock().unwrap()[before..].to_vec();
    assert_eq!(
        ranges
            .iter()
            .map(|(start, end)| end - start + 1)
            .sum::<usize>(),
        fixture.state.data.len()
    );
    assert_eq!(done.checksums.expected(), expected);
    assert_eq!(
        tokio::fs::read(dir.path().join(&done.filename))
            .await
            .unwrap(),
        fixture.state.data
    );
    restored.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn metadata_digests_verify_whole_representations_and_ignore_partial_message_digests() {
    for mode in [
        Mode::MetadataRepr,
        Mode::MetadataContent,
        Mode::PartialDigest,
    ] {
        let fixture = Fixture::start(mode).await;
        let dir = tempfile::tempdir().unwrap();
        let manager = manager(&dir, 2).await;
        let item = manager.add(&fixture.url, None).await.unwrap();
        let done = terminal(&manager, item.id).await;
        assert_eq!(done.status, Status::Completed, "{:?}", done.error);
        if matches!(mode, Mode::PartialDigest) {
            assert!(done.checksums.expected().is_empty());
        } else {
            assert_eq!(
                done.checksums.expected(),
                expected_hashes(&fixture.state.data)
            );
            assert!(done.checksums.verified_at.is_some());
        }
        manager.shutdown().await.unwrap();
        fixture.close().await;
    }
}

#[tokio::test]
async fn explicit_hashes_override_metadata_and_md5_mismatch_alone_blocks_publication() {
    for mode in [Mode::MetadataRepr, Mode::Ranges] {
        let fixture = Fixture::start(mode).await;
        let dir = tempfile::tempdir().unwrap();
        let manager = manager(&dir, 2).await;
        let sha = if matches!(mode, Mode::MetadataRepr) {
            Some("0".repeat(64))
        } else {
            None
        };
        let md5 = if matches!(mode, Mode::Ranges) {
            Some("0".repeat(32))
        } else {
            None
        };
        let item = manager
            .add_with_checksums(
                &fixture.url,
                None,
                ExpectedChecksums::new(sha.as_deref(), md5.as_deref()).unwrap(),
            )
            .await
            .unwrap();
        let done = terminal(&manager, item.id).await;
        assert_eq!(done.status, Status::Corrupt);
        assert!(!dir.path().join(&done.filename).exists());
        if let Some(sha) = sha {
            assert_eq!(done.checksums.expected_sha256, Some(sha));
        }
        if let Some(md5) = md5 {
            assert_eq!(done.checksums.expected_md5, Some(md5));
        }
        manager.shutdown().await.unwrap();
        fixture.close().await;
    }
}

#[tokio::test]
async fn cli_checksum_flags_accept_uppercase_and_mismatches_exit_nonzero() {
    for mismatch in [false, true] {
        let fixture = Fixture::start(Mode::Ranges).await;
        let dir = tempfile::tempdir().unwrap();
        let expected = expected_hashes(&fixture.state.data);
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_vise"));
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
async fn interrupted_publication_rechecks_hashes_before_recovering_or_retracting_the_target() {
    for tamper in [false, true] {
        let fixture = Fixture::start(Mode::Ranges).await;
        let dir = tempfile::tempdir().unwrap();
        let original = manager(&dir, 2).await;
        let item = original
            .add_with_checksums(&fixture.url, None, expected_hashes(&fixture.state.data))
            .await
            .unwrap();
        assert_eq!(terminal(&original, item.id).await.status, Status::Completed);
        original.shutdown().await.unwrap();
        drop(original);
        let part = dir.path().join(format!(".vise/{}.part", item.id));
        let target = dir.path().join(&item.filename);
        tokio::fs::hard_link(&target, &part).await.unwrap();
        let connection =
            rusqlite::Connection::open(dir.path().join(".vise/state.sqlite3")).unwrap();
        connection
            .execute(
                "UPDATE downloads SET status='verifying' WHERE id=?1",
                [item.id.to_string()],
            )
            .unwrap();
        drop(connection);
        if tamper {
            use tokio::io::AsyncWriteExt;
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .open(&part)
                .await
                .unwrap();
            file.write_all(b"corrupt bytes").await.unwrap();
            file.sync_all().await.unwrap();
        }
        let requests = fixture.state.ranges.lock().unwrap().len();
        let restored = manager(&dir, 2).await;
        let saved = restored.get(item.id).await.unwrap();
        assert_eq!(
            saved.status,
            if tamper {
                Status::Corrupt
            } else {
                Status::Completed
            }
        );
        assert_eq!(part.exists(), tamper);
        assert_eq!(target.exists(), !tamper);
        assert_eq!(requests, fixture.state.ranges.lock().unwrap().len());
        assert_eq!(saved.checksums.verified_at.is_some(), !tamper);
        restored.shutdown().await.unwrap();
        fixture.close().await;
    }
}

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
    let connection = rusqlite::Connection::open(dir.path().join(".vise/state.sqlite3")).unwrap();
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
        retries: 0,
        ..Config::default()
    })
    .await
    .unwrap();
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
    assert!(fixture.state.ranges.lock().unwrap().is_empty());
    let public = serde_json::to_string(&failed).unwrap();
    assert!(!public.contains("private-user"));
    assert!(!public.contains("private-password"));
    assert!(!dir.path().join(&failed.filename).exists());
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

#[tokio::test]
async fn cli_proxy_flag_routes_the_entire_batch_and_debug_logs_hide_credentials() {
    use proxy_support::{Kind, TestProxy};
    let fixture = Fixture::start(Mode::Ranges).await;
    let proxy = TestProxy::start(Kind::Http, true).await;
    let dir = tempfile::tempdir().unwrap();
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_vise"));
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
