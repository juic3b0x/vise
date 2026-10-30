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
    engine::{Config, Manager},
    model::{Download, Status},
};

/// Child binaries must never create or read the developer's real home config.
pub(super) fn command(root: &std::path::Path) -> std::process::Command {
    let home = root.join("test-home");
    std::fs::create_dir_all(&home).unwrap();
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_vise"));
    command
        .env("HOME", &home)
        .env("USERPROFILE", &home)
        .env_remove("VISE_API_TOKEN");
    command
}

#[derive(Clone, Copy)]
pub(super) enum Mode {
    Ranges,
    Heterogeneous,
    SplitValidator,
    Split429,
    Split503,
    SplitRefused,
    SplitDonorRetry,
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

pub(super) struct Fixture {
    pub(super) url: String,
    pub(super) state: Arc<ServerState>,
    stop: CancellationToken,
    server: tokio::task::JoinHandle<()>,
}
pub(super) struct ServerState {
    pub(super) data: Vec<u8>,
    pub(super) mode: Mode,
    pub(super) fail_once: AtomicBool,
    pub(super) version: AtomicUsize,
    pub(super) ranges: Mutex<Vec<(usize, usize)>>,
    pub(super) if_ranges: Mutex<Vec<Option<String>>>,
    bodies: AtomicUsize,
    pub(super) active: AtomicUsize,
    pub(super) peak: AtomicUsize,
    pub(super) proxy_auth_leaked: AtomicBool,
}
struct Active(Arc<ServerState>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Fixture {
    pub(super) async fn start(mode: Mode) -> Self {
        let state = Arc::new(ServerState {
            data: (0..if matches!(
                mode,
                Mode::Heterogeneous
                    | Mode::SplitValidator
                    | Mode::Split429
                    | Mode::Split503
                    | Mode::SplitRefused
                    | Mode::SplitDonorRetry
            ) {
                32 * 1024 * 1024
            } else {
                8 * 1024 * 1024 + 137
            })
                .map(|i| (i % 251) as u8)
                .collect(),
            mode,
            fail_once: AtomicBool::new(true),
            version: AtomicUsize::new(0),
            ranges: Mutex::new(vec![]),
            if_ranges: Mutex::new(vec![]),
            bodies: AtomicUsize::new(0),
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
    pub(super) async fn close(self) {
        self.stop.cancel();
        self.server.await.unwrap();
    }
}

pub(super) async fn serve(State(state): State<Arc<ServerState>>, headers: HeaderMap) -> Response {
    if headers.contains_key("proxy-authorization") {
        state.proxy_auth_leaked.store(true, Ordering::SeqCst);
    }
    if let Some(response) = special_response(state.mode) {
        return response;
    }
    let Some(range) = transfer_range(&state, &headers) else {
        return Response::builder().status(416).body(Body::empty()).unwrap();
    };
    if matches!(state.mode, Mode::Transient)
        && !range.probe
        && state.fail_once.swap(false, Ordering::SeqCst)
    {
        return Response::builder()
            .status(503)
            .header(header::RETRY_AFTER, "1")
            .body(Body::empty())
            .unwrap();
    }
    if !range.probe {
        state.if_ranges.lock().unwrap().push(
            headers
                .get(header::IF_RANGE)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned),
        );
        let body = state.bodies.fetch_add(1, Ordering::SeqCst);
        if body >= 4 && matches!(state.mode, Mode::SplitValidator) {
            return Response::builder()
                .status(200)
                .header(header::ETAG, "\"version-1\"")
                .body(Body::empty())
                .unwrap();
        }
        if body >= 4
            && matches!(state.mode, Mode::Split429 | Mode::Split503)
            && state.fail_once.swap(false, Ordering::SeqCst)
        {
            return Response::builder()
                .status(if matches!(state.mode, Mode::Split429) {
                    429
                } else {
                    503
                })
                .header(header::RETRY_AFTER, "1")
                .body(Body::empty())
                .unwrap();
        }
    }
    let version = state.version.load(Ordering::SeqCst);
    let builder = response_headers(&state, &range, version);
    if range.probe && range.honors_range {
        return builder.body(Body::from(vec![state.data[0]])).unwrap();
    }
    builder.body(transfer_body(state, range, version)).unwrap()
}

fn special_response(mode: Mode) -> Option<Response> {
    match mode {
        Mode::NotFound => Some(Response::builder().status(404).body(Body::empty()).unwrap()),
        Mode::Empty => Some(
            Response::builder()
                .status(416)
                .header(header::CONTENT_RANGE, "bytes */0")
                .body(Body::empty())
                .unwrap(),
        ),
        _ => None,
    }
}

struct TransferRange {
    start: usize,
    end: usize,
    probe: bool,
    requested: bool,
    honors_range: bool,
}

fn transfer_range(state: &ServerState, headers: &HeaderMap) -> Option<TransferRange> {
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
        return None;
    }
    Some(TransferRange {
        start,
        end,
        probe: range == Some((0, 0)),
        requested: range.is_some(),
        honors_range,
    })
}

fn response_headers(
    state: &ServerState,
    range: &TransferRange,
    version: usize,
) -> axum::http::response::Builder {
    let builder = Response::builder().status(if range.honors_range && range.requested {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    });
    let mut builder = digest_headers(builder, state, range);
    if !matches!(state.mode, Mode::NoValidator | Mode::TruncatedNoValidator) {
        builder = builder.header(header::ETAG, format!("\"version-{version}\""));
    }
    if range.honors_range && range.requested {
        let wrong = usize::from(matches!(state.mode, Mode::BadRange) && !range.probe);
        builder = builder.header(
            header::CONTENT_RANGE,
            format!(
                "bytes {}-{}/{}",
                range.start + wrong,
                range.end,
                state.data.len()
            ),
        );
    }
    if !matches!(state.mode, Mode::UnknownLength) {
        builder = builder.header(header::CONTENT_LENGTH, range.end - range.start + 1);
    }
    builder
}

fn digest_headers(
    mut builder: axum::http::response::Builder,
    state: &ServerState,
    range: &TransferRange,
) -> axum::http::response::Builder {
    if matches!(state.mode, Mode::MetadataRepr | Mode::PartialDigest)
        || (matches!(state.mode, Mode::MetadataContent) && !range.probe)
    {
        use base64::Engine;
        let data = if matches!(state.mode, Mode::PartialDigest) {
            &state.data[range.start..=range.end]
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
    builder
}

type FrameState = (usize, usize, bool, Active);
type Frame = (std::io::Result<Bytes>, FrameState);

fn transfer_body(state: Arc<ServerState>, range: TransferRange, version: usize) -> Body {
    state.ranges.lock().unwrap().push((range.start, range.end));
    let active = state.active.fetch_add(1, Ordering::SeqCst) + 1;
    state.peak.fetch_max(active, Ordering::SeqCst);
    let truncated = matches!(state.mode, Mode::Truncated | Mode::TruncatedNoValidator)
        && state.fail_once.swap(false, Ordering::SeqCst);
    let slow = range.start == state.data.len() * 3 / 4
        && matches!(
            state.mode,
            Mode::Heterogeneous
                | Mode::SplitValidator
                | Mode::Split429
                | Mode::Split503
                | Mode::SplitRefused
                | Mode::SplitDonorRetry
        );
    let refused = range.start > state.data.len() * 3 / 4
        && matches!(state.mode, Mode::SplitRefused)
        && state.fail_once.swap(false, Ordering::SeqCst);
    let truncated = truncated || refused;
    let guard = Active(state);
    let stream = futures_util::stream::unfold((range.start, 0, slow, guard), move |state| {
        next_frame(state, range.end, truncated, version)
    });
    Body::from_stream(stream)
}

async fn next_frame(
    (offset, pieces, slow, guard): FrameState,
    end: usize,
    truncated: bool,
    version: usize,
) -> Option<Frame> {
    if offset > end {
        return None;
    }
    if matches!(guard.0.mode, Mode::Stalled) {
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    let delay = if slow {
        200
    } else if matches!(
        guard.0.mode,
        Mode::Heterogeneous
            | Mode::SplitValidator
            | Mode::Split429
            | Mode::Split503
            | Mode::SplitRefused
            | Mode::SplitDonorRetry
            | Mode::SlowRanges
    ) {
        20
    } else {
        6
    };
    tokio::time::sleep(Duration::from_millis(delay)).await;
    if (truncated && pieces == 6)
        || (slow
            && pieces == 16
            && matches!(guard.0.mode, Mode::SplitDonorRetry)
            && guard.0.fail_once.swap(false, Ordering::SeqCst))
    {
        return Some((
            Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "intentional drop",
            )),
            (end + 1, pieces, slow, guard),
        ));
    }
    let next = (offset + 64 * 1024).min(end + 1);
    let bytes: Vec<_> = guard.0.data[offset..next]
        .iter()
        .map(|b| b.wrapping_add(version as u8))
        .collect();
    Some((Ok(Bytes::from(bytes)), (next, pieces + 1, slow, guard)))
}

pub(super) async fn manager(dir: &tempfile::TempDir, connections: usize) -> Manager {
    Manager::open(Config {
        output_dir: dir.path().into(),
        home_dir: Some(dir.path().join("test-home")),
        concurrent_downloads: 1,
        connections_per_download: connections,
        retries: 2,
        ..Config::default()
    })
    .await
    .unwrap()
}

/// Own the isolated resources shared by ordinary download scenarios, leaving
/// each test responsible for its admissions and scenario-specific assertions.
pub(super) struct DownloadCase {
    pub(super) fixture: Fixture,
    pub(super) manager: Manager,
    directory: tempfile::TempDir,
}

impl DownloadCase {
    pub(super) async fn start(mode: Mode, connections: usize) -> Self {
        let fixture = Fixture::start(mode).await;
        let directory = tempfile::tempdir().unwrap();
        let manager = manager(&directory, connections).await;
        Self {
            fixture,
            manager,
            directory,
        }
    }

    pub(super) async fn add(&self) -> Download {
        self.manager.add(&self.fixture.url, None).await.unwrap()
    }

    pub(super) async fn wait(&self, id: Uuid) -> Download {
        terminal(&self.manager, id).await
    }

    pub(super) async fn assert_complete_bytes(&self, id: Uuid) {
        let item = self.wait(id).await;
        assert_eq!(item.status, Status::Completed, "{:?}", item.error);
        assert_eq!(
            tokio::fs::read(self.directory.path().join(item.filename))
                .await
                .unwrap(),
            self.fixture.state.data
        );
    }

    pub(super) async fn close(self) {
        self.manager.shutdown().await.unwrap();
        self.fixture.close().await;
    }
}

/// Stop a wrapping server before its backing origin so teardown cannot outlive
/// either fixture. Cookie and credential capture fixtures share this ordering.
pub(super) async fn close_fixture_server(
    stop: CancellationToken,
    server: tokio::task::JoinHandle<()>,
    origin: Fixture,
) {
    stop.cancel();
    server.await.unwrap();
    origin.close().await;
}

pub(super) async fn terminal(manager: &Manager, id: Uuid) -> vise::model::Download {
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
pub(super) async fn progress(manager: &Manager, id: Uuid) {
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

pub(super) fn expected_hashes(bytes: &[u8]) -> ExpectedChecksums {
    ExpectedChecksums::new(
        Some(&format!("{:x}", sha2::Sha256::digest(bytes))),
        Some(&format!("{:x}", md5::Md5::digest(bytes))),
    )
    .unwrap()
}

pub(super) async fn manager_for_checksum(dir: &tempfile::TempDir) -> Manager {
    manager(dir, 4).await
}
