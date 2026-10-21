use super::fixture::{self, Fixture, Mode, ServerState, progress, terminal};
use axum::{
    Router,
    body::Body,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::Response,
    routing::get,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use vise::{
    engine::{Config, Manager, TransferOptions},
    model::Status,
};

struct Authentication {
    origin: Arc<ServerState>,
    expected: Mutex<String>,
    requests: Mutex<Vec<(String, String)>>,
    rotate: AtomicBool,
}

struct ProtectedFixture {
    origin: Fixture,
    url: String,
    auth: Arc<Authentication>,
    stop: CancellationToken,
    server: tokio::task::JoinHandle<()>,
}

impl ProtectedFixture {
    async fn start(mode: Mode) -> Self {
        let origin = Fixture::start(mode).await;
        let auth = Arc::new(Authentication {
            origin: origin.state.clone(),
            expected: Mutex::new("initial-secret".into()),
            requests: Mutex::new(Vec::new()),
            rotate: AtomicBool::new(false),
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/private/file.bin", listener.local_addr().unwrap());
        let router = Router::new()
            .route("/private/file.bin", get(authenticated))
            .with_state(auth.clone());
        let stop = CancellationToken::new();
        let token = stop.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(token.cancelled_owned())
                .await
                .unwrap();
        });
        Self {
            origin,
            url,
            auth,
            stop,
            server,
        }
    }

    fn cookies(&self, root: &std::path::Path, name: &str, value: &str) -> std::path::PathBuf {
        let path = root.join(name);
        std::fs::write(&path, format!("# Netscape HTTP Cookie File\n#HttpOnly_127.0.0.1\tFALSE\t/private\tFALSE\t0\tsession\t{value}\n")).unwrap();
        path
    }

    async fn close(self) {
        self.stop.cancel();
        self.server.await.unwrap();
        self.origin.close().await;
    }
}

async fn authenticated(State(auth): State<Arc<Authentication>>, headers: HeaderMap) -> Response {
    let cookie = headers
        .get(header::COOKIE)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    let range = headers
        .get(header::RANGE)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    auth.requests
        .lock()
        .unwrap()
        .push((range.into(), cookie.into()));
    if cookie != format!("session={}", auth.expected.lock().unwrap()) {
        return Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .body(Body::empty())
            .unwrap();
    }
    let rotated = range == "bytes=0-0" && auth.rotate.swap(false, Ordering::SeqCst);
    if rotated {
        *auth.expected.lock().unwrap() = "rotated-secret".into();
    }
    let mut response = fixture::serve(State(auth.origin.clone()), headers).await;
    if rotated {
        response.headers_mut().insert(
            header::SET_COOKIE,
            "session=rotated-secret; Path=/private; HttpOnly"
                .parse()
                .unwrap(),
        );
    }
    response
}

#[tokio::test]
async fn cli_cookie_file_authenticates_probes_parallel_chunks_and_retries() {
    let server = ProtectedFixture::start(Mode::Truncated).await;
    let root = tempfile::tempdir().unwrap();
    let path = server.cookies(root.path(), "cookies.txt", "initial-secret");
    let output = root.path().join("output");
    let mut command = fixture::command(root.path());
    command
        .args(["--batch", "-c", "4", "--cookies"])
        .arg(&path)
        .arg("-o")
        .arg(&output)
        .arg(&server.url)
        .env("RUST_LOG", "vise=debug");
    let result = tokio::task::spawn_blocking(move || command.output().unwrap())
        .await
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(
        tokio::fs::read(output.join("file.bin")).await.unwrap(),
        server.origin.state.data
    );
    assert!(!String::from_utf8_lossy(&result.stderr).contains("initial-secret"));
    let requests = server.auth.requests.lock().unwrap().clone();
    assert!(requests.len() > 5, "a truncated chunk must be retried");
    assert!(
        requests
            .iter()
            .all(|(_, cookie)| cookie == "session=initial-secret")
    );
    assert!(server.origin.state.peak.load(Ordering::SeqCst) > 1);
    server.close().await;
}

#[tokio::test]
async fn saved_cookie_paths_resume_offsets_and_reload_refreshed_sessions() {
    let server = ProtectedFixture::start(Mode::SlowRanges).await;
    let root = tempfile::tempdir().unwrap();
    let path = server.cookies(root.path(), "cookies.txt", "initial-secret");
    let output = root.path().join("output");
    let manager = Manager::open(Config {
        output_dir: output.clone(),
        cookies_file: Some(path.clone()),
        connections_per_download: 1,
        ..Config::default()
    })
    .await
    .unwrap();
    let item = manager.add(&server.url, None).await.unwrap();
    progress(&manager, item.id).await;
    manager.pause(item.id).await.unwrap();
    let saved = manager.get(item.id).await.unwrap().downloaded_bytes;
    assert!(saved > 0 && saved < server.origin.state.data.len() as u64);
    manager.shutdown().await.unwrap();
    drop(manager);
    let database = rusqlite::Connection::open(output.join(".vise/state.sqlite3")).unwrap();
    let stored: String = database
        .query_row(
            "SELECT cookies_file FROM downloads WHERE id=?1",
            [item.id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        std::path::PathBuf::from(stored),
        path.canonicalize().unwrap()
    );
    // No cookie values are written to SQLite, including the WAL.
    assert!(
        !database
            .query_row(
                "SELECT coalesce(error,'') FROM downloads WHERE id=?1",
                [item.id.to_string()],
                |row| row.get::<_, String>(0)
            )
            .unwrap()
            .contains("secret")
    );
    drop(database);
    *server.auth.expected.lock().unwrap() = "renewed-secret".into();
    server.cookies(root.path(), "cookies.txt", "renewed-secret");
    server.origin.state.ranges.lock().unwrap().clear();
    // An unrelated process default must not replace saved task authentication.
    let other = server.cookies(root.path(), "other.txt", "wrong-secret");
    let manager = Manager::open(Config {
        output_dir: output.clone(),
        cookies_file: Some(other),
        connections_per_download: 1,
        ..Config::default()
    })
    .await
    .unwrap();
    manager.resume(item.id).await.unwrap();
    let done = terminal(&manager, item.id).await;
    assert_eq!(done.status, Status::Completed, "{:?}", done.error);
    assert_eq!(
        server
            .origin
            .state
            .ranges
            .lock()
            .unwrap()
            .first()
            .unwrap()
            .0,
        saved as usize
    );
    assert_eq!(
        tokio::fs::read(output.join("file.bin")).await.unwrap(),
        server.origin.state.data
    );
    manager.shutdown().await.unwrap();
    server.close().await;
}

#[tokio::test]
async fn missing_or_malformed_saved_cookies_fail_without_unauthenticated_requests() {
    let server = ProtectedFixture::start(Mode::SlowRanges).await;
    let root = tempfile::tempdir().unwrap();
    let path = server.cookies(root.path(), "cookies.txt", "initial-secret");
    let output = root.path().join("output");
    let manager = Manager::open(Config {
        output_dir: output.clone(),
        cookies_file: Some(path.clone()),
        ..Config::default()
    })
    .await
    .unwrap();
    let item = manager.add(&server.url, None).await.unwrap();
    progress(&manager, item.id).await;
    manager.shutdown().await.unwrap();
    drop(manager);
    tokio::fs::remove_file(&path).await.unwrap();
    let manager = Manager::open(Config {
        output_dir: output,
        ..Config::default()
    })
    .await
    .unwrap();
    let requests = server.auth.requests.lock().unwrap().len();
    manager.resume(item.id).await.unwrap();
    let failed = terminal(&manager, item.id).await;
    assert_eq!(failed.status, Status::Failed);
    assert!(failed.error.unwrap().contains("cannot read cookie file"));
    tokio::fs::write(&path, "# header\nsecret malformed\n")
        .await
        .unwrap();
    manager.resume(item.id).await.unwrap();
    let failed = terminal(&manager, item.id).await;
    assert_eq!(failed.status, Status::Failed);
    let error = failed.error.unwrap();
    assert!(error.contains("line 2") && !error.contains("secret"));
    assert_eq!(server.auth.requests.lock().unwrap().len(), requests);
    server.cookies(root.path(), "cookies.txt", "initial-secret");
    manager.resume(item.id).await.unwrap();
    assert_eq!(terminal(&manager, item.id).await.status, Status::Completed);
    manager.shutdown().await.unwrap();
    server.close().await;
}

#[tokio::test]
async fn jars_accept_response_cookies_without_sharing_sessions_between_tasks() {
    let server = ProtectedFixture::start(Mode::Ranges).await;
    let root = tempfile::tempdir().unwrap();
    let path = server.cookies(root.path(), "cookies.txt", "initial-secret");
    server.auth.rotate.store(true, Ordering::SeqCst);
    let manager = Manager::open(Config {
        output_dir: root.path().join("output"),
        ..Config::default()
    })
    .await
    .unwrap();
    let item = manager
        .add_with_options(
            &server.url,
            None,
            TransferOptions {
                cookies_file: Some(path),
                ..TransferOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(terminal(&manager, item.id).await.status, Status::Completed);
    let other = manager
        .add(&server.url, Some("unauthenticated.bin"))
        .await
        .unwrap();
    let failed = terminal(&manager, other.id).await;
    assert_eq!(failed.status, Status::Failed);
    assert!(failed.error.unwrap().contains("401"));
    assert!(
        server
            .auth
            .requests
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .1
            .is_empty()
    );
    manager.shutdown().await.unwrap();
    server.close().await;
}

#[tokio::test]
async fn malformed_cli_files_fail_before_creating_the_download_journal() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("cookies.txt");
    std::fs::write(&path, "# header\nsecret malformed\n").unwrap();
    let output = root.path().join("output");
    let mut command = fixture::command(root.path());
    command
        .arg("--cookies")
        .arg(&path)
        .arg("-o")
        .arg(&output)
        .arg("http://127.0.0.1:1/file.bin")
        .arg("--batch");
    let result = tokio::task::spawn_blocking(move || command.output().unwrap())
        .await
        .unwrap();
    assert!(!result.status.success());
    let error = String::from_utf8_lossy(&result.stderr);
    assert!(error.contains("cookie file") && error.contains("line 2") && !error.contains("secret"));
    assert!(!output.exists());
}

#[tokio::test]
async fn redirects_observe_cookie_scope_and_do_not_leak_to_another_host() {
    let root = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let captured = Arc::new(Mutex::new(None::<String>));
    let received = captured.clone();
    let router = Router::new()
        .route(
            "/file.bin",
            get(move || async move {
                Response::builder()
                    .status(StatusCode::FOUND)
                    .header(
                        header::LOCATION,
                        format!("http://localhost:{}/capture", address.port()),
                    )
                    .body(Body::empty())
                    .unwrap()
            }),
        )
        .route(
            "/capture",
            get(move |headers: HeaderMap| {
                let received = received.clone();
                async move {
                    *received.lock().unwrap() = Some(
                        headers
                            .get(header::COOKIE)
                            .and_then(|h| h.to_str().ok())
                            .unwrap_or("")
                            .into(),
                    );
                    Response::builder()
                        .status(StatusCode::UNAUTHORIZED)
                        .body(Body::empty())
                        .unwrap()
                }
            }),
        );
    let stop = CancellationToken::new();
    let token = stop.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(token.cancelled_owned())
            .await
            .unwrap();
    });
    let path = root.path().join("cookies.txt");
    std::fs::write(
        &path,
        "127.0.0.1\tFALSE\t/\tFALSE\t0\tsession\tprivate-secret\n",
    )
    .unwrap();
    let manager = Manager::open(Config {
        output_dir: root.path().join("output"),
        cookies_file: Some(path),
        ..Config::default()
    })
    .await
    .unwrap();
    let item = manager
        .add(&format!("http://{address}/file.bin"), None)
        .await
        .unwrap();
    assert_eq!(terminal(&manager, item.id).await.status, Status::Failed);
    assert_eq!(captured.lock().unwrap().as_deref(), Some(""));
    manager.shutdown().await.unwrap();
    stop.cancel();
    server.await.unwrap();
}
