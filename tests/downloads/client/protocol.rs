use super::fixture::{SECRET, run};
use axum::{
    Router,
    body::Body,
    http::{HeaderMap, StatusCode, header},
    response::Response,
    routing::{get, post},
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[tokio::test]
async fn redirects_are_not_followed_and_mutation_errors_never_echo_server_secrets() {
    let requests = Arc::new(AtomicUsize::new(0));
    let worker = requests.clone();
    let redirects = Arc::new(AtomicUsize::new(0));
    let redirected = redirects.clone();
    let router = Router::new()
        .route(
            "/api/downloads",
            post(move |headers: HeaderMap| {
                let requests = worker.clone();
                async move {
                    requests.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(
                        headers.get(header::AUTHORIZATION).unwrap(),
                        &format!("Bearer {SECRET}")
                    );
                    Response::builder()
                        .status(StatusCode::TEMPORARY_REDIRECT)
                        .header(header::LOCATION, "/stolen")
                        .body(Body::from(SECRET))
                        .unwrap()
                }
            }),
        )
        .route(
            "/stolen",
            post(move || {
                redirected.fetch_add(1, Ordering::SeqCst);
                async { StatusCode::UNAUTHORIZED }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let root = tempfile::tempdir().unwrap();
    let mut command = crate::fixture::command(root.path());
    command
        .args([
            "remote",
            "--endpoint",
            &endpoint,
            "add",
            "https://example.org/file",
        ])
        .env("VISE_API_TOKEN", SECRET);
    let output = run(command).await;
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    assert_eq!(redirects.load(Ordering::SeqCst), 0);
    assert!(!String::from_utf8_lossy(&output.stderr).contains(SECRET));
    assert!(output.stdout.is_empty());
    assert!(!root.path().join("test-home/.vise").exists());
    server.abort();
}

#[tokio::test]
async fn a_lost_post_response_is_not_automatically_replayed() {
    use tokio::io::AsyncReadExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let expected = b"POST /api/downloads ";
        let mut bytes = vec![0u8; expected.len()];
        stream.read_exact(&mut bytes).await.unwrap();
        assert_eq!(bytes, expected);
        drop(stream);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(500), listener.accept())
                .await
                .is_err(),
            "client replayed a possibly admitted POST"
        );
    });
    let root = tempfile::tempdir().unwrap();
    let mut command = crate::fixture::command(root.path());
    command
        .args([
            "remote",
            "--endpoint",
            &endpoint,
            "add",
            "https://example.org/file",
        ])
        .env("VISE_API_TOKEN", SECRET);
    let output = run(command).await;
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("may already have been accepted"));
    server.await.unwrap();
}

#[tokio::test]
async fn human_output_escapes_untrusted_snapshot_text() {
    let router = Router::new().route(
        "/api/downloads",
        get(|| async {
            axum::Json(serde_json::json!([{
                "id": "ba8d340a-6d57-4991-9f94-398e585ead15",
                "url": "https://example.org/file", "filename": "file\u{1b}[31m\nforged",
                "status": "failed", "total_bytes": null, "downloaded_bytes": 0,
                "bytes_per_second": 0.0, "eta_seconds": null,
                "error": "failure\r\nforged\u{1b}[2J", "created_at": 0
            }]))
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let root = tempfile::tempdir().unwrap();
    let mut command = crate::fixture::command(root.path());
    command
        .args(["remote", "--endpoint", &endpoint, "list"])
        .env("VISE_API_TOKEN", SECRET);
    let output = run(command).await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(!text.contains('\u{1b}') && !text.contains('\r'));
    assert_eq!(text.lines().count(), 1);
    assert!(text.contains("forged"));
    server.abort();
}

#[tokio::test]
async fn malformed_and_oversized_responses_fail_without_copying_them_into_diagnostics() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new()
        .route("/api/downloads", get(|| async { SECRET }))
        .route(
            "/api/config",
            get(|| async { vec![b'x'; 32 * 1024 * 1024 + 1] }),
        );
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let root = tempfile::tempdir().unwrap();
    for operation in ["list", "limit-rate"] {
        let mut command = crate::fixture::command(root.path());
        command
            .args(["remote", "--endpoint", &endpoint, operation, "--json"])
            .env("VISE_API_TOKEN", SECRET);
        let output = run(command).await;
        assert_eq!(output.status.code(), Some(1));
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(!error.contains(SECRET));
        assert!(error.contains("incompatible JSON") || error.contains("32 MiB"));
        assert!(output.stdout.is_empty());
    }
    server.abort();
}
