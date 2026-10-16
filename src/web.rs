use std::{net::SocketAddr, time::Duration};

use axum::{
    Json, Router,
    extract::{
        DefaultBodyLimit, Path, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use futures_util::SinkExt;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    checksum::ExpectedChecksums,
    engine::{Manager, TransferOptions},
    error::Error,
    events::EventKind,
    model::Download,
    rate_limiter::RateConfiguration,
};

#[derive(Clone)]
struct AppState {
    manager: Manager,
    authority: String,
    stop: CancellationToken,
}

/// Bind only to loopback; remote access can use an authenticated SSH tunnel.
pub async fn run(
    manager: Manager,
    bind: SocketAddr,
    stop: CancellationToken,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        bind.ip().is_loopback(),
        "WebUI must bind to a loopback address"
    );
    let listener = TcpListener::bind(bind).await?;
    let address = listener.local_addr()?;
    let state = AppState {
        manager: manager.clone(),
        authority: address.to_string(),
        stop: stop.clone(),
    };
    manager.server_started(format!("http://{address}"));
    axum::serve(listener, router(state))
        .with_graceful_shutdown(stop.cancelled_owned())
        .await?;
    Ok(())
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/assets/dashboard.css", get(stylesheet))
        .route("/assets/dashboard.js", get(javascript))
        .route("/api/telemetry", get(telemetry))
        .route("/api/config", get(configuration).post(set_configuration))
        .route("/api/downloads", get(list).post(add))
        .route("/api/downloads/{id}", get(detail).delete(delete))
        .route("/api/downloads/{id}/pause", post(pause))
        .route("/api/downloads/{id}/resume", post(resume))
        .route("/api/downloads/{id}/retry", post(retry))
        .route("/ws", get(websocket))
        .route("/ws/telemetry", get(telemetry_websocket))
        .layer(DefaultBodyLimit::max(16 * 1024))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            local_requests,
        ))
        .with_state(state)
}

// Host validation blocks DNS rebinding. Origin checks and JSON-only mutations
// prevent other websites from controlling a user's unauthenticated local server.
async fn local_requests(
    State(state): State<AppState>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    let host = request
        .headers()
        .get(header::HOST)
        .and_then(|v| v.to_str().ok());
    if host != Some(state.authority.as_str()) {
        return rejection("use the exact loopback URL printed by Vise");
    }
    if let Some(origin) = request.headers().get(header::ORIGIN) {
        let expected = format!("http://{}", state.authority);
        if origin.to_str().ok() != Some(expected.as_str()) {
            return rejection("cross-origin requests are forbidden");
        }
    }
    if request
        .headers()
        .get("sec-fetch-site")
        .is_some_and(|v| v == "cross-site")
    {
        return rejection("cross-site requests are forbidden");
    }
    if matches!(
        *request.method(),
        axum::http::Method::POST | axum::http::Method::DELETE
    ) {
        let content_type = request
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if content_type.split(';').next().unwrap_or("").trim() != "application/json" {
            return (
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                Json(serde_json::json!({"error":"use Content-Type: application/json"})),
            )
                .into_response();
        }
    }
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        "no-store".parse().expect("static header"),
    );
    response.headers_mut().insert(
        "x-content-type-options",
        "nosniff".parse().expect("static header"),
    );
    response.headers_mut().insert("content-security-policy", "default-src 'none'; script-src 'self'; style-src 'self'; style-src-attr 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'; form-action 'self'".parse().expect("static header"));
    response
}
fn rejection(message: &str) -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(serde_json::json!({"error":message})),
    )
        .into_response()
}

async fn index() -> Html<&'static str> {
    Html(include_str!("dashboard.html"))
}
async fn stylesheet() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        include_str!("dashboard.css"),
    )
}
async fn javascript() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        include_str!("dashboard.js"),
    )
}

#[derive(Serialize)]
struct TelemetrySnapshot {
    timestamp: i64,
    downloads: Vec<Download>,
    configuration: RateConfiguration,
}
impl TelemetrySnapshot {
    async fn capture(manager: &Manager) -> Self {
        Self {
            timestamp: crate::model::timestamp_millis(),
            downloads: manager.list().await,
            configuration: manager.rate_configuration(),
        }
    }
}
async fn telemetry(State(state): State<AppState>) -> Json<TelemetrySnapshot> {
    Json(TelemetrySnapshot::capture(&state.manager).await)
}
async fn list(State(state): State<AppState>) -> Json<Vec<Download>> {
    Json(state.manager.list().await)
}
async fn configuration(State(state): State<AppState>) -> Json<RateConfiguration> {
    Json(state.manager.rate_configuration())
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigurationRequest {
    limit_rate: String,
}
async fn set_configuration(
    State(state): State<AppState>,
    Json(input): Json<ConfigurationRequest>,
) -> Result<Json<RateConfiguration>, ApiError> {
    let rate = input
        .limit_rate
        .parse()
        .inspect_err(|error| rejected_action(&state.manager, "limit-rate", error))?;
    Ok(Json(state.manager.set_rate_limit(rate).await.inspect_err(
        |error| rejected_action(&state.manager, "limit-rate", error),
    )?))
}
async fn detail(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Download>, ApiError> {
    Ok(Json(state.manager.get(id).await?))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AddRequest {
    url: String,
    filename: Option<String>,
    expected_sha256: Option<String>,
    expected_md5: Option<String>,
    proxy_url: Option<String>,
}
async fn add(
    State(state): State<AppState>,
    Json(input): Json<AddRequest>,
) -> Result<(StatusCode, Json<Download>), ApiError> {
    let options = TransferOptions {
        expected: ExpectedChecksums::new(
            input.expected_sha256.as_deref(),
            input.expected_md5.as_deref(),
        )
        .inspect_err(|error| rejected_action(&state.manager, "add", error))?,
        proxy_url: input
            .proxy_url
            .as_deref()
            .map(str::parse)
            .transpose()
            .inspect_err(|error| rejected_action(&state.manager, "add", error))?,
    };
    Ok((
        StatusCode::CREATED,
        Json(
            state
                .manager
                .add_with_options(&input.url, input.filename.as_deref(), options)
                .await
                .inspect_err(|error| rejected_action(&state.manager, "add", error))?,
        ),
    ))
}
async fn pause(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Download>, ApiError> {
    state
        .manager
        .pause(id)
        .await
        .inspect_err(|error| rejected_action(&state.manager, "pause", error))?;
    Ok(Json(state.manager.get(id).await?))
}
async fn resume(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Download>, ApiError> {
    state
        .manager
        .resume(id)
        .await
        .inspect_err(|error| rejected_action(&state.manager, "resume", error))?;
    Ok(Json(state.manager.get(id).await?))
}
async fn delete(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    state
        .manager
        .delete(id)
        .await
        .inspect_err(|error| rejected_action(&state.manager, "delete", error))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn retry(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Download>, ApiError> {
    state
        .manager
        .retry(id)
        .await
        .inspect_err(|error| rejected_action(&state.manager, "retry", error))?;
    Ok(Json(state.manager.get(id).await?))
}

fn rejected_action(manager: &Manager, action: &str, error: &Error) {
    manager.publish(
        EventKind::Warning {
            message: format!("{action} rejected: {error}"),
        },
        None,
    );
}

async fn websocket(State(state): State<AppState>, upgrade: WebSocketUpgrade) -> Response {
    upgrade
        .max_message_size(16 * 1024)
        .max_frame_size(16 * 1024)
        .on_upgrade(move |socket| stream(socket, state, false))
        .into_response()
}
async fn telemetry_websocket(State(state): State<AppState>, upgrade: WebSocketUpgrade) -> Response {
    upgrade
        .max_message_size(16 * 1024)
        .max_frame_size(16 * 1024)
        .on_upgrade(move |socket| stream(socket, state, true))
        .into_response()
}
async fn stream(mut socket: WebSocket, state: AppState, with_configuration: bool) {
    let mut events = state.manager.subscribe();
    let mut interval = tokio::time::interval(Duration::from_millis(500));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let payload = if with_configuration {
            serde_json::to_string(&TelemetrySnapshot::capture(&state.manager).await)
        } else {
            serde_json::to_string(&state.manager.list().await)
        };
        let Ok(json) = payload else {
            break;
        };
        let sent = tokio::select! {
            _ = state.stop.cancelled() => break,
            sent = tokio::time::timeout(Duration::from_secs(5), socket.send(Message::Text(json.into()))) => sent,
        };
        if !matches!(sent, Ok(Ok(()))) {
            break;
        }
        tokio::select! {
            _ = state.stop.cancelled() => break,
            _ = interval.tick() => (),
            result = events.changed() => { if result.is_err() { break; } },
            message = socket.recv() => {
                match message {
                    None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break,
                    Some(Ok(Message::Ping(data))) => {
                        if !matches!(tokio::time::timeout(Duration::from_secs(5), socket.send(Message::Pong(data))).await, Ok(Ok(()))) { break; }
                    }
                    _ => (),
                }
            }
        }
    }
    let _ = tokio::time::timeout(Duration::from_secs(1), socket.close()).await;
}

struct ApiError(Error);
impl From<Error> for ApiError {
    fn from(error: Error) -> Self {
        Self(error)
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self.0 {
            Error::Invalid(_) => StatusCode::BAD_REQUEST,
            Error::NotFound(_) => StatusCode::NOT_FOUND,
            Error::Conflict(_) => StatusCode::CONFLICT,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (
            status,
            Json(serde_json::json!({"error":self.0.to_string()})),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;
    #[tokio::test]
    async fn embedded_assets_have_explicit_types_and_a_script_policy() {
        let dir = tempfile::tempdir().unwrap();
        let manager = Manager::open(crate::engine::Config {
            output_dir: dir.path().into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let app = router(AppState {
            manager: manager.clone(),
            authority: "127.0.0.1:8080".into(),
            stop: CancellationToken::new(),
        });
        for (path, content_type) in [
            ("/", "text/html; charset=utf-8"),
            ("/assets/dashboard.css", "text/css; charset=utf-8"),
            ("/assets/dashboard.js", "text/javascript; charset=utf-8"),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(path)
                        .header("host", "127.0.0.1:8080")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.headers()[header::CONTENT_TYPE], content_type);
            assert_eq!(response.headers()["x-content-type-options"], "nosniff");
            let policy = response.headers()["content-security-policy"]
                .to_str()
                .unwrap();
            assert!(policy.contains("script-src 'self';"));
            assert!(!policy.contains("script-src 'self' 'unsafe-inline'"));
            assert!(
                !axum::body::to_bytes(response.into_body(), 64 * 1024)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/assets/missing.js")
                    .header("host", "127.0.0.1:8080")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        manager.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn rejects_rebinding_and_cross_origin_mutations() {
        let dir = tempfile::tempdir().unwrap();
        let manager = Manager::open(crate::engine::Config {
            output_dir: dir.path().into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let app = router(AppState {
            manager,
            authority: "127.0.0.1:8080".into(),
            stop: CancellationToken::new(),
        });
        for (host, origin) in [
            ("evil.example:8080", "http://evil.example:8080"),
            ("127.0.0.1:8080", "https://evil.example"),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri("/api/downloads")
                        .method("POST")
                        .header("host", host)
                        .header("origin", origin)
                        .header("content-type", "application/json")
                        .body(Body::from(r#"{"url":"http://example.com/file"}"#))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
        }
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/downloads")
                    .method("POST")
                    .header("host", "127.0.0.1:8080")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/")
                    .header("host", "127.0.0.1:8080")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
