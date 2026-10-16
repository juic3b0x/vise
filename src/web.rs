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
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    engine::Manager, error::Error, events::EventKind, model::Download,
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
        .route("/api/config", get(configuration).post(set_configuration))
        .route("/api/downloads", get(list).post(add))
        .route("/api/downloads/{id}", get(detail).delete(delete))
        .route("/api/downloads/{id}/pause", post(pause))
        .route("/api/downloads/{id}/resume", post(resume))
        .route("/api/downloads/{id}/retry", post(resume))
        .route("/ws", get(websocket))
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
    response.headers_mut().insert("content-security-policy", "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'".parse().expect("static header"));
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
}
async fn add(
    State(state): State<AppState>,
    Json(input): Json<AddRequest>,
) -> Result<(StatusCode, Json<Download>), ApiError> {
    Ok((
        StatusCode::CREATED,
        Json(
            state
                .manager
                .add(&input.url, input.filename.as_deref())
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
        .on_upgrade(move |socket| stream(socket, state))
        .into_response()
}
async fn stream(mut socket: WebSocket, state: AppState) {
    let mut events = state.manager.subscribe();
    let mut interval = tokio::time::interval(Duration::from_millis(500));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let Ok(json) = serde_json::to_string(&state.manager.list().await) else {
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
