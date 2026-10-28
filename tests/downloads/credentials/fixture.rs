use crate::fixture::{self, Fixture, Mode};
use axum::{
    Router,
    body::Body,
    extract::{OriginalUri, State},
    http::{HeaderMap, StatusCode, header},
    response::Response,
    routing::get,
};
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

pub(super) const SECRET: &str = "unique-request-credential-sentinel";
pub(super) struct Capture {
    pub origin: Fixture,
    pub url: String,
    pub requests: Arc<Mutex<Vec<(String, HeaderMap)>>>,
    pub redirect: Arc<Mutex<Option<String>>>,
    stop: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}
#[derive(Clone)]
struct Data {
    origin: Arc<fixture::ServerState>,
    requests: Arc<Mutex<Vec<(String, HeaderMap)>>>,
    redirect: Arc<Mutex<Option<String>>>,
}
impl Capture {
    pub async fn start(mode: Mode) -> Self {
        let origin = Fixture::start(mode).await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/file", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let redirect = Arc::new(Mutex::new(None));
        let data = Data {
            origin: origin.state.clone(),
            requests: requests.clone(),
            redirect: redirect.clone(),
        };
        let stop = CancellationToken::new();
        let token = stop.clone();
        let task = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/{*path}", get(serve)).with_state(data),
            )
            .with_graceful_shutdown(token.cancelled_owned())
            .await
            .unwrap();
        });
        Self {
            origin,
            url,
            requests,
            redirect,
            stop,
            task,
        }
    }
    // Intentionally mirrors the independent cookie fixture: stop the capture
    // listener before its backing origin so teardown cannot outlive either server.
    pub async fn close(self) {
        self.stop.cancel();
        self.task.await.unwrap();
        self.origin.close().await;
    }
}
async fn serve(
    State(data): State<Data>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Response {
    data.requests
        .lock()
        .unwrap()
        .push((uri.path().into(), headers.clone()));
    let destination = data.redirect.lock().unwrap().clone();
    if uri.path() == "/redirect"
        && let Some(location) = destination
    {
        return Response::builder()
            .status(StatusCode::FOUND)
            .header(header::LOCATION, location)
            .body(Body::empty())
            .unwrap();
    }
    if uri.path() == "/bad" {
        return Response::builder()
            .status(StatusCode::PARTIAL_CONTENT)
            .header(header::CONTENT_RANGE, SECRET)
            .body(Body::empty())
            .unwrap();
    }
    let mut response = fixture::serve(State(data.origin), headers).await;
    response.headers_mut().insert(
        header::SET_COOKIE,
        "learned=server-session; Path=/; HttpOnly".parse().unwrap(),
    );
    response
}

pub(super) fn options() -> vise::engine::TransferOptions {
    let request = serde_json::from_value(serde_json::json!({
        "headers":[{"name":"X-Secret","value":SECRET},{"name":"Authorization","value":SECRET}],
        "referer":format!("http://source.example/private/{SECRET}?session=private"),"user_agent":SECRET,
        "cookies":[{"name":"session","value":SECRET,"domain":"127.0.0.1","path":"/","secure":false,"expires":null,"httpOnly":true}]
    })).unwrap();
    vise::engine::TransferOptions {
        request,
        ..Default::default()
    }
}
