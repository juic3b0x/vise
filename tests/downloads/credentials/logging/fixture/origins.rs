use super::Secrets;
use crate::proxy_support::{Kind, TestProxy};
use axum::{
    Router,
    body::Body,
    extract::{OriginalUri, State},
    http::{HeaderMap, StatusCode, header},
    response::Response,
    routing::get,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::{net::TcpListener, sync::Notify};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct Data {
    destination: String,
    cookie: String,
    requests: Arc<Mutex<Vec<(String, HeaderMap)>>>,
    changed: Arc<Notify>,
}
pub(crate) struct Origins {
    pub url: String,
    pub proxy: TestProxy,
    pub proxy_url: String,
    data: Data,
    stop: CancellationToken,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}
impl Origins {
    pub async fn start(secrets: &Secrets) -> Self {
        let source = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/redirect", source.local_addr().unwrap());
        let data = Data {
            destination: format!("http://{}/file", target.local_addr().unwrap()),
            cookie: secrets.get("response_cookie").into(),
            requests: Default::default(),
            changed: Default::default(),
        };
        let stop = CancellationToken::new();
        let mut tasks = Vec::new();
        for listener in [source, target] {
            let state = data.clone();
            let token = stop.clone();
            tasks.push(tokio::spawn(async move {
                axum::serve(
                    listener,
                    Router::new()
                        .route("/{*path}", get(serve))
                        .with_state(state),
                )
                .with_graceful_shutdown(token.cancelled_owned())
                .await
                .unwrap();
            }));
        }
        let proxy = TestProxy::start(Kind::Http, false).await;
        let proxy_url = proxy.url.replacen(
            "http://",
            &format!("http://audit:{}@", secrets.get("proxy_password")),
            1,
        );
        Self {
            url,
            proxy,
            proxy_url,
            data,
            stop,
            tasks,
        }
    }
    pub async fn wait_for_stall(&self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if self
                    .data
                    .requests
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|(path, _)| path == "/stall")
                {
                    break;
                }
                self.data.changed.notified().await;
            }
        })
        .await
        .unwrap();
    }
    pub fn verify(&self, secrets: &Secrets) {
        let requests = self.data.requests.lock().unwrap();
        let original = requests
            .iter()
            .filter(|(path, _)| path == "/redirect")
            .collect::<Vec<_>>();
        let final_hops = requests
            .iter()
            .filter(|(path, _)| path == "/file")
            .collect::<Vec<_>>();
        assert!(original.len() >= 2 && final_hops.len() >= 2);
        for (_, headers) in &original {
            assert_eq!(headers.get("x-secret").unwrap(), secrets.get("header"));
            assert!(
                headers
                    .get("authorization")
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .contains(secrets.get("authorization"))
            );
            assert!(
                headers
                    .get("referer")
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .contains(secrets.get("referer"))
            );
            assert_eq!(
                headers.get("user-agent").unwrap(),
                secrets.get("user_agent")
            );
            let cookies = headers.get("cookie").unwrap().to_str().unwrap();
            assert!(
                cookies.contains(secrets.get("supplied_cookie"))
                    && cookies.contains(secrets.get("file_cookie"))
            );
        }
        assert!(original.iter().any(|(_, h)| {
            h.get("cookie")
                .unwrap()
                .to_str()
                .unwrap()
                .contains(secrets.get("response_cookie"))
        }));
        for (_, headers) in final_hops {
            assert!(
                headers.get("x-secret").is_none()
                    && headers.get("authorization").is_none()
                    && headers.get("cookie").is_none()
            );
            assert_ne!(
                headers.get("user-agent").unwrap(),
                secrets.get("user_agent")
            );
            assert_eq!(headers.get("referer").unwrap(), "http://referer.invalid/");
        }
        assert!(self.proxy.requests.lock().unwrap().len() >= 4);
    }
    pub async fn close(self) {
        self.proxy.close().await;
        self.stop.cancel();
        for task in self.tasks {
            task.await.unwrap();
        }
    }
}
async fn serve(
    State(data): State<Data>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Response {
    let first_payload = {
        let mut requests = data.requests.lock().unwrap();
        let first = uri.path() == "/file" && !requests.iter().any(|(path, _)| path == "/file");
        requests.push((uri.path().into(), headers.clone()));
        first
    };
    data.changed.notify_one();
    if uri.path() == "/stall" {
        return std::future::pending().await;
    }
    if uri.path() == "/redirect" {
        return Response::builder()
            .status(StatusCode::FOUND)
            .header(header::LOCATION, data.destination)
            .header(
                header::SET_COOKIE,
                format!("learned={}; Path=/; HttpOnly", data.cookie),
            )
            .body(Body::empty())
            .unwrap();
    }
    if first_payload {
        // Force a retry so response cookies are exercised even when fresh C=1
        // downloads use a single metadata-and-payload request.
        return Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header(header::RETRY_AFTER, "1")
            .body(Body::empty())
            .unwrap();
    }
    let total = 8192usize;
    let (start, end) = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("bytes="))
        .and_then(|v| v.split_once('-'))
        .map(|(start, end)| {
            (
                start.parse::<usize>().unwrap(),
                end.parse::<usize>().unwrap_or(total - 1),
            )
        })
        .unwrap_or((0, total - 1));
    Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header(header::ETAG, "\"logging-v1\"")
        .header(
            header::CONTENT_RANGE,
            format!("bytes {start}-{end}/{total}"),
        )
        .body(Body::from(vec![42; end - start + 1]))
        .unwrap()
}
