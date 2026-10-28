use super::fixture::{Capture, SECRET, options};
use crate::fixture::{Mode, terminal};
use futures_util::StreamExt;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use tokio::io::AsyncReadExt;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_util::sync::CancellationToken;
use vise::{
    engine::{Config, Manager},
    events::EventKind,
    net::credentials::RequestOverrides,
    web::DaemonConfig,
};

const TOKEN: &str = "daemon-api-token-0123456789abcdef0123456789";
pub(super) struct Api {
    pub root: tempfile::TempDir,
    pub manager: Manager,
    pub base: String,
    stop: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}
impl Api {
    pub async fn start(defaults: RequestOverrides) -> Self {
        let root = tempfile::tempdir().unwrap();
        let manager = Manager::open(Config {
            home_dir: Some(root.path().join("test-home")),
            output_dir: root.path().join("out"),
            request: defaults,
            ..Default::default()
        })
        .await
        .unwrap();
        let mut events = manager.subscribe_events();
        let stop = CancellationToken::new();
        let token = stop.clone();
        let worker = manager.clone();
        let task = tokio::spawn(async move {
            vise::web::run_daemon(
                worker,
                DaemonConfig::new("127.0.0.1:0".parse().unwrap(), Some(TOKEN.into())).unwrap(),
                token,
            )
            .await
            .unwrap();
        });
        let event = tokio::time::timeout(std::time::Duration::from_secs(5), events.recv())
            .await
            .unwrap()
            .unwrap();
        let EventKind::ServerStarted { address: base } = event.kind else {
            panic!("expected server readiness")
        };
        Self {
            root,
            manager,
            base,
            stop,
            task,
        }
    }
    pub async fn post(&self, path: &str, body: Value) -> reqwest::Response {
        Client::new()
            .post(format!("{}{path}", self.base))
            .bearer_auth(TOKEN)
            .header("content-type", "application/json")
            .body(body.to_string())
            .send()
            .await
            .unwrap()
    }
    pub async fn close(self) {
        self.manager.shutdown().await.unwrap();
        self.stop.cancel();
        self.task.await.unwrap();
    }
    pub fn command(&self) -> std::process::Command {
        let mut command = crate::fixture::command(self.root.path());
        command
            .env("VISE_API_TOKEN", TOKEN)
            .args(["remote", "--endpoint", &self.base, "--json"]);
        command
    }
}

#[tokio::test]
async fn credentials_absent_from_api_websocket_monitor_and_events() {
    let source = Capture::start(Mode::Transient).await;
    let api = Api::start(Default::default()).await;
    let events = api.manager.subscribe_events();
    let stop = CancellationToken::new();
    let (writer, mut reader) = tokio::io::duplex(65536);
    let monitor = tokio::spawn(vise::telemetry::run_with_writer(
        api.manager.clone(),
        events,
        stop.clone(),
        writer,
        vise::telemetry::Options {
            interval: std::time::Duration::from_millis(5),
            color: false,
        },
    ));
    let mut body = json!({"url":source.url});
    options().request.encode_admission(&mut body).unwrap();
    let response = api.post("/api/downloads", body).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let text = response.text().await.unwrap();
    assert!(!text.contains(SECRET));
    let task: vise::model::Download = serde_json::from_str(&text).unwrap();
    assert_eq!(task.request.header_names, ["authorization", "x-secret"]);
    assert!(task.request.has_cookies && task.request.has_user_agent && task.request.has_referer);
    terminal(&api.manager, task.id).await;
    let mut bad = json!({"url":source.url.replace("/file","/bad")});
    options().request.encode_admission(&mut bad).unwrap();
    let response = api.post("/api/downloads", bad).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let failed: vise::model::Download =
        serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    let failed = terminal(&api.manager, failed.id).await;
    assert_eq!(failed.status, vise::model::Status::Failed);
    assert!(!failed.error.as_deref().unwrap().contains(SECRET));
    for path in [
        "/api/downloads".into(),
        format!("/api/downloads/{}", task.id),
        format!("/api/downloads/{}", failed.id),
        "/api/telemetry".into(),
    ] {
        let text = Client::new()
            .get(format!("{}{path}", api.base))
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(!text.contains(SECRET) && !text.contains(TOKEN));
    }
    for path in ["/ws", "/ws/telemetry"] {
        let mut request = format!("{}{path}", api.base.replace("http:", "ws:"))
            .into_client_request()
            .unwrap();
        request
            .headers_mut()
            .insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
        let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let frame = socket.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(!frame.contains(SECRET) && !frame.contains(TOKEN));
        socket.close(None).await.unwrap();
    }
    stop.cancel();
    monitor.await.unwrap().unwrap();
    let mut log = String::new();
    reader.read_to_string(&mut log).await.unwrap();
    assert!(log.contains("ADDED") && !log.contains(SECRET) && !log.contains(TOKEN));
    let saved = rusqlite::Connection::open(api.manager.database_path())
        .unwrap()
        .query_row("SELECT request_credentials FROM downloads", [], |r| {
            r.get::<_, String>(0)
        })
        .unwrap();
    assert!(saved.contains(SECRET) && !saved.contains(TOKEN));
    api.close().await;
    source.close().await;
}

#[tokio::test]
async fn api_validation_and_multi_origin_batch_fail_before_admission_without_echo() {
    let api = Api::start(Default::default()).await;
    for body in [
        json!({"url":"http://127.0.0.1/file","headers":[{"name":"Cookie","value":SECRET}]}),
        json!({"url":"http://127.0.0.1/file","headers":[{"name":"X-Secret","value":format!("{SECRET}\r\ninjected")}]}),
        json!({"url":"http://127.0.0.1/file","cookies":[{"name":"session","value":SECRET,"secure":SECRET}]}),
        json!({"url":"http://127.0.0.1/file",SECRET:SECRET}),
    ] {
        let response = api.post("/api/downloads", body).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(!response.text().await.unwrap().contains(SECRET));
    }
    let mut body = json!({"urls":["http://127.0.0.1:1/file","http://127.0.0.1:2/file"]});
    options().request.encode_admission(&mut body).unwrap();
    let response = api.post("/api/downloads/batch", body).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let error = response.text().await.unwrap();
    assert!(error.contains("split") && !error.contains(SECRET));
    assert!(api.manager.list().await.is_empty());
    api.close().await;
}

#[tokio::test]
async fn api_omitted_and_null_fields_inherit_process_defaults() {
    let source = Capture::start(Mode::Ranges).await;
    let defaults = RequestOverrides::from_cli(&[], None, Some(SECRET)).unwrap();
    let api = Api::start(defaults).await;
    for fields in [
        json!({}),
        json!({"headers":null,"referer":null,"user_agent":null,"cookies":null}),
        json!({"user_agent":""}),
    ] {
        let mut body = fields;
        body["url"] = json!(source.url);
        let response = api.post("/api/downloads", body.clone()).await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let task: vise::model::Download =
            serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
        assert_eq!(task.request.has_user_agent, body["user_agent"] != "");
        terminal(&api.manager, task.id).await;
    }
    api.close().await;
    source.close().await;
}

#[tokio::test]
async fn daemon_token_never_enters_task_credentials() {
    let source = Capture::start(Mode::Ranges).await;
    let api = Api::start(Default::default()).await;
    let response = api
        .post(
            "/api/downloads",
            json!({"url":source.url,"user_agent":SECRET}),
        )
        .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let task: vise::model::Download =
        serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    terminal(&api.manager, task.id).await;
    let connection = rusqlite::Connection::open(api.manager.database_path()).unwrap();
    let saved: String = connection
        .query_row("SELECT request_credentials FROM downloads", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert!(!saved.contains(TOKEN));
    for (_, headers) in source.requests.lock().unwrap().iter() {
        assert!(headers.get("authorization").is_none());
    }
    drop(connection);
    api.close().await;
    source.close().await;
}

#[tokio::test]
async fn user_agent_only_batch_may_span_origins() {
    let first = Capture::start(Mode::Ranges).await;
    let second = Capture::start(Mode::Ranges).await;
    let api = Api::start(Default::default()).await;
    let response = api
        .post(
            "/api/downloads/batch",
            json!({"urls":[first.url,second.url],"user_agent":SECRET}),
        )
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    let result: vise::engine::BatchResult =
        serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(result.accepted.len(), 2);
    for entry in result.accepted {
        assert_eq!(
            terminal(&api.manager, entry.download.id).await.status,
            vise::model::Status::Completed
        );
    }
    for capture in [&first, &second] {
        for (_, headers) in capture.requests.lock().unwrap().iter() {
            assert_eq!(headers.get("user-agent").unwrap(), SECRET);
        }
    }
    api.close().await;
    first.close().await;
    second.close().await;
}
