use std::{
    process::{Child, Command, Output, Stdio},
    time::Duration,
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use vise::{
    engine::{Config, Manager},
    events::EventKind,
    web::DaemonConfig,
};

pub(super) const SECRET: &str = "client-fixture-token-0123456789abcdef";

pub(super) struct Daemon {
    pub root: tempfile::TempDir,
    pub manager: Manager,
    pub endpoint: String,
    stop: CancellationToken,
    server: Option<JoinHandle<()>>,
}

impl Daemon {
    pub async fn start() -> Self {
        let root = tempfile::tempdir().unwrap();
        let home = root.path().join("test-home");
        let manager = Manager::open(Config {
            home_dir: Some(home.clone()),
            output_dir: root.path().join("output"),
            concurrent_downloads: 1,
            connections_per_download: 2,
            ..Config::default()
        })
        .await
        .unwrap();
        let mut events = manager.subscribe_events();
        let stop = CancellationToken::new();
        let worker = manager.clone();
        let token = stop.clone();
        let server = tokio::spawn(async move {
            vise::web::run_daemon(
                worker,
                DaemonConfig::new("127.0.0.1:0".parse().unwrap(), Some(SECRET.into())).unwrap(),
                token,
            )
            .await
            .unwrap();
        });
        let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
            .await
            .unwrap()
            .unwrap();
        let EventKind::ServerStarted { address: endpoint } = event.kind else {
            panic!("expected listener readiness");
        };
        let address: std::net::SocketAddr = endpoint.trim_start_matches("http://").parse().unwrap();
        std::fs::write(home.join(".vise/config.toml"), format!(
            "[daemon]\nhost = \"127.0.0.1\"\nport = {}\nVISE_API_TOKEN = \"{SECRET}\"\n[downloads]\ndefault_dir = \"~/unused-client-default\"\ncookies_file = \"missing-client-cookies.txt\"\n[proxy]\nurl = \"invalid-download-proxy\"\n", address.port())).unwrap();
        Self {
            root,
            manager,
            endpoint,
            stop,
            server: Some(server),
        }
    }

    pub fn command(&self) -> Command {
        let mut command = crate::fixture::command(self.root.path());
        command.arg("remote");
        command
    }

    pub async fn run(&self, args: &[&str]) -> Output {
        let mut command = self.command();
        command.args(args);
        run(command).await
    }

    pub async fn close(mut self) {
        self.manager.shutdown().await.unwrap();
        self.stop.cancel();
        self.server.take().unwrap().await.unwrap();
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        self.stop.cancel();
        if let Some(server) = self.server.take() {
            server.abort();
        }
    }
}

pub(super) struct Process(pub Option<Child>);
impl Drop for Process {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

pub(super) async fn run(mut command: Command) -> Output {
    let mut process = Process(Some(
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    ));
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if process.0.as_mut().unwrap().try_wait().unwrap().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("client did not exit within its test deadline");
    process.0.take().unwrap().wait_with_output().unwrap()
}

pub(super) fn decoded<T: serde::de::DeserializeOwned>(output: &Output, code: i32) -> T {
    assert_eq!(
        output.status.code(),
        Some(code),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
