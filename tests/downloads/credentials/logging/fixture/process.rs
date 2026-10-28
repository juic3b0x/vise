//! Concurrent pipe draining and bounded real-process/API supervision.
use serde_json::Value;
use std::{
    io::Read,
    process::{Child, Command, Output, Stdio},
    thread::JoinHandle,
    time::Duration,
};
use vise::model::{Download, Status};

pub(crate) struct Process {
    child: Child,
    stdout: Option<JoinHandle<Vec<u8>>>,
    stderr: Option<JoinHandle<Vec<u8>>>,
}
impl Process {
    pub fn spawn(mut command: Command) -> Self {
        let mut child = command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        fn drain(mut stream: impl Read + Send + 'static) -> JoinHandle<Vec<u8>> {
            std::thread::spawn(move || {
                let mut bytes = Vec::new();
                stream.read_to_end(&mut bytes).unwrap();
                bytes
            })
        }
        let stdout = Some(drain(child.stdout.take().unwrap()));
        let stderr = Some(drain(child.stderr.take().unwrap()));
        Self {
            child,
            stdout,
            stderr,
        }
    }
    pub async fn finish(mut self, terminate: bool) -> Output {
        if terminate {
            self.child.kill().unwrap();
        }
        let status = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(status) = self.child.try_wait().unwrap() {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        Output {
            status,
            stdout: self.stdout.take().unwrap().join().unwrap(),
            stderr: self.stderr.take().unwrap().join().unwrap(),
        }
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub(crate) async fn request(base: &str, path: &str, token: &str, body: Option<Value>) -> Value {
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let request = if let Some(body) = body {
        client
            .post(format!("{base}{path}"))
            .header("content-type", "application/json")
            .body(body.to_string())
    } else {
        client.get(format!("{base}{path}"))
    };
    let response = request.bearer_auth(token).send().await.unwrap();
    assert!(response.status().is_success());
    serde_json::from_slice(&response.bytes().await.unwrap()).unwrap()
}
pub(crate) async fn ready(base: &str, token: &str) {
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(1))
        .build()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if client
                .get(format!("{base}/api/config"))
                .bearer_auth(token)
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
}
pub(crate) async fn completed(base: &str, token: &str, id: &str) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let value = request(base, &format!("/api/downloads/{id}"), token, None).await;
            let task: Download = serde_json::from_value(value).unwrap();
            if task.status == Status::Completed {
                break;
            }
            assert!(task.status.active());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
