//! Shared real-binary logging scenarios, with private temporary homes.
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use std::{path::Path, process::Output};
use vise::engine::{Config, Manager, TransferOptions};
mod origins;
mod process;
pub(super) use origins::Origins;
pub(super) use process::{Process, completed, ready, request};

pub(super) const FILTERS: [&str; 4] = [
    "trace",
    "debug",
    "cookie_store=trace",
    "vise=trace,cookie_store::cookie_store=trace",
];

pub(super) struct Secrets {
    values: Vec<(&'static str, String)>,
}
impl Secrets {
    pub fn new() -> Self {
        let id = uuid::Uuid::new_v4().simple().to_string();
        Self {
            values: [
                "header",
                "authorization",
                "referer",
                "user_agent",
                "supplied_cookie",
                "response_cookie",
                "file_cookie",
                "proxy_password",
                "api_token",
            ]
            .into_iter()
            .map(|name| (name, format!("vise_{name}_{id}")))
            .collect(),
        }
    }
    pub fn get(&self, name: &str) -> &str {
        &self.values.iter().find(|(key, _)| *key == name).unwrap().1
    }
    pub fn matches(&self, output: &Output) -> Vec<(&'static str, usize)> {
        let mut bytes = output.stdout.clone();
        bytes.extend_from_slice(&output.stderr);
        let text = String::from_utf8_lossy(&bytes);
        let mut counts = self
            .values
            .iter()
            .map(|(name, value)| (*name, text.matches(value).count()))
            .collect::<Vec<_>>();
        let encoded = STANDARD.encode(format!("audit:{}", self.get("proxy_password")));
        counts.push(("encoded_proxy_password", text.matches(&encoded).count()));
        counts
    }
    pub fn admission(&self, url: &str) -> Value {
        json!({"url":url,"headers":[{"name":"X-Secret","value":self.get("header")},{"name":"Authorization","value":format!("Bearer {}",self.get("authorization"))}],
            "referer":format!("http://referer.invalid/private/{}",self.get("referer")),"user_agent":self.get("user_agent"),
            "cookies":[{"name":"session","value":self.get("supplied_cookie"),"domain":"127.0.0.1","path":"/","secure":false,"expires":null,"httpOnly":true}]})
    }
}
pub(super) fn cookie_file(root: &Path, secrets: &Secrets) -> std::path::PathBuf {
    let file = root.join("cookies.txt");
    std::fs::write(
        &file,
        format!(
            "127.0.0.1\tFALSE\t/\tFALSE\t0\tfile_session\t{}\n",
            secrets.get("file_cookie")
        ),
    )
    .unwrap();
    file
}
pub(super) async fn seed(root: &Path, origins: &Origins, secrets: &Secrets, file: &Path) {
    let manager = Manager::open(Config {
        home_dir: Some(root.join("test-home")),
        output_dir: root.join("out"),
        concurrent_downloads: 1,
        cookies_file: Some(file.into()),
        proxy_url: Some(origins.proxy_url.parse().unwrap()),
        ..Default::default()
    })
    .await
    .unwrap();
    let blocker = manager
        .add(&origins.url.replace("/redirect", "/stall"), None)
        .await
        .unwrap();
    origins.wait_for_stall().await;
    let request = serde_json::from_value(secrets.admission(&origins.url)).unwrap();
    let task = manager
        .add_with_options(
            &origins.url,
            None,
            TransferOptions {
                request,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    manager.pause(task.id).await.unwrap();
    manager.delete(blocker.id).await.unwrap();
    manager.shutdown().await.unwrap();
}
