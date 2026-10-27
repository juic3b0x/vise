use std::time::Duration;

use anyhow::{Result, ensure};
use reqwest::{Client, Method, Response, StatusCode, header};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use uuid::Uuid;
use vise::{model::Download, rate_limiter::RateConfiguration};

use super::settings::Settings;

const MAX_RESPONSE: usize = 32 * 1024 * 1024;

pub(super) struct Api {
    client: Client,
    settings: Settings,
}

#[derive(Deserialize, Serialize)]
pub(super) struct Telemetry {
    pub timestamp: i64,
    pub downloads: Vec<Download>,
    pub configuration: RateConfiguration,
}

impl Api {
    pub fn new(settings: Settings, timeout: u64) -> Result<Self> {
        let client = Client::builder()
            // API credentials must never follow download/environment proxies,
            // cookies, redirects, or retries of an ambiguously admitted POST.
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .timeout(Duration::from_secs(timeout))
            .connect_timeout(Duration::from_secs(timeout.min(15)))
            .user_agent(concat!("Vise-client/", env!("CARGO_PKG_VERSION")))
            .build()?;
        Ok(Self { client, settings })
    }

    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        self.json(Method::GET, path, None::<&()>).await
    }

    pub async fn post<T: DeserializeOwned>(&self, path: &str, body: &impl Serialize) -> Result<T> {
        self.json(Method::POST, path, Some(body)).await
    }

    async fn json<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<&impl Serialize>,
    ) -> Result<T> {
        let mut response = self.request(method, path, body).await?;
        ensure!(
            response
                .content_length()
                .is_none_or(|n| n <= MAX_RESPONSE as u64),
            "daemon response exceeds 32 MiB; no mutation will be retried"
        );
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| {
            anyhow::anyhow!(
                "daemon response was interrupted; check task history before repeating a mutation"
            )
        })? {
            ensure!(
                chunk.len() <= MAX_RESPONSE.saturating_sub(bytes.len()),
                "daemon response exceeds 32 MiB; no mutation will be retried"
            );
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).map_err(|_| {
            anyhow::anyhow!(
                "daemon returned incompatible JSON; check task history before repeating a mutation"
            )
        })
    }

    async fn request(
        &self,
        method: Method,
        path: &str,
        body: Option<&impl Serialize>,
    ) -> Result<Response> {
        let url = self.settings.endpoint.join(path.trim_start_matches('/'))?;
        let mut request = self
            .client
            .request(method, url)
            .header(header::AUTHORIZATION, self.settings.authorization.clone());
        if let Some(body) = body {
            let bytes = serde_json::to_vec(body)?;
            let maximum = if path == "api/downloads/batch" {
                vise::input::MAX_BATCH_BODY_BYTES
            } else {
                16 * 1024
            };
            ensure!(bytes.len() <= maximum, "API request exceeds its size limit");
            request = request
                .header(header::CONTENT_TYPE, "application/json")
                .body(bytes);
        }
        let response = request.send().await.map_err(|_| anyhow::anyhow!(
            "daemon request failed; check endpoint, connectivity, and TLS trust; a mutation may already have been accepted, so inspect history before repeating it"))?;
        let status = response.status();
        ensure!(
            status.is_success(),
            "daemon returned HTTP {status}; verify authentication, task state, and server configuration"
        );
        Ok(response)
    }

    pub async fn delete(&self, id: Uuid) -> Result<()> {
        let response = self
            .request(
                Method::DELETE,
                &format!("api/downloads/{id}"),
                Some(&serde_json::json!({})),
            )
            .await?;
        ensure!(
            response.status() == StatusCode::NO_CONTENT,
            "unexpected daemon deletion response; inspect history before repeating it"
        );
        Ok(())
    }

    pub async fn resolve_id(&self, input: &str) -> Result<Uuid> {
        if let Ok(id) = Uuid::parse_str(input) {
            return Ok(id);
        }
        ensure!(
            input.len() >= 4
                && input.len() <= 36
                && input.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-'),
            "use a full UUID or a unique hexadecimal ID prefix of at least 4 characters"
        );
        let jobs: Vec<Download> = self.get("api/downloads").await?;
        let prefix = input.to_ascii_lowercase();
        let mut matches = jobs
            .iter()
            .filter(|job| job.id.to_string().starts_with(&prefix));
        let id = matches
            .next()
            .map(|job| job.id)
            .ok_or_else(|| anyhow::anyhow!("no download matches that ID"))?;
        ensure!(matches.next().is_none(), "ambiguous download ID");
        Ok(id)
    }
}
