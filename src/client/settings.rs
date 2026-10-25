use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use anyhow::{Context, Result, ensure};
use reqwest::header::HeaderValue;
use url::Url;
use vise::config;

pub(super) struct Settings {
    pub endpoint: Url,
    pub authorization: HeaderValue,
}

impl Settings {
    pub async fn load(endpoint: Option<String>) -> Result<Self> {
        let token = match std::env::var("VISE_API_TOKEN") {
            Ok(value) => Some(value),
            Err(std::env::VarError::NotPresent) => None,
            Err(_) => anyhow::bail!("VISE_API_TOKEN must contain valid Unicode"),
        };
        // A fully specified remote client does not even need a local home.
        let file = if endpoint.is_none() || token.is_none() {
            config::read_existing()
                .await?
                .map(|loaded| loaded.values.daemon)
        } else {
            None
        };
        let endpoint = match endpoint {
            Some(value) => endpoint_url(&value)?,
            None => {
                let daemon = file.as_ref().context(
                    "no daemon configuration found; specify --endpoint and VISE_API_TOKEN",
                )?;
                let host = match daemon.host {
                    IpAddr::V4(ip) if ip.is_unspecified() => Ipv4Addr::LOCALHOST.into(),
                    IpAddr::V6(ip) if ip.is_unspecified() => Ipv6Addr::LOCALHOST.into(),
                    host => host,
                };
                endpoint_url(&format!("http://{}", SocketAddr::new(host, daemon.port)))?
            }
        };
        let token = token
            .or_else(|| file.map(|daemon| daemon.api_token))
            .context("set VISE_API_TOKEN or configure daemon.VISE_API_TOKEN")?;
        Ok(Self {
            endpoint,
            authorization: authorization(&token)?,
        })
    }
}

fn endpoint_url(value: &str) -> Result<Url> {
    let invalid = || {
        anyhow::anyhow!(
            "endpoint must be an HTTP(S) daemon origin without credentials, path, query, or fragment"
        )
    };
    ensure!(
        value.trim() == value && !value.chars().any(char::is_control),
        "{}",
        invalid()
    );
    let url = Url::parse(value).map_err(|_| invalid())?;
    ensure!(
        matches!(url.scheme(), "http" | "https")
            && url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.path() == "/"
            && url.query().is_none()
            && url.fragment().is_none()
            && url.port_or_known_default().is_some_and(|port| port != 0),
        "{}",
        invalid()
    );
    let loopback = match url.host() {
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        Some(url::Host::Domain(name)) => name.eq_ignore_ascii_case("localhost"),
        None => false,
    };
    ensure!(
        url.scheme() == "https" || loopback,
        "non-loopback endpoints require HTTPS; use a local SSH tunnel for an HTTP daemon"
    );
    Ok(url)
}

fn authorization(token: &str) -> Result<HeaderValue> {
    ensure!(
        (32..=256).contains(&token.len())
            && token
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
        "VISE_API_TOKEN must contain 32–256 ASCII letters, digits, '-' or '_'"
    );
    let mut header = HeaderValue::from_str(&format!("Bearer {token}"))
        .map_err(|_| anyhow::anyhow!("invalid VISE_API_TOKEN header"))?;
    header.set_sensitive(true);
    Ok(header)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoints_require_safe_origins_and_encrypted_remote_transport() {
        for value in [
            "http://127.0.0.1:8080",
            "http://[::1]:8080",
            "http://localhost:9090",
            "https://vise.example.org",
        ] {
            assert!(endpoint_url(value).is_ok(), "{value}");
        }
        for value in [
            "http://192.0.2.1:8080",
            "http://example.org",
            "http://0.0.0.0:8080",
            "http://127.0.0.1:0",
            "http://secret@localhost",
            "http://localhost/api",
            "http://localhost?token=secret",
            "http://localhost/#secret",
            " http://localhost",
            "file:///etc/passwd",
        ] {
            let message = endpoint_url(value).unwrap_err().to_string();
            assert!(!message.contains(value) && !message.contains("secret"));
        }
    }

    #[test]
    fn authorization_is_validated_and_marked_sensitive() {
        let header = authorization("random-test-token-0123456789abcdef").unwrap();
        assert!(header.is_sensitive());
        for value in ["", "private-invalid-token", "secret\nAuthorization: x"] {
            let message = authorization(value).unwrap_err().to_string();
            assert!(!message.contains(value) || value.is_empty());
        }
    }
}
