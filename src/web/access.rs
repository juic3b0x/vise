//! Shared request guards for the local dashboard and authenticated daemon.
use std::net::SocketAddr;

use axum::{
    Json,
    http::{HeaderMap, StatusCode, header, uri::Authority},
    response::{IntoResponse, Response},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use sha2::{Digest, Sha256};

#[derive(Clone)]
pub(super) struct Token([u8; 32]);

impl Token {
    pub(super) fn new(value: String) -> anyhow::Result<Self> {
        anyhow::ensure!(
            (32..=256).contains(&value.len())
                && value
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
            "VISE_API_TOKEN must contain 32–256 ASCII letters, digits, '-' or '_'"
        );
        Ok(Self(Sha256::digest(value.as_bytes()).into()))
    }

    fn matches(&self, value: &[u8]) -> bool {
        let actual = Sha256::digest(value);
        self.0
            .iter()
            .zip(actual)
            .fold(0u8, |difference, (a, b)| difference | (a ^ b))
            == 0
    }

    fn authorized(&self, headers: &HeaderMap) -> bool {
        let Some(value) = headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
        else {
            return false;
        };
        if value.len() > 1024 {
            return false;
        }
        let Some((scheme, credential)) = value.split_once(' ') else {
            return false;
        };
        if scheme.eq_ignore_ascii_case("bearer") {
            return self.matches(credential.as_bytes());
        }
        if !scheme.eq_ignore_ascii_case("basic") {
            return false;
        }
        STANDARD
            .decode(credential)
            .ok()
            .and_then(|decoded| {
                decoded
                    .strip_prefix(b"vise:")
                    .map(|value| self.matches(value))
            })
            .unwrap_or(false)
    }
}

#[derive(Clone)]
pub(super) struct Access {
    authority: String,
    token: Option<Token>,
}

impl Access {
    pub(super) fn new(address: SocketAddr, token: Option<Token>) -> Self {
        Self {
            authority: address.to_string(),
            token,
        }
    }

    pub(super) fn check(&self, headers: &HeaderMap) -> Result<(), Denied> {
        let host = headers
            .get(header::HOST)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if host.is_empty() || host.parse::<Authority>().is_err() {
            return Err(Denied::Forbidden("a valid Host header is required"));
        }
        if self.token.is_none() && host != self.authority {
            return Err(Denied::Forbidden(
                "use the exact loopback URL printed by Vise",
            ));
        }
        self.check_origin(headers, host)?;
        if self
            .token
            .as_ref()
            .is_some_and(|token| !token.authorized(headers))
        {
            return Err(Denied::Unauthorized);
        }
        Ok(())
    }

    fn check_origin(&self, headers: &HeaderMap, host: &str) -> Result<(), Denied> {
        if let Some(origin) = headers.get(header::ORIGIN) {
            let origin = origin.to_str().unwrap_or("");
            if origin != format!("http://{host}") && origin != format!("https://{host}") {
                return Err(Denied::Forbidden("cross-origin requests are forbidden"));
            }
            if self.token.is_none() && origin != format!("http://{host}") {
                return Err(Denied::Forbidden("cross-origin requests are forbidden"));
            }
        }
        if headers
            .get("sec-fetch-site")
            .is_some_and(|v| v == "cross-site")
        {
            return Err(Denied::Forbidden("cross-site requests are forbidden"));
        }
        Ok(())
    }
}

pub(super) enum Denied {
    Forbidden(&'static str),
    Unauthorized,
}

impl IntoResponse for Denied {
    fn into_response(self) -> Response {
        match self {
            Self::Forbidden(message) => super::rejection(message),
            Self::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                [
                    (
                        header::WWW_AUTHENTICATE,
                        "Basic realm=\"Vise\", charset=\"UTF-8\"",
                    ),
                    (header::CACHE_CONTROL, "no-store"),
                ],
                Json(serde_json::json!({"error":"daemon authentication required"})),
            )
                .into_response(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "synthetic-test-token-0123456789abcdef";

    #[test]
    fn daemon_configuration_requires_valid_credentials_for_remote_interfaces() {
        for bind in ["0.0.0.0:8080", "[::]:8080", "192.0.2.1:8080"] {
            let bind = bind.parse().unwrap();
            assert!(crate::web::DaemonConfig::new(bind, None).is_err());
            assert!(crate::web::DaemonConfig::new(bind, Some(SECRET.into())).is_ok());
        }
        assert!(crate::web::DaemonConfig::new("127.0.0.1:8080".parse().unwrap(), None).is_ok());
        for value in [
            "",
            "private-short-value",
            "bad value with spaces and enough length",
            "é",
        ] {
            let error = Token::new(value.into()).err().unwrap().to_string();
            assert!(!error.contains("private-short-value"));
        }
    }

    #[test]
    fn daemon_accepts_bearer_and_basic_but_rejects_invalid_credentials() {
        let token = Token::new(SECRET.into()).unwrap();
        for credential in [
            format!("Bearer {SECRET}"),
            format!("Basic {}", STANDARD.encode(format!("vise:{SECRET}"))),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(header::AUTHORIZATION, credential.parse().unwrap());
            assert!(token.authorized(&headers));
        }
        for credential in [
            "Bearer incorrect".to_owned(),
            "Basic %%%".into(),
            format!("Basic {}", STANDARD.encode(format!("other:{SECRET}"))),
            format!("Bearer {}", "a".repeat(2048)),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(header::AUTHORIZATION, credential.parse().unwrap());
            assert!(!token.authorized(&headers));
        }
        assert!(!token.authorized(&HeaderMap::new()));
    }

    #[test]
    fn daemon_preserves_origin_guards_with_dns_and_ipv6_hosts() {
        let access = Access::new(
            "0.0.0.0:8080".parse().unwrap(),
            Some(Token::new(SECRET.into()).unwrap()),
        );
        for host in ["downloads.example:8080", "192.0.2.1:8080", "[::1]:8080"] {
            let mut headers = HeaderMap::new();
            headers.insert(header::HOST, host.parse().unwrap());
            headers.insert(
                header::AUTHORIZATION,
                format!("Bearer {SECRET}").parse().unwrap(),
            );
            for scheme in ["http", "https"] {
                headers.insert(
                    header::ORIGIN,
                    format!("{scheme}://{host}").parse().unwrap(),
                );
                assert!(access.check(&headers).is_ok());
            }
            headers.insert(header::ORIGIN, "https://untrusted.example".parse().unwrap());
            assert!(matches!(access.check(&headers), Err(Denied::Forbidden(_))));
            headers.remove(header::ORIGIN);
            headers.insert("sec-fetch-site", "cross-site".parse().unwrap());
            assert!(access.check(&headers).is_err());
        }
    }
}
