//! Validated per-transfer proxy routes and a common HTTP client configuration.
use std::{fmt, str::FromStr, time::Duration};

use reqwest::{Client, ClientBuilder, Proxy};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use url::Url;

use crate::error::{Error, Result};

/// Serialization retains authentication for recovery. Display/Debug and public
/// download snapshots intentionally expose only the scheme, host, and port.
#[derive(Clone, PartialEq, Eq)]
pub struct ProxyUrl(Url);

impl FromStr for ProxyUrl {
    type Err = Error;
    fn from_str(value: &str) -> Result<Self> {
        let invalid = || {
            Error::Invalid("invalid proxy URL; use http://host:port, https://host:port, socks5://host:port, or socks5h://host:port (optional username:password)".into())
        };
        if value.is_empty() || value.len() > 8192 || value.chars().any(char::is_whitespace) {
            return Err(invalid());
        }
        let url = Url::parse(value).map_err(|_| invalid())?;
        if !matches!(url.scheme(), "http" | "https" | "socks5" | "socks5h")
            || url.host_str().is_none()
            || url.port() == Some(0)
            || !matches!(url.path(), "" | "/")
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(invalid());
        }
        // Validate against the HTTP client's supported route syntax too. Never
        // surface its parser error, which may contain the unredacted input.
        Proxy::all(url.as_str()).map_err(|_| invalid())?;
        Ok(Self(url))
    }
}
impl ProxyUrl {
    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn redacted(&self) -> String {
        let mut url = self.0.clone();
        let _ = url.set_password(None);
        let _ = url.set_username("");
        url.into()
    }
    pub fn authenticated(&self) -> bool {
        !self.0.username().is_empty() || self.0.password().is_some()
    }
    /// Explicit routes override environment proxies and NO_PROXY; failure never
    /// falls back to a direct connection. TLS verification remains enabled.
    pub fn apply(&self, builder: ClientBuilder) -> Result<ClientBuilder> {
        let route = Proxy::all(self.as_str())
            .map_err(|_| Error::Invalid("cannot configure the saved proxy route".into()))?;
        Ok(builder.no_proxy().proxy(route))
    }
}
impl fmt::Display for ProxyUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.redacted())
    }
}
impl fmt::Debug for ProxyUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ProxyUrl")
            .field(&self.redacted())
            .finish()
    }
}
impl Serialize for ProxyUrl {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}
impl<'de> Deserialize<'de> for ProxyUrl {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(D::Error::custom)
    }
}

pub(crate) fn client(read_timeout: Duration, route: Option<&ProxyUrl>) -> Result<Client> {
    let builder = Client::builder()
        .user_agent(concat!("Vise/", env!("CARGO_PKG_VERSION")))
        .connect_timeout(Duration::from_secs(15))
        .read_timeout(read_timeout)
        .redirect(reqwest::redirect::Policy::limited(10))
        .pool_max_idle_per_host(64);
    let builder = match route {
        Some(route) => route.apply(builder)?,
        None => builder,
    };
    Ok(builder.build()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validates_routes_and_redacts_credentials() {
        for value in [
            "http://127.0.0.1:8080",
            "https://proxy.example:8443",
            "socks5://[::1]:1080",
            "socks5h://proxy.example",
        ] {
            assert!(value.parse::<ProxyUrl>().is_ok(), "{value}");
        }
        for value in [
            "",
            "proxy:8080",
            "ftp://proxy:80",
            "socks4://proxy:1080",
            "http://proxy/path",
            "http://proxy?token=secret",
            "http://proxy/#secret",
            "http://proxy:0",
            "http://u:secret@proxy:99999",
            " http://proxy",
        ] {
            let error = value.parse::<ProxyUrl>().unwrap_err();
            assert!(!error.to_string().contains("secret"));
        }
        let route: ProxyUrl = "http://alice:p%40ssword@proxy.example:8080"
            .parse()
            .unwrap();
        assert!(route.authenticated());
        assert_eq!(route.redacted(), "http://proxy.example:8080/");
        assert!(!format!("{route:?} {route}").contains("alice"));
        assert!(!format!("{route:?} {route}").contains("ssword"));
        let saved = serde_json::to_string(&route).unwrap();
        assert!(saved.contains("p%40ssword"));
        assert_eq!(serde_json::from_str::<ProxyUrl>(&saved).unwrap(), route);
    }
}
