use super::*;
use crate::net::cookies::NetscapeCookie;
use reqwest::cookie::Jar;

fn host_only() -> bool {
    true
}
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SuppliedCookie {
    name: Secret,
    value: Secret,
    domain: Secret,
    path: Secret,
    secure: bool,
    expires: Option<u64>,
    #[serde(rename = "httpOnly")]
    http_only: bool,
    #[serde(rename = "hostOnly", default = "host_only")]
    host_only: bool,
}

impl SuppliedCookie {
    fn record(&self) -> NetscapeCookie<'_> {
        NetscapeCookie {
            domain: &self.domain.0,
            subdomains: !self.host_only,
            path: &self.path.0,
            secure: self.secure,
            expires: self.expires.map_or(0, |v| v.max(1)),
            name: &self.name.0,
            value: &self.value.0,
            http_only: self.http_only,
        }
    }
    pub(super) fn normalize(&mut self) -> Result<()> {
        if self.size() > 4096 {
            return Err(invalid("supplied cookie exceeds 4 KiB"));
        }
        self.domain.0 = self
            .domain
            .0
            .strip_prefix('.')
            .unwrap_or(&self.domain.0)
            .to_ascii_lowercase();
        self.record().validate().map_err(invalid)?;
        Ok(())
    }
    pub(super) fn size(&self) -> usize {
        self.name.0.len() + self.value.0.len() + self.domain.0.len() + self.path.0.len() + 64
    }
    pub(super) fn validate_scope(&self, url: &Url) -> Result<()> {
        let host = url.host_str().unwrap_or_default();
        let domain = &self.domain.0;
        let matches = host == domain
            || (!self.host_only
                && host
                    .strip_suffix(domain.as_str())
                    .is_some_and(|prefix| prefix.ends_with('.')));
        let path = &self.path.0;
        let path_matches = url.path() == path
            || (url.path().starts_with(path)
                && (path.ends_with('/')
                    || url
                        .path()
                        .get(path.len()..)
                        .is_some_and(|suffix| suffix.starts_with('/'))));
        if !matches || !path_matches || (self.secure && url.scheme() != "https") {
            return Err(invalid(
                "supplied cookie is outside the original URL's domain/path/secure scope",
            ));
        }
        Ok(())
    }
    pub(super) fn insert(&self, jar: &Jar) -> Result<()> {
        self.record().insert(jar).map_err(invalid)
    }
    pub(super) fn wire(&self) -> serde_json::Value {
        serde_json::json!({"name":self.name.0,"value":self.value.0,"domain":self.domain.0,
            "path":self.path.0,"secure":self.secure,"expires":self.expires,"httpOnly":self.http_only,"hostOnly":self.host_only})
    }
}
