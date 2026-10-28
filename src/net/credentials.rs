//! Private task credentials. Ordinary formatting/serialization is always redacted;
//! only the explicitly named journal and outbound codecs expose credential bytes.
use crate::error::{Error, Result};
use reqwest::header::{HeaderName, HeaderValue};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use std::{collections::BTreeSet, fmt};
use url::Url;

mod codec;
mod cookie;
#[cfg(test)]
mod tests;
mod validation;

pub const ADMISSION_BODY_BYTES: usize = 128 * 1024;
pub const BUILTIN_USER_AGENT: &str = concat!("Vise/", env!("CARGO_PKG_VERSION"));

#[derive(Clone, PartialEq, Eq, Deserialize)]
struct Secret(String);
impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[redacted]")
    }
}
impl fmt::Display for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
impl Serialize for Secret {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str("[redacted]")
    }
}

#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    name: String,
    value: Secret,
}

#[derive(Clone, Default, PartialEq, Eq)]
pub struct RequestOverrides {
    headers: Option<Vec<Header>>,
    referer: Option<Secret>,
    user_agent: Option<Secret>,
    cookies: Option<Vec<cookie::SuppliedCookie>>,
}

impl<'de> Deserialize<'de> for RequestOverrides {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Input {
            headers: Option<Vec<Header>>,
            referer: Option<Secret>,
            user_agent: Option<Secret>,
            cookies: Option<Vec<cookie::SuppliedCookie>>,
        }
        let input = Input::deserialize(d)
            .map_err(|_| D::Error::custom("invalid request credential fields"))?;
        Ok(Self {
            headers: input.headers,
            referer: input.referer,
            user_agent: input.user_agent,
            cookies: input.cookies,
        })
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct TaskRequestContext {
    headers: Vec<Header>,
    referer: Option<Secret>,
    user_agent: Secret,
    custom_user_agent: bool,
    cookies: Vec<cookie::SuppliedCookie>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RequestSummary {
    pub header_names: Vec<String>,
    pub header_count: usize,
    pub has_custom_headers: bool,
    pub has_referer: bool,
    pub has_user_agent: bool,
    pub has_cookies: bool,
}

impl RequestOverrides {
    pub fn from_cli(
        headers: &[String],
        referer: Option<&str>,
        user_agent: Option<&str>,
    ) -> Result<Self> {
        let headers = headers
            .iter()
            .map(|line| {
                let (name, value) = line
                    .split_once(':')
                    .ok_or_else(|| invalid("header must use Name: value syntax"))?;
                Ok(Header {
                    name: name.to_owned(),
                    value: Secret(value.trim_matches(' ').to_owned()),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Self {
            headers: (!headers.is_empty()).then_some(headers),
            referer: referer.map(|v| Secret(v.into())),
            user_agent: user_agent.map(|v| Secret(v.into())),
            cookies: None,
        }
        .normalized()
    }

    /// Validate before queue admission, retaining explicit empty overrides.
    pub fn resolve(&self, defaults: &Self) -> Result<Option<TaskRequestContext>> {
        let defaults = defaults.normalized()?;
        let selected = self.normalized()?;
        let present = self.present() || defaults.present();
        if !present {
            return Ok(None);
        }
        let mut headers = defaults.headers.unwrap_or_default();
        if let Some(replacements) = selected.headers {
            if replacements.is_empty() {
                headers.clear();
            } else {
                let names: BTreeSet<_> = replacements.iter().map(|h| h.name.clone()).collect();
                headers.retain(|h| !names.contains(&h.name));
                headers.extend(replacements);
            }
        }
        let referer = selected
            .referer
            .or(defaults.referer)
            .filter(|v| !v.0.is_empty());
        let user_agent = selected
            .user_agent
            .or(defaults.user_agent)
            .filter(|v| !v.0.is_empty());
        let context = TaskRequestContext {
            headers,
            referer,
            custom_user_agent: user_agent.is_some(),
            user_agent: user_agent.unwrap_or_else(|| Secret(BUILTIN_USER_AGENT.into())),
            cookies: selected.cookies.or(defaults.cookies).unwrap_or_default(),
        };
        context.validate()?;
        Ok(Some(context))
    }

    pub fn with_user_agent_default(mut self, value: Option<&str>) -> Result<Self> {
        if self.user_agent.is_none() {
            self.user_agent = value.map(|v| Secret(v.into()));
        }
        self.normalized()
    }

    fn present(&self) -> bool {
        self.headers.is_some()
            || self.referer.is_some()
            || self.user_agent.is_some()
            || self.cookies.is_some()
    }

    pub fn validate_urls(&self, defaults: &Self, urls: &[String]) -> Result<()> {
        if let Some(context) = self.resolve(defaults)? {
            context.validate_urls(urls)?;
        }
        Ok(())
    }
}

impl TaskRequestContext {
    pub fn summary(&self) -> RequestSummary {
        RequestSummary {
            header_names: self
                .headers
                .iter()
                .map(|h| h.name.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
            header_count: self.headers.len(),
            has_custom_headers: !self.headers.is_empty(),
            has_referer: self.referer.is_some(),
            has_user_agent: self.custom_user_agent,
            has_cookies: !self.cookies.is_empty(),
        }
    }

    pub fn validate_urls(&self, urls: &[String]) -> Result<()> {
        let mut first = None;
        for value in urls {
            let url = Url::parse(value).map_err(|_| invalid("invalid credential target URL"))?;
            if !matches!(url.scheme(), "http" | "https")
                || url.host_str().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
            {
                return Err(invalid("invalid credential target URL"));
            }
            if self.single_origin() {
                let origin = url.origin();
                if first.as_ref().is_some_and(|v| v != &origin) {
                    return Err(invalid(
                        "credentialed batches require one origin; split the batch by origin",
                    ));
                }
                first = Some(origin);
            }
            for cookie in &self.cookies {
                cookie.validate_scope(&url)?;
            }
        }
        Ok(())
    }

    fn single_origin(&self) -> bool {
        !self.headers.is_empty() || self.referer.is_some() || !self.cookies.is_empty()
    }

    pub(super) fn apply(
        &self,
        mut request: reqwest::RequestBuilder,
    ) -> Result<reqwest::RequestBuilder> {
        for header in &self.headers {
            request = request.header(&header.name, sensitive(&header.value.0)?);
        }
        request = request.header(reqwest::header::USER_AGENT, sensitive(&self.user_agent.0)?);
        if let Some(value) = &self.referer {
            request = request.header(reqwest::header::REFERER, sensitive(&value.0)?);
        }
        Ok(request)
    }

    pub(super) fn origin_referer(&self) -> Result<Option<HeaderValue>> {
        self.referer
            .as_ref()
            .map(|value| {
                let url = Url::parse(&value.0).map_err(|_| invalid("invalid saved referer"))?;
                sensitive(&format!("{}/", url.origin().ascii_serialization()))
            })
            .transpose()
    }

    pub(super) fn populate(&self, jar: &reqwest::cookie::Jar) -> Result<()> {
        for cookie in &self.cookies {
            cookie.insert(jar)?;
        }
        Ok(())
    }
}

fn invalid(reason: &str) -> Error {
    Error::Invalid(reason.into())
}
fn sensitive(value: &str) -> Result<HeaderValue> {
    let mut header =
        HeaderValue::from_str(value).map_err(|_| invalid("invalid request header value"))?;
    header.set_sensitive(true);
    Ok(header)
}

impl Serialize for TaskRequestContext {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        self.summary().serialize(s)
    }
}
impl fmt::Debug for TaskRequestContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.summary().fmt(f)
    }
}
impl fmt::Display for TaskRequestContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
impl Serialize for RequestOverrides {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        self.resolve(&Self::default())
            .ok()
            .flatten()
            .map(|v| v.summary())
            .unwrap_or_default()
            .serialize(s)
    }
}
impl fmt::Debug for RequestOverrides {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RequestOverrides([redacted])")
    }
}
