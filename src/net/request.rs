//! Per-task transport. NULL journal contexts retain Reqwest's original redirect
//! path; explicit request contexts use an origin-aware, credential-stripping loop.
use super::credentials::{BUILTIN_USER_AGENT, TaskRequestContext};
use crate::{
    error::{Error, Result},
    proxy::ProxyUrl,
};
use reqwest::{
    Client, Response, StatusCode, Url,
    cookie::{CookieStore, Jar},
    header,
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub(crate) struct Transport {
    client: Client,
    private: Option<Private>,
}
#[derive(Clone)]
struct Private {
    context: Arc<TaskRequestContext>,
    jar: Arc<Jar>,
}
impl From<Client> for Transport {
    fn from(client: Client) -> Self {
        Self {
            client,
            private: None,
        }
    }
}
impl Transport {
    /// Only a client with no task-specific inputs may enter the shared pool.
    pub(crate) async fn for_task(
        shared: &tokio::sync::OnceCell<Self>,
        timeout: Duration,
        proxy: Option<&ProxyUrl>,
        file: Option<&Path>,
        context: Option<&TaskRequestContext>,
    ) -> Result<(Self, Option<PathBuf>)> {
        if proxy.is_none() && file.is_none() && context.is_none() {
            let transport = shared
                .get_or_try_init(|| async {
                    Self::new(timeout, None, None, None).await.map(|v| v.0)
                })
                .await?;
            return Ok((transport.clone(), None));
        }
        Self::new(timeout, proxy, file, context).await
    }

    pub(crate) async fn new(
        timeout: Duration,
        proxy: Option<&ProxyUrl>,
        file: Option<&Path>,
        context: Option<&TaskRequestContext>,
    ) -> Result<(Self, Option<PathBuf>)> {
        let Some(context) = context else {
            let (client, path) = super::client(timeout, proxy, file).await?;
            return Ok((client.into(), path));
        };
        let (jar, path) = if let Some(file) = file {
            let loaded = super::cookies::load(file.to_owned()).await?;
            (loaded.jar, Some(loaded.path))
        } else {
            (Arc::new(Jar::default()), None)
        };
        context.populate(&jar)?;
        let mut builder = Client::builder()
            .user_agent(BUILTIN_USER_AGENT)
            .connect_timeout(Duration::from_secs(15))
            .read_timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .referer(false)
            .pool_max_idle_per_host(64);
        if let Some(proxy) = proxy {
            builder = proxy.apply(builder)?;
        }
        let client = builder.build().map_err(Error::private_transport)?;
        Ok((
            Self {
                client,
                private: Some(Private {
                    context: Arc::new(context.clone()),
                    jar,
                }),
            },
            path,
        ))
    }

    pub(crate) async fn request(
        &self,
        url: &str,
        range: Option<&str>,
        validator: Option<&str>,
        cancel: &CancellationToken,
    ) -> Result<Response> {
        let Some(private) = &self.private else {
            let request = self.base(url, range, validator);
            return tokio::select! {biased; _=cancel.cancelled()=>Err(Error::Cancelled),response=request.send()=>Ok(response?)};
        };
        let initial =
            Url::parse(url).map_err(|_| Error::Invalid("invalid saved request URL".into()))?;
        private.context.validate_urls(&[url.to_owned()])?;
        let mut current = initial.clone();
        let mut credentials_enabled = true;
        for hop in 0..=10 {
            credentials_enabled =
                redirect_credentials_allowed(&initial, &current, !credentials_enabled);
            let request =
                self.private_request(private, &current, range, validator, credentials_enabled)?;
            let response = tokio::select! {biased; _=cancel.cancelled()=>return Err(Error::Cancelled), response=request.send()=>response.map_err(Error::private_transport)?};
            if credentials_enabled {
                private.jar.set_cookies(
                    &mut response.headers().get_all(header::SET_COOKIE).iter(),
                    &current,
                );
            }
            if !matches!(
                response.status(),
                StatusCode::MOVED_PERMANENTLY
                    | StatusCode::FOUND
                    | StatusCode::SEE_OTHER
                    | StatusCode::TEMPORARY_REDIRECT
                    | StatusCode::PERMANENT_REDIRECT
            ) {
                return Ok(response);
            }
            if hop == 10 {
                return Err(Error::Protocol("redirect limit exceeded".into()));
            }
            let location = response
                .headers()
                .get(header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| Error::Protocol("redirect is missing a valid Location".into()))?;
            let next = current
                .join(location)
                .map_err(|_| Error::Protocol("invalid redirect location".into()))?;
            validate_redirect(&current, &next)?;
            current = next;
        }
        Err(Error::Protocol("redirect limit exceeded".into()))
    }

    fn base(
        &self,
        url: impl reqwest::IntoUrl,
        range: Option<&str>,
        validator: Option<&str>,
    ) -> reqwest::RequestBuilder {
        let mut request = self
            .client
            .get(url)
            .header(header::ACCEPT_ENCODING, "identity")
            .header("want-repr-digest", "sha-256=10, md5=1");
        if let Some(range) = range {
            request = request.header(header::RANGE, range);
        }
        if let Some(validator) = validator {
            request = request.header(header::IF_RANGE, validator);
        }
        request
    }

    fn private_request(
        &self,
        private: &Private,
        url: &Url,
        range: Option<&str>,
        validator: Option<&str>,
        enabled: bool,
    ) -> Result<reqwest::RequestBuilder> {
        let mut request = self.base(url.clone(), range, validator);
        if enabled {
            request = private.context.apply(request)?;
            if let Some(mut cookie) = private.jar.cookies(url) {
                cookie.set_sensitive(true);
                request = request.header(header::COOKIE, cookie);
            }
        } else if let Some(referer) = private.context.origin_referer()? {
            request = request.header(header::REFERER, referer);
        }
        Ok(request)
    }
}
/// An origin change permanently disables credentials for the current chain.
fn redirect_credentials_allowed(initial: &Url, next: &Url, already_stripped: bool) -> bool {
    !already_stripped && next.origin() == initial.origin()
}

fn validate_redirect(previous: &Url, next: &Url) -> Result<()> {
    if !matches!(next.scheme(), "http" | "https")
        || next.host_str().is_none()
        || !next.username().is_empty()
        || next.password().is_some()
    {
        return Err(Error::Protocol(
            "redirect target must be HTTP(S) without embedded credentials".into(),
        ));
    }
    if previous.scheme() == "https" && next.scheme() == "http" {
        return Err(Error::Protocol("refusing HTTPS redirect downgrade".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
