use super::{ApiError, AppState, rejected_action};
use crate::{
    engine::BatchResult,
    error::{Error, Result},
    input,
};
use axum::{Json, extract::State};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Request {
    urls: Option<Vec<String>>,
    content: Option<String>,
    proxy_url: Option<String>,
}

impl Request {
    fn urls(self) -> Result<(Vec<String>, Option<crate::proxy::ProxyUrl>)> {
        let urls = match (self.urls, self.content) {
            (Some(urls), None) => input::validate_urls(urls)?,
            (None, Some(content)) => input::parse(&content)?,
            _ => {
                return Err(Error::Invalid(
                    "provide exactly one of urls or content".into(),
                ));
            }
        };
        let proxy = self.proxy_url.as_deref().map(str::parse).transpose()?;
        Ok((urls, proxy))
    }
}

pub(super) async fn add(
    State(state): State<AppState>,
    Json(request): Json<Request>,
) -> std::result::Result<Json<BatchResult>, ApiError> {
    let (urls, proxy) = request
        .urls()
        .inspect_err(|error| rejected_action(&state.manager, "batch", error))?;
    Ok(Json(state.manager.add_batch(urls, proxy).await?))
}
