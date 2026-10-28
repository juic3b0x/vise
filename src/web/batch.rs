use super::{ApiError, AppState, admission_json, rejected_action};
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
    #[serde(flatten)]
    request: crate::net::credentials::RequestOverrides,
}

impl Request {
    fn urls(self) -> Result<(Vec<String>, crate::engine::TransferOptions)> {
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
        Ok((
            urls,
            crate::engine::TransferOptions {
                proxy_url: proxy,
                request: self.request,
                ..Default::default()
            },
        ))
    }
}

pub(super) async fn add(
    State(state): State<AppState>,
    request: std::result::Result<Json<Request>, axum::extract::rejection::JsonRejection>,
) -> std::result::Result<Json<BatchResult>, ApiError> {
    let request = admission_json(request)?;
    let (urls, options) = request
        .urls()
        .inspect_err(|error| rejected_action(&state.manager, "batch", error))?;
    Ok(Json(
        state.manager.add_batch_with_options(urls, options).await?,
    ))
}
