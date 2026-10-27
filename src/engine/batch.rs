//! Batch admission reuses the single-task path, journal, and worker semaphore.
use super::{Manager, TransferOptions};
use crate::{
    error::{Error, Result},
    events::EventKind,
    input,
    model::Download,
    proxy::ProxyUrl,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Accepted {
    /// One-based index in the submitted URL list, excluding comments/blank lines.
    pub index: usize,
    pub download: Download,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Rejected {
    pub index: usize,
    pub error: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct BatchResult {
    pub accepted: Vec<Accepted>,
    pub rejected: Vec<Rejected>,
}

impl Manager {
    /// Validation is all-or-nothing; operational admission errors are per-entry.
    /// Finish the admitted request even if its HTTP connection disappears. Never
    /// replay a batch automatically after an ambiguous transport failure.
    pub async fn add_batch(
        &self,
        urls: Vec<String>,
        proxy_url: Option<ProxyUrl>,
    ) -> Result<BatchResult> {
        let urls = input::validate_urls(urls)?;
        let manager = self.clone();
        tokio::spawn(async move {
            let mut result = BatchResult::default();
            for (index, url) in urls.into_iter().enumerate() {
                let index = index + 1;
                let options = TransferOptions {
                    proxy_url: proxy_url.clone(),
                    ..Default::default()
                };
                match manager.add_with_options(&url, None, options).await {
                    Ok(download) => result.accepted.push(Accepted { index, download }),
                    Err(error) => {
                        let error = error.to_string();
                        manager.publish(
                            EventKind::Warning {
                                message: format!("batch entry {index} rejected: {error}"),
                            },
                            None,
                        );
                        result.rejected.push(Rejected { index, error });
                    }
                }
            }
            result
        })
        .await
        .map_err(|error| Error::Worker(error.to_string()))
    }
}
