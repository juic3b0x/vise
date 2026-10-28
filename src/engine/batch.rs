//! Batch admission reuses the single-task path, journal, and worker semaphore.
use super::{Manager, TransferOptions};
use crate::{
    checksum::ExpectedChecksums,
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
        self.add_batch_with_options(
            urls,
            TransferOptions {
                proxy_url,
                ..Default::default()
            },
        )
        .await
    }

    /// Preflight request credentials for the complete admission before changing state.
    pub async fn validate_request_batch(
        &self,
        urls: &[String],
        request: &crate::net::credentials::RequestOverrides,
    ) -> Result<()> {
        if let Some(context) = request.resolve(&self.0.config.request)? {
            context.validate_urls(urls)?;
            self.0.database.check_credentials_permissions().await?;
        }
        Ok(())
    }

    pub async fn add_batch_with_options(
        &self,
        urls: Vec<String>,
        options: TransferOptions,
    ) -> Result<BatchResult> {
        let urls = input::validate_urls(urls)?;
        ExpectedChecksums::new(
            options.expected.sha256.as_deref(),
            options.expected.md5.as_deref(),
        )?;
        self.validate_request_batch(&urls, &options.request).await?;
        let manager = self.clone();
        tokio::spawn(async move {
            let mut result = BatchResult::default();
            for (index, url) in urls.into_iter().enumerate() {
                let index = index + 1;
                let options = options.clone();
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
