use super::{Manager, Metadata};
use crate::net::Transport;
use crate::{
    checksum::{self, ExpectedChecksums},
    error::{Error, Result},
};
use reqwest::{Response, StatusCode, header};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

impl Manager {
    pub(super) async fn probe(
        &self,
        client: &Transport,
        url: &str,
        cancel: &CancellationToken,
    ) -> Result<Metadata> {
        self.initial_response(client, url, cancel, true)
            .await
            .map(|(metadata, _)| metadata)
    }

    /// Retain a fresh C=1 response; resumed/parallel tasks still probe identity.
    pub(super) async fn initial_response(
        &self,
        client: &Transport,
        url: &str,
        cancel: &CancellationToken,
        probe: bool,
    ) -> Result<(Metadata, Option<Response>)> {
        let mut attempt = 0;
        loop {
            let result: Result<(Metadata, Option<Response>)> = async {
                let response = self
                    .request(
                        client,
                        url,
                        Some(if probe { "bytes=0-0" } else { "bytes=0-" }),
                        None,
                        cancel,
                    )
                    .await?;
                let status = response.status();
                let metadata = initial_metadata(&response, probe)?;
                if probe && status == StatusCode::PARTIAL_CONTENT {
                    consume_probe(response, cancel, self.0.config.read_timeout).await?;
                    Ok((metadata, None))
                } else {
                    let retained = !probe && status != StatusCode::RANGE_NOT_SATISFIABLE;
                    Ok((metadata, retained.then_some(response)))
                }
            }
            .await;
            match result {
                Ok(value) => return Ok(value),
                Err(error) if error.retryable() && attempt < self.0.config.retries => {
                    self.backoff(attempt, &error, cancel).await?;
                    attempt += 1;
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(super) async fn request(
        &self,
        client: &Transport,
        url: &str,
        range: Option<&str>,
        validator: Option<&str>,
        cancel: &CancellationToken,
    ) -> Result<Response> {
        client.request(url, range, validator, cancel).await
    }

    pub(super) async fn backoff(
        &self,
        attempt: u32,
        error: &Error,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let seconds = match error {
            Error::Remote {
                retry_after: Some(seconds),
                ..
            } => (*seconds).clamp(1, 60),
            _ => (1u64 << attempt.min(5)).min(30),
        };
        tokio::select! { biased; _ = cancel.cancelled() => Err(Error::Cancelled), _ = tokio::time::sleep(Duration::from_secs(seconds)) => Ok(()) }
    }
}

fn initial_metadata(response: &Response, probe: bool) -> Result<Metadata> {
    let status = response.status();
    if status == StatusCode::RANGE_NOT_SATISFIABLE
        && response
            .headers()
            .get(header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            == Some("bytes */0")
    {
        return Ok(Metadata {
            total: Some(0),
            ranged: false,
            validator: None,
            expected: ExpectedChecksums::default(),
        });
    }
    check_response(response)?;
    let expected = checksum::from_response(response)?;
    let validator = validator(response);
    match status {
        StatusCode::PARTIAL_CONTENT => {
            let (start, end, total) = content_range(response)?;
            if start != 0 || (probe && end != 0) || (!probe && end + 1 != total) {
                return Err(Error::Protocol(
                    "initial range response has unexpected bounds".into(),
                ));
            }
            // Parallel/resumed ranges need an identity validator. Without one
            // a changing resource could silently splice two versions.
            Ok(Metadata {
                total: Some(total),
                ranged: validator.is_some(),
                validator,
                expected,
            })
        }
        StatusCode::OK => Ok(Metadata {
            total: response.content_length(),
            ranged: false,
            validator,
            expected,
        }),
        _ => Err(Error::Protocol(format!(
            "unexpected initial status: {status}"
        ))),
    }
}

pub(super) fn check_response(response: &Response) -> Result<()> {
    if !response.status().is_success() {
        return Err(Error::Remote {
            status: response.status(),
            retry_after: response
                .headers()
                .get(header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse().ok()),
        });
    }
    if response
        .headers()
        .get(header::CONTENT_ENCODING)
        .is_some_and(|v| v.as_bytes() != b"identity")
    {
        return Err(Error::Protocol(
            "server returned encoded bytes despite Accept-Encoding: identity".into(),
        ));
    }
    Ok(())
}

pub(super) fn validator(response: &Response) -> Option<String> {
    response
        .headers()
        .get(header::ETAG)
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.starts_with("W/"))
        .or_else(|| {
            response
                .headers()
                .get(header::LAST_MODIFIED)
                .and_then(|v| v.to_str().ok())
        })
        .map(str::to_owned)
}

pub(super) fn content_range(response: &Response) -> Result<(u64, u64, u64)> {
    let value = response
        .headers()
        .get(header::CONTENT_RANGE)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| Error::Protocol("missing Content-Range".into()))?;
    let parsed = (|| {
        let (span, total) = value.strip_prefix("bytes ")?.split_once('/')?;
        let (start, end) = span.split_once('-')?;
        let (start, end, total) = (
            start.parse::<u64>().ok()?,
            end.parse::<u64>().ok()?,
            total.parse::<u64>().ok()?,
        );
        (start <= end && end < total).then_some((start, end, total))
    })();
    parsed.ok_or_else(|| Error::Protocol("malformed Content-Range".into()))
}

// Only a validated 206 for byte zero reaches here. Never drain an ignored Range.
async fn consume_probe(
    mut response: Response,
    cancel: &CancellationToken,
    timeout: Duration,
) -> Result<()> {
    if response.content_length().is_some_and(|length| length != 1) {
        return Err(Error::Protocol(
            "range probe body length is not one byte".into(),
        ));
    }
    let receive = async {
        let mut received = 0;
        while let Some(bytes) = response.chunk().await.map_err(Error::private_transport)? {
            if bytes.len() > 1 - received {
                return Err(Error::Protocol("range probe body exceeds one byte".into()));
            }
            received += bytes.len();
        }
        if received != 1 {
            return Err(Error::Protocol(
                "unexpected end of response probe body".into(),
            ));
        }
        Ok(())
    };
    tokio::select! { biased;
        _ = cancel.cancelled() => Err(Error::Cancelled),
        result = tokio::time::timeout(timeout, receive) => result.unwrap_or(Err(Error::PrivateHttp {
            kind: "probe body timeout", retryable: true,
        })),
    }
}

#[cfg(test)]
mod tests;
