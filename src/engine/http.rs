use super::{Manager, Metadata};
use crate::net::Transport;
use crate::{
    checksum::{self, ExpectedChecksums},
    error::{Error, Result},
};
use reqwest::{Response, StatusCode, header};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy)]
pub(super) enum InitialRange {
    Probe,
    Whole,
    FreshPrefix,
}
impl InitialRange {
    fn header(self) -> &'static str {
        match self {
            Self::Probe => "bytes=0-0",
            Self::FreshPrefix => "bytes=0-1048575",
            Self::Whole => "bytes=0-",
        }
    }
    fn end(self, total: u64) -> u64 {
        match self {
            Self::Probe => 0,
            Self::FreshPrefix => (super::layout::MIN_CHUNK_SIZE - 1).min(total - 1),
            Self::Whole => total - 1,
        }
    }
}

impl Manager {
    pub(super) async fn probe(
        &self,
        client: &Transport,
        url: &str,
        cancel: &CancellationToken,
    ) -> Result<Metadata> {
        self.initial_response(client, url, cancel, InitialRange::Probe)
            .await
            .map(|(metadata, _)| metadata)
    }

    /// Fresh downloads retain a complete body or validated prefix; resumes probe identity.
    pub(super) async fn initial_response(
        &self,
        client: &Transport,
        url: &str,
        cancel: &CancellationToken,
        mut range: InitialRange,
    ) -> Result<(Metadata, Option<Response>)> {
        let mut attempt = 0;
        let mut fallback_expected = ExpectedChecksums::default();
        loop {
            let result = self.initial_once(client, url, cancel, range).await;
            match result {
                Ok((mut metadata, response)) => {
                    if matches!(range, InitialRange::FreshPrefix)
                        && response.is_none()
                        && metadata
                            .total
                            .is_some_and(|total| total > super::layout::MIN_CHUNK_SIZE)
                    {
                        fallback_expected = metadata.expected;
                        range = InitialRange::Whole;
                        continue;
                    }
                    metadata.expected.sha256 =
                        fallback_expected.sha256.or(metadata.expected.sha256);
                    metadata.expected.md5 = fallback_expected.md5.or(metadata.expected.md5);
                    return Ok((metadata, response));
                }
                Err(error) if error.retryable() && attempt < self.0.config.retries => {
                    self.backoff(attempt, &error, cancel).await?;
                    attempt += 1;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Validate headers before handing off an owned body. A missing body on a
    /// large fresh prefix means a bounded, fully consumed single-stream fallback.
    async fn initial_once(
        &self,
        client: &Transport,
        url: &str,
        cancel: &CancellationToken,
        range: InitialRange,
    ) -> Result<(Metadata, Option<Response>)> {
        let response = self
            .request(client, url, Some(range.header()), None, cancel)
            .await?;
        let status = response.status();
        let metadata = initial_metadata(&response, range)?;
        if matches!(range, InitialRange::Probe) && status == StatusCode::PARTIAL_CONTENT {
            consume_probe(response, cancel, self.0.config.read_timeout).await?;
            Ok((metadata, None))
        } else {
            let complete = status == StatusCode::OK
                || (status == StatusCode::PARTIAL_CONTENT
                    && content_range(&response)?.1 + 1 == metadata.total.unwrap_or(0));
            let retained = !matches!(range, InitialRange::Probe) && (complete || metadata.ranged);
            if !retained
                && matches!(range, InitialRange::FreshPrefix)
                && status == StatusCode::PARTIAL_CONTENT
            {
                // No identity: never splice the prefix with another
                // request. Drain only this bounded body for pool reuse,
                // charging its bytes, then restart as one whole stream.
                consume_initial_body(
                    response,
                    super::layout::MIN_CHUNK_SIZE,
                    cancel,
                    self.0.config.read_timeout,
                    Some(&self.0.rate_limiter),
                )
                .await?;
                return Ok((metadata, None));
            }
            Ok((metadata, retained.then_some(response)))
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

fn initial_metadata(response: &Response, range: InitialRange) -> Result<Metadata> {
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
            if start != 0 || end != range.end(total) {
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
    response: Response,
    cancel: &CancellationToken,
    timeout: Duration,
) -> Result<()> {
    consume_initial_body(response, 1, cancel, timeout, None).await
}

async fn consume_initial_body(
    mut response: Response,
    length: u64,
    cancel: &CancellationToken,
    timeout: Duration,
    limiter: Option<&crate::rate_limiter::RateLimiter>,
) -> Result<()> {
    if response
        .content_length()
        .is_some_and(|actual| actual != length)
    {
        return Err(Error::Protocol(
            "initial range body length does not match its bounds".into(),
        ));
    }
    let receive = async {
        let mut received = 0;
        while let Some(bytes) = response.chunk().await.map_err(Error::private_transport)? {
            if bytes.len() as u64 > length - received {
                return Err(Error::Protocol(
                    "initial range body exceeds its bounds".into(),
                ));
            }
            if let Some(limiter) = limiter {
                let mut remaining = bytes.len();
                while remaining > 0 {
                    remaining -= limiter.acquire(remaining, cancel).await?;
                }
            }
            received += bytes.len() as u64;
        }
        if received != length {
            return Err(Error::Protocol(
                "unexpected end of response probe body".into(),
            ));
        }
        Ok(())
    };
    tokio::select! { biased;
        _ = cancel.cancelled() => Err(Error::Cancelled),
        result = async {
            if limiter.is_some() {
                // Reqwest still enforces read_timeout on network reads. An
                // application bandwidth wait must not exhaust a probe deadline.
                receive.await
            } else {
                tokio::time::timeout(timeout, receive).await.unwrap_or(Err(Error::PrivateHttp {
                    kind: "probe body timeout", retryable: true,
                }))
            }
        } => result,
    }
}

#[cfg(test)]
mod tests;
