//! Shared failures and results for Vise's internal binary and test interfaces.
use reqwest::StatusCode;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Invalid(String),
    #[error("download not found: {0}")]
    NotFound(uuid::Uuid),
    #[error("{0}")]
    Conflict(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("HTTP transport error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("HTTP transport error ({kind})")]
    PrivateHttp { kind: &'static str, retryable: bool },
    #[error("server returned HTTP {status}")]
    Remote {
        status: StatusCode,
        retry_after: Option<u64>,
    },
    #[error("invalid server response: {0}")]
    Protocol(String),
    #[error("checksum mismatch: {0}")]
    ChecksumMismatch(String),
    #[error("state file error: {0}")]
    State(#[from] serde_json::Error),
    #[error("SQLite journal error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("download admission body exceeds its size limit")]
    PayloadTooLarge,
    #[error("download interrupted")]
    Cancelled,
    #[error(transparent)]
    Checkpoint(std::sync::Arc<Error>),
    #[error("worker failed: {0}")]
    Worker(String),
}

impl Error {
    pub(crate) fn private_transport(error: reqwest::Error) -> Self {
        let retryable = error.is_timeout()
            || error.is_connect()
            || error.is_body()
            || error.is_request()
            || error.is_decode();
        let kind = if error.is_timeout() {
            "timeout"
        } else if error.is_connect() {
            "connection failed"
        } else {
            "request or body failed"
        };
        Self::PrivateHttp { kind, retryable }
    }
    pub(crate) fn without_request_details(self) -> Self {
        match self {
            Self::Http(error) => Self::private_transport(error),
            other => other,
        }
    }
    pub(crate) fn retryable(&self) -> bool {
        match self {
            Self::Checkpoint(error) => error.retryable(),
            Self::Http(e) => {
                e.is_timeout() || e.is_connect() || e.is_body() || e.is_request() || e.is_decode()
            }
            Self::PrivateHttp { retryable, .. } => *retryable,
            Self::Remote { status, .. } => {
                status.is_server_error()
                    || *status == StatusCode::REQUEST_TIMEOUT
                    || *status == StatusCode::TOO_MANY_REQUESTS
            }
            // Truncated response bodies can surface as a clean early EOF.
            Self::Protocol(message) => message.starts_with("unexpected end of response"),
            _ => false,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::Error;
    #[test]
    fn shared_checkpoint_error_preserves_display_source_and_retry_classification() {
        let original = Error::Io(std::io::Error::other("journal failure"));
        let display = original.to_string();
        let source = std::error::Error::source(&original).map(ToString::to_string);
        let retryable = original.retryable();
        let shared = Error::Checkpoint(std::sync::Arc::new(original));
        assert_eq!(shared.to_string(), display);
        assert_eq!(
            std::error::Error::source(&shared).map(ToString::to_string),
            source,
            "shared checkpoint errors changed the source chain"
        );
        assert_eq!(shared.retryable(), retryable);
    }
}
