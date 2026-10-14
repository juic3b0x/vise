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
    #[error("server returned HTTP {status}")]
    Remote {
        status: StatusCode,
        retry_after: Option<u64>,
    },
    #[error("invalid server response: {0}")]
    Protocol(String),
    #[error("state file error: {0}")]
    State(#[from] serde_json::Error),
    #[error("download interrupted")]
    Cancelled,
    #[error("worker failed: {0}")]
    Worker(String),
}

impl Error {
    pub(crate) fn retryable(&self) -> bool {
        match self {
            Self::Http(e) => {
                e.is_timeout() || e.is_connect() || e.is_body() || e.is_request() || e.is_decode()
            }
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
