//! Error types for the Amp client

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("{0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("{0}")]
    Arrow(#[from] common::arrow::error::ArrowError),
    #[error("gRPC status error: {0}")]
    Status(#[from] Box<tonic::Status>),
    #[error("server error: {0}")]
    Server(String),
    #[error("JSON deserialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("state store error: {0}")]
    Store(String),
}

impl From<tonic::Status> for Error {
    fn from(value: tonic::Status) -> Self {
        Self::from(Box::new(value))
    }
}

impl Error {
    /// Check if this error is retryable (transient issues that may resolve).
    pub fn is_retryable(&self) -> bool {
        match self {
            // Network/connection failures are retryable
            Error::Transport(_) => true,
            // Retry transient gRPC errors
            Error::Status(status) => {
                matches!(
                    status.code(),
                    tonic::Code::Unavailable | tonic::Code::Internal | tonic::Code::Unknown
                )
            }
            // Protocol violations and data errors are not retryable
            Error::Server(_) | Error::Arrow(_) | Error::Json(_) | Error::Store(_) => false,
        }
    }
}

/// Result type alias for amp-client operations.
#[allow(dead_code)]
pub type Result<T> = std::result::Result<T, Error>;
