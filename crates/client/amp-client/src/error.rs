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
    #[error("invalid batch: {0}")]
    InvalidBatch(String),
    /// Protocol invariant violation (validation at stream boundary)
    #[error("protocol invariant violation: {0}")]
    ProtocolInvariantViolation(String),
    /// Partial reorg detected (reorg point inside watermark range).
    #[error("partial reorg")]
    PartialReorg,
    /// Unrecoverable reorg (all buffered watermarks affected)
    #[error("unrecoverable reorg")]
    UnrecoverableReorg,
}

impl From<tonic::Status> for Error {
    fn from(value: tonic::Status) -> Self {
        Self::from(Box::new(value))
    }
}
