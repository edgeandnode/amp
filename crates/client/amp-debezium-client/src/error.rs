use thiserror::Error;

/// Errors that can occur when working with the Debezium client.
#[derive(Error, Debug)]
pub enum Error {
    /// Error from the underlying Amp client
    #[error("Amp client error: {0}")]
    AmpClient(#[from] amp_client::Error),

    /// Error serializing record to JSON
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    /// Error with Arrow data types
    #[error("Arrow error: {0}")]
    Arrow(#[from] common::arrow::error::ArrowError),

    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(String),

    /// State store error
    #[error("State store error: {0}")]
    StateStore(String),
}

/// Result type alias for amp-debezium operations.
pub type Result<T> = std::result::Result<T, Error>;
