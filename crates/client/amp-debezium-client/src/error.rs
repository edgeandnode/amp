use thiserror::Error;

/// Errors that can occur when working with the Debezium client.
#[derive(Error, Debug)]
pub enum Error {
    #[error("Amp client error: {0}")]
    AmpClient(#[from] amp_client::Error),
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Arrow error: {0}")]
    Arrow(#[from] common::arrow::error::ArrowError),
    #[error("UTF-8 conversion error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("State store error: {0}")]
    StateStore(String),
    #[cfg(feature = "rocksdb")]
    #[error("RocksDB error: {0}")]
    RocksDb(#[from] rocksdb::Error),
    #[cfg(feature = "rocksdb")]
    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::error::EncodeError),
    #[cfg(feature = "rocksdb")]
    #[error("Deserialization error: {0}")]
    Deserialization(#[from] bincode::error::DecodeError),
}

/// Result type alias for amp-debezium operations.
pub type Result<T> = std::result::Result<T, Error>;
