use axum::http::StatusCode;
use common::BoxError;
use http_common::RequestError;

/// Registry handler errors
///
/// Unified error type for all registry handlers.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid manifest JSON
    #[error("invalid manifest: {source}")]
    InvalidManifest { source: BoxError },

    /// Dataset already exists
    #[error("dataset '{name}' version '{version}' already exists")]
    DatasetAlreadyExists { name: String, version: String },

    /// Dataset store error
    #[error("dataset store error: {0}")]
    DatasetStoreError(String),

    /// Metadata DB error
    #[error("metadata db error: {0}")]
    MetadataDbError(#[from] metadata_db::Error),

    /// Serialization error
    #[error("serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
}

impl RequestError for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidManifest { .. } => StatusCode::BAD_REQUEST,
            Error::DatasetAlreadyExists { .. } => StatusCode::CONFLICT,
            Error::DatasetStoreError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::MetadataDbError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::SerializationError(_) => StatusCode::BAD_REQUEST,
        }
    }

    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidManifest { .. } => "INVALID_MANIFEST",
            Error::DatasetAlreadyExists { .. } => "DATASET_ALREADY_EXISTS",
            Error::DatasetStoreError(_) => "DATASET_STORE_ERROR",
            Error::MetadataDbError(_) => "METADATA_DB_ERROR",
            Error::SerializationError(_) => "SERIALIZATION_ERROR",
        }
    }
}
