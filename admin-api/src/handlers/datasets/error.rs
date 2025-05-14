use axum::http::StatusCode;
use common::BoxError;
use http_common::RequestError;

/// Dataset handler errors
///
/// Unified error type for all dataset handlers.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid manifest
    ///
    /// Error originating from the manifest parsing process.
    #[error("invalid manifest: {0}")]
    InvalidManifest(#[source] serde_json::Error),

    /// Invalid dataset ID
    #[error("invalid dataset ID '{name}': {source}")]
    InvalidId { name: String, source: BoxError },

    /// Dataset not found
    #[error("dataset '{name}' not found")]
    NotFound { name: String },

    /// Dataset store error
    #[error("dataset store error: {0}")]
    StoreError(#[from] dataset_store::DatasetError),

    /// Metadata DB error
    #[error("metadata db error: {0}")]
    MetadataDbError(#[from] metadata_db::Error),

    /// Scheduler error
    #[error("scheduler error: {0}")]
    SchedulerError(#[source] BoxError),

    /// Dataset definition store write error
    #[error("dataset definition store error: {0}")]
    DatasetDefStoreError(#[from] object_store::Error),
}

impl RequestError for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidManifest(_) => StatusCode::BAD_REQUEST,
            Error::InvalidId { .. } => StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::StoreError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::MetadataDbError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::SchedulerError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::DatasetDefStoreError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidManifest(_) => "INVALID_MANIFEST",
            Error::InvalidId { .. } => "INVALID_DATASET_ID",
            Error::NotFound { .. } => "DATASET_NOT_FOUND",
            Error::StoreError(_) => "DATASET_STORE_ERROR",
            Error::MetadataDbError(_) => "METADATA_DB_ERROR",
            Error::SchedulerError(_) => "SCHEDULER_ERROR",
            Error::DatasetDefStoreError(_) => "DATASET_DEF_STORE_ERROR",
        }
    }
}
