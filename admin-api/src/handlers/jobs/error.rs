use axum::http::StatusCode;
use common::BoxError;
use http_common::RequestError;

/// Job handler errors
///
/// Unified error type for all job handlers.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Job not found
    #[error("job '{id}' not found")]
    NotFound { id: String },

    /// Metadata DB error
    #[error("metadata db error: {0}")]
    MetadataDbError(#[from] metadata_db::Error),

    /// Invalid request
    #[error("invalid request: {0}")]
    InvalidRequest(BoxError),
}

impl RequestError for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::NotFound { .. } => "JOB_NOT_FOUND",
            Error::MetadataDbError(_) => "METADATA_DB_ERROR",
            Error::InvalidRequest(_) => "INVALID_REQUEST",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::MetadataDbError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::InvalidRequest(_) => StatusCode::BAD_REQUEST,
        }
    }
}
