//! Error handling types for HTTP handlers

use axum::{Json, http::StatusCode};

/// Standard error response returned by the API
///
/// This struct represents error information returned in HTTP error responses.
/// It provides structured error details including a machine-readable error code
/// and human-readable message.
///
/// ## Error Code Conventions
/// - Error codes use SCREAMING_SNAKE_CASE (e.g., `DATASET_NOT_FOUND`)
/// - Codes are stable and can be relied upon programmatically
/// - Messages may change and should only be used for display/logging
///
/// ## Example JSON Response
/// ```json
/// {
///   "error_code": "DATASET_NOT_FOUND",
///   "error_message": "dataset 'eth_mainnet' version '1.0.0' not found"
/// }
/// ```
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct ErrorResponse {
    /// HTTP status code for this error
    ///
    /// Not serialized to JSON - used internally for response construction
    #[serde(skip)]
    pub status_code: StatusCode,

    /// Machine-readable error code in SCREAMING_SNAKE_CASE format
    ///
    /// Error codes are stable across API versions and should be used
    /// for programmatic error handling. Examples: `INVALID_SELECTOR`,
    /// `DATASET_NOT_FOUND`, `METADATA_DB_ERROR`
    pub error_code: String,

    /// Human-readable error message
    ///
    /// Messages provide detailed context about the error but may change
    /// over time. Use `error_code` for programmatic decisions.
    pub error_message: String,
}

/// Trait for error types that can be converted to HTTP error responses
///
/// This trait must be implemented by all handler-specific error enums to enable
/// automatic conversion into `ErrorResponse`.
pub trait IntoErrorResponse: std::fmt::Display + Send + Sync + 'static {
    /// Returns a stable, machine-readable error code
    ///
    /// Error codes should use SCREAMING_SNAKE_CASE and remain stable across versions.
    fn error_code(&self) -> &'static str;

    /// Returns the HTTP status code for this error
    fn status_code(&self) -> StatusCode;
}

impl<E> From<E> for ErrorResponse
where
    E: IntoErrorResponse,
{
    fn from(error: E) -> Self {
        ErrorResponse {
            status_code: error.status_code(),
            error_code: error.error_code().to_string(),
            error_message: error.to_string(),
        }
    }
}

impl std::fmt::Display for ErrorResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error_message.fmt(f)
    }
}

impl axum::response::IntoResponse for ErrorResponse {
    fn into_response(self) -> axum::response::Response {
        (self.status_code, Json(self)).into_response()
    }
}
