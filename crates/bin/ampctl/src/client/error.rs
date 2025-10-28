//! Common error types for the admin API client.

/// Standard error response structure from the admin API.
///
/// Used for 4XX/5XX status codes.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct ErrorResponse {
    /// Error code identifier
    pub error_code: String,
    /// Human-readable error message
    pub error_message: String,
}

/// API error that can be used as an error source.
///
/// Wraps the error code and message from the API response.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{error_code}: {error_message}")]
pub struct ApiError {
    /// Error code identifier
    pub error_code: String,
    /// Human-readable error message
    pub error_message: String,
}

impl From<ErrorResponse> for ApiError {
    fn from(resp: ErrorResponse) -> Self {
        Self {
            error_code: resp.error_code,
            error_message: resp.error_message,
        }
    }
}
