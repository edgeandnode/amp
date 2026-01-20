//! Auth error types.

use thiserror::Error;

/// Errors that can occur during authentication operations.
#[derive(Debug, Error)]
pub enum AuthError {
    // Token errors
    #[error("Token expired")]
    TokenExpired,

    // Refresh errors
    #[error("Refresh failed: {0}")]
    RefreshError(String),
    #[error("User ID mismatch: expected {expected}, got {received}")]
    UserMismatch { expected: String, received: String },

    // Device flow errors
    #[error("Device authorization failed: {0}")]
    DeviceAuthorizationError(String),
    #[error("Device token polling failed: {0}")]
    DeviceTokenPollingError(String),
    #[error("Device token expired - please restart authentication")]
    DeviceTokenExpired,
    #[error("Failed to open browser: {0}")]
    OpenBrowserError(String),

    // General errors
    #[error("Rate limited, retry after {retry_after} seconds")]
    RateLimited { retry_after: u64 },
    #[error("HTTP request failed: {0}")]
    HttpError(#[from] reqwest::Error),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}
