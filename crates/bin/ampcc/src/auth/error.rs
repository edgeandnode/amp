//! Auth error types.

use thiserror::Error;

/// Errors that can occur during authentication operations.
#[derive(Debug, Error)]
pub enum AuthError {
    /// Refresh token expired error
    ///
    /// This occurs when the refresh token stored on the users machine has past its expiry.
    /// When this occurs, the user will need to re-authenticate.
    #[error("Token expired")]
    TokenExpired,

    /// Refresh token request error
    ///
    /// This is a catch-all error that is thrown if an error occurs when hitting the refresh endpoint.
    /// Specifically if 400/5xx errors are returned in the response body.
    #[error("Refresh failed: {0}")]
    RefreshError(String),
    /// User mismatch error.
    ///
    /// This occurs when the user id returned in the refresh token response does NOT match the user
    /// id on the AuthStorage instance.
    #[error("User ID mismatch: expected {expected}, got {received}")]
    UserMismatch { expected: String, received: String },

    /// PKCE Device Authorization error.
    ///
    /// Thrown if the /device/authorize HTTP request fails and returns a non-success (200) status.
    #[error("Device authorization failed: {0}")]
    DeviceAuthorizationError(String),
    /// PKCE Device token polling Error.
    ///
    /// Thrown if the /device/token HTTP request fails and returns a non-success (200) status.
    /// This is called while polling the endpoint to determine if the user has completed authentication
    /// through the UI.
    #[error("Device token polling failed: {0}")]
    DeviceTokenPollingError(String),
    /// PKCE Device request expired.
    ///
    /// This occurs if the user takes too long to authenticate through the browser and the request
    /// cycle times out.
    /// User will need to restart device flow.
    #[error("Device token expired - please restart authentication")]
    DeviceTokenExpired,
    /// PKCE Device Flow browser open error.
    ///
    /// Thrown if an error occurs while trying to open the users browser to the auth UI.
    #[error("Failed to open browser: {0}")]
    OpenBrowserError(String),

    /// PKCE Device flow rate-limit error.
    ///
    /// This error is thrown if the user attempts to make too many refresh requests in the given
    /// time-period established by the Auth UI.
    /// Prevents DDoS of the Auth UI refresh endpoint and other potential attack vectors.
    /// User can retry after the returned retry_after.
    #[error("Rate limited, retry after {retry_after} seconds")]
    RateLimited { retry_after: u64 },
    /// Generic HTTP Error occurred.
    #[error("HTTP request failed: {0}")]
    HttpError(#[from] reqwest::Error),
    /// Generic IO Error occurred.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    /// Generic JSON Error Occurred.
    ///
    /// This would be when decoding the response JSON body from the device flow or refresh HTTP requests.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}
