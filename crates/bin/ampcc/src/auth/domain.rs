//! Auth domain for cli authentication with PKCE device flow

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Default storage path relative to home directory.
const AUTH_STORAGE_PATH: &str = ".amp/cache/amp_cli_auth";

/// Auth credentials stored on disk at ~/.amp/cache/amp_cli_auth
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthStorage {
    /// JWT access token for authenticated API requests.
    pub access_token: String,
    /// Token for obtaining new access tokens when expired.
    pub refresh_token: String,
    /// The authenticated user's unique identifier.
    pub user_id: String,
    /// Optional list of accounts (can be user IDs or wallet addresses)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accounts: Option<Vec<String>>,
    /// Token expiry as Unix timestamp (seconds since epoch)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiry: Option<i64>,
}

impl AuthStorage {
    /// Get the path to the auth storage file.
    fn storage_path() -> Option<PathBuf> {
        dirs::home_dir().map(|home| home.join(AUTH_STORAGE_PATH))
    }

    /// Load auth credentials from disk.
    ///
    /// Returns `None` if the file doesn't exist or can't be parsed.
    pub fn load() -> Option<Self> {
        let path = Self::storage_path()?;
        let contents = std::fs::read_to_string(&path).ok()?;
        serde_json::from_str(&contents).ok()
    }

    /// Save auth credentials to disk.
    ///
    /// Creates the parent directory if it doesn't exist.
    /// Sets file permissions to 0600 (owner read/write only) on Unix.
    pub fn save(&self) -> std::io::Result<()> {
        let path = Self::storage_path()
            .ok_or_else(|| std::io::Error::other("Could not determine home directory"))?;

        // Create parent directory if needed
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let contents =
            serde_json::to_string_pretty(self).map_err(|e| std::io::Error::other(e.to_string()))?;

        std::fs::write(&path, &contents)?;

        // Set restrictive permissions on Unix (owner read/write only)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let permissions = std::fs::Permissions::from_mode(0o600);
            std::fs::set_permissions(&path, permissions)?;
        }

        Ok(())
    }

    /// Clear auth credentials from disk.
    ///
    /// Returns `Ok(())` if the file was deleted or didn't exist.
    pub fn clear() -> std::io::Result<()> {
        let Some(path) = Self::storage_path() else {
            return Ok(());
        };

        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }
}

/// Request body for POST /refresh endpoint.
#[derive(Debug, Serialize)]
pub struct RefreshTokenRequest {
    pub user_id: String,
    pub refresh_token: String,
}

impl RefreshTokenRequest {
    /// Create a refresh token request from cached auth storage.
    pub fn from_auth(auth: &AuthStorage) -> Self {
        Self {
            user_id: auth.user_id.clone(),
            refresh_token: auth.refresh_token.clone(),
        }
    }
}

/// Response from POST /refresh endpoint.
#[derive(Debug, Deserialize)]
pub struct RefreshTokenResponse {
    /// New access token
    pub token: String,
    /// New refresh token (if rotated)
    pub refresh_token: Option<String>,
    /// Token lifetime in seconds
    pub expires_in: i64,
    /// User information
    pub user: RefreshTokenUser,
}

/// User info returned in refresh token response.
#[derive(Debug, Deserialize)]
pub struct RefreshTokenUser {
    pub id: String,
    pub accounts: Option<Vec<String>>,
}

// ============================================================================
// Device Flow Types
// ============================================================================

/// Request body for POST /api/v1/device/authorize endpoint.
#[derive(Debug, Serialize)]
pub struct DeviceAuthorizationRequest {
    pub code_challenge: String,
    pub code_challenge_method: String,
}

impl DeviceAuthorizationRequest {
    /// Create a new device authorization request with S256 challenge method.
    pub fn new(code_challenge: String) -> Self {
        Self {
            code_challenge,
            code_challenge_method: "S256".to_string(),
        }
    }
}

/// Response from POST /api/v1/device/authorize endpoint.
#[derive(Debug, Deserialize)]
pub struct DeviceAuthorizationResponse {
    /// Device verification code used for polling.
    pub device_code: String,
    /// User code to display for manual entry.
    pub user_code: String,
    /// URL where user enters the code.
    pub verification_uri: String,
    /// Time in seconds until device code expires.
    /// Currently unused but part of the API response - could be used for countdown timer.
    #[allow(dead_code)]
    pub expires_in: i64,
    /// Minimum polling interval in seconds.
    pub interval: i64,
}

/// Success response from GET /api/v1/device/token endpoint.
#[derive(Debug, Deserialize)]
pub struct DeviceTokenResponse {
    /// The access token for authenticated requests.
    pub access_token: String,
    /// The refresh token for renewing access.
    pub refresh_token: String,
    /// The authenticated user's ID.
    pub user_id: String,
    /// List of user accounts (wallet addresses, etc.).
    pub user_accounts: Vec<String>,
    /// Seconds until the token expires from receipt.
    pub expires_in: i64,
}

impl DeviceTokenResponse {
    /// Convert to AuthStorage for persisting to disk.
    pub fn to_auth_storage(&self, expiry: i64) -> AuthStorage {
        AuthStorage {
            access_token: self.access_token.clone(),
            refresh_token: self.refresh_token.clone(),
            user_id: self.user_id.clone(),
            accounts: Some(self.user_accounts.clone()),
            expiry: Some(expiry),
        }
    }
}

/// Polling response that can be either success, pending, or expired.
///
/// NOTE: This uses `#[serde(untagged)]` which tries variants in order.
/// `Success` must come first because it has more required fields than `Error`.
/// If deserialization of `Success` fails (missing fields), it falls back to `Error`.
/// Do not reorder these variants.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum DeviceTokenPollingResponse {
    /// Successfully received tokens (must be first - has more required fields).
    Success(DeviceTokenResponse),
    /// Error response (pending or expired).
    Error(DeviceTokenErrorResponse),
}

/// Error response from GET /api/v1/device/token endpoint.
#[derive(Debug, Deserialize)]
pub struct DeviceTokenErrorResponse {
    pub error: String,
}

impl DeviceTokenErrorResponse {
    /// Check if this is an "authorization_pending" response.
    pub fn is_pending(&self) -> bool {
        self.error == "authorization_pending"
    }

    /// Check if this is an "expired_token" response.
    pub fn is_expired(&self) -> bool {
        self.error == "expired_token"
    }
}
