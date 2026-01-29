//! PKCE (Proof Key for Code Exchange) helpers for OAuth 2.0 device flow.

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rand::RngCore;
use reqwest::Client;
use sha2::{Digest, Sha256};

use super::{
    domain::{
        DeviceAuthorizationRequest, DeviceAuthorizationResponse, DeviceTokenPollingResponse,
        DeviceTokenResponse,
    },
    error::AuthError,
};

/// Generate a cryptographically random code_verifier.
///
/// The verifier is 43 characters long (32 random bytes, base64url encoded).
/// This meets the RFC 7636 requirement of 43-128 characters using
/// unreserved characters [A-Za-z0-9-._~].
pub fn generate_code_verifier() -> String {
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

/// Generate code_challenge from code_verifier using S256 method.
///
/// code_challenge = BASE64URL(SHA256(code_verifier))
pub fn generate_code_challenge(verifier: &str) -> String {
    let hash = Sha256::digest(verifier.as_bytes());
    URL_SAFE_NO_PAD.encode(hash)
}

/// Device flow client for OAuth 2.0 PKCE authentication.
pub struct PkceDeviceFlowClient {
    http_client: Client,
    auth_url: String,
}

impl PkceDeviceFlowClient {
    /// Create a new DeviceFlowClient.
    ///
    /// # Arguments
    /// * `http_client` - Shared reqwest client
    /// * `auth_url` - Base URL for auth platform (e.g., `https://auth.amp.thegraph.com`)
    pub fn new(http_client: Client, auth_url: String) -> Self {
        Self {
            http_client,
            auth_url,
        }
    }

    /// Request device authorization.
    ///
    /// Generates PKCE code_verifier and code_challenge, then requests
    /// device authorization from the auth server.
    ///
    /// Returns the authorization response and the code_verifier needed for polling.
    pub async fn request_authorization(&self) -> Result<PkceDeviceAuthorizationResult, AuthError> {
        // Generate PKCE parameters
        let code_verifier = generate_code_verifier();
        let code_challenge = generate_code_challenge(&code_verifier);

        let url = format!("{}/api/v1/device/authorize", self.auth_url);
        let request_body = DeviceAuthorizationRequest::new(code_challenge);

        let response = self
            .http_client
            .post(&url)
            .json(&request_body)
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
            .map_err(AuthError::HttpError)?;

        let status = response.status();

        if !status.is_success() {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "(failed to read error body)".to_string());
            return Err(AuthError::DeviceAuthorizationError(format!(
                "HTTP {}: {}",
                status, error_text
            )));
        }

        let auth_response: DeviceAuthorizationResponse =
            response.json().await.map_err(AuthError::HttpError)?;

        Ok(PkceDeviceAuthorizationResult {
            response: auth_response,
            code_verifier,
        })
    }

    /// Poll for device token.
    ///
    /// Returns:
    /// - `Ok(Some(token))` if authentication succeeded
    /// - `Ok(None)` if still pending (authorization_pending)
    /// - `Err(DeviceTokenExpired)` if the device code expired
    /// - `Err(_)` for other errors
    pub async fn poll_for_token(
        &self,
        device_code: &str,
        code_verifier: &str,
    ) -> Result<Option<DeviceTokenResponse>, AuthError> {
        let url = format!(
            "{}/api/v1/device/token?device_code={}&code_verifier={}",
            self.auth_url, device_code, code_verifier
        );

        let response = self
            .http_client
            .get(&url)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .map_err(AuthError::HttpError)?;

        let status = response.status();

        // The server returns 400 for "authorization_pending" and "expired_token",
        // so we need to parse the JSON body even on non-success status codes.
        // Only fail early on server errors (5xx) or other unexpected statuses.
        if status.is_server_error() {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "(failed to read error body)".to_string());
            return Err(AuthError::DeviceTokenPollingError(format!(
                "HTTP {}: {}",
                status, error_text
            )));
        }

        let polling_response: DeviceTokenPollingResponse =
            response.json().await.map_err(AuthError::HttpError)?;

        match polling_response {
            DeviceTokenPollingResponse::Success(token) => Ok(Some(token)),
            DeviceTokenPollingResponse::Error(err) => {
                if err.is_pending() {
                    Ok(None)
                } else if err.is_expired() {
                    Err(AuthError::DeviceTokenExpired)
                } else {
                    Err(AuthError::DeviceTokenPollingError(format!(
                        "Unknown error: {}",
                        err.error
                    )))
                }
            }
        }
    }

    /// Open the verification URL in the user's default browser.
    pub fn open_browser(url: &str) -> Result<(), AuthError> {
        open::that(url).map_err(|e| AuthError::OpenBrowserError(e.to_string()))
    }
}

/// Result of requesting device authorization.
pub struct PkceDeviceAuthorizationResult {
    /// The authorization response from the server.
    pub response: DeviceAuthorizationResponse,
    /// The code verifier needed for token polling.
    pub code_verifier: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_code_verifier_returns_43_char_string() {
        //* When
        let verifier = generate_code_verifier();

        //* Then
        assert_eq!(
            verifier.len(),
            43,
            "32 bytes base64url encoded should be 43 characters"
        );
    }

    #[test]
    fn generate_code_verifier_produces_unique_values() {
        //* When
        let v1 = generate_code_verifier();
        let v2 = generate_code_verifier();

        //* Then
        assert_ne!(v1, v2, "consecutive verifiers should be unique");
    }

    #[test]
    fn generate_code_challenge_with_same_input_returns_same_output() {
        //* Given
        let verifier = "test_verifier_string";

        //* When
        let c1 = generate_code_challenge(verifier);
        let c2 = generate_code_challenge(verifier);

        //* Then
        assert_eq!(
            c1, c2,
            "challenge should be deterministic for same verifier"
        );
    }

    #[test]
    fn generate_code_challenge_returns_valid_base64url_format() {
        //* Given
        let verifier = generate_code_verifier();

        //* When
        let challenge = generate_code_challenge(&verifier);

        //* Then
        assert_eq!(
            challenge.len(),
            43,
            "SHA256 (32 bytes) base64url encoded should be 43 characters"
        );
        assert!(
            challenge
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
            "challenge should only contain URL-safe base64 characters"
        );
    }
}
