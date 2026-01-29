//! Auth client for token refresh.

use chrono::Utc;
use reqwest::Client;

use super::{
    domain::{AuthStorage, RefreshTokenRequest, RefreshTokenResponse},
    error::AuthError,
};

/// Time before expiry to trigger refresh (5 minutes).
const REFRESH_THRESHOLD_SECS: i64 = 5 * 60;

/// Authentication client for refreshing credentials.
pub struct AuthClient {
    /// HTTP client for API requests
    http_client: Client,
    /// Auth web app base URL (for refresh endpoint)
    auth_url: String,
}

impl AuthClient {
    /// Create a new AuthClient.
    ///
    /// # Arguments
    /// * `http_client` - Shared reqwest client
    /// * `auth_url` - Base URL for auth web app (e.g., `https://auth.amp.thegraph.com/api/v1/auth`)
    pub fn new(http_client: Client, auth_url: String) -> Self {
        Self {
            http_client,
            auth_url,
        }
    }

    /// Check if the auth token needs to be refreshed.
    ///
    /// Returns true if:
    /// - No expiry is set (refresh to be safe)
    /// - Token is expired
    /// - Token expires within 5 minutes
    pub fn needs_refresh(auth: &AuthStorage) -> bool {
        match auth.expiry {
            None => true,
            Some(expiry) => {
                let now = Utc::now().timestamp();
                expiry - now <= REFRESH_THRESHOLD_SECS
            }
        }
    }

    /// Refresh an expired or expiring access token.
    ///
    /// Makes a POST request to the auth web app's refresh endpoint.
    pub async fn refresh_token(&self, auth: &AuthStorage) -> Result<AuthStorage, AuthError> {
        let url = format!("{}/refresh", self.auth_url);
        let request_body = RefreshTokenRequest::from_auth(auth);

        let response = self
            .http_client
            .post(&url)
            .bearer_auth(&auth.access_token)
            .json(&request_body)
            .timeout(std::time::Duration::from_secs(15))
            .send()
            .await
            .map_err(AuthError::HttpError)?;

        let status = response.status();

        // Handle error responses
        if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
            return Err(AuthError::TokenExpired);
        }

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after = response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse().ok())
                .unwrap_or(60);
            return Err(AuthError::RateLimited { retry_after });
        }

        if !status.is_success() {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "(failed to read error body)".to_string());
            return Err(AuthError::RefreshError(format!(
                "HTTP {}: {}",
                status, error_text
            )));
        }

        let refresh_response: RefreshTokenResponse =
            response.json().await.map_err(AuthError::HttpError)?;

        // Validate user ID matches
        if refresh_response.user.id != auth.user_id {
            return Err(AuthError::UserMismatch {
                expected: auth.user_id.clone(),
                received: refresh_response.user.id,
            });
        }

        // Calculate new expiry
        let now = Utc::now().timestamp();
        let expiry = now + refresh_response.expires_in;

        // Build updated auth storage
        Ok(AuthStorage {
            access_token: refresh_response.token,
            refresh_token: refresh_response
                .refresh_token
                .unwrap_or_else(|| auth.refresh_token.clone()),
            user_id: refresh_response.user.id,
            accounts: refresh_response.user.accounts,
            expiry: Some(expiry),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_auth_storage(expiry: Option<i64>) -> AuthStorage {
        AuthStorage {
            access_token: "test_access_token".to_string(),
            refresh_token: "test_refresh_token".to_string(),
            user_id: "test_user_id".to_string(),
            accounts: Some(vec![
                "0x1234567890123456789012345678901234567890".to_string(),
            ]),
            expiry,
        }
    }

    #[test]
    fn test_needs_refresh_no_expiry() {
        // No expiry set should trigger refresh
        let auth = make_auth_storage(None);
        assert!(AuthClient::needs_refresh(&auth));
    }

    #[test]
    fn test_needs_refresh_expired() {
        // Token that expired 1 hour ago
        let now = Utc::now().timestamp();
        let auth = make_auth_storage(Some(now - 3600));
        assert!(AuthClient::needs_refresh(&auth));
    }

    #[test]
    fn test_needs_refresh_expiring_soon() {
        // Token expiring in 2 minutes (within 5 minute threshold)
        let now = Utc::now().timestamp();
        let auth = make_auth_storage(Some(now + 120));
        assert!(AuthClient::needs_refresh(&auth));
    }

    #[test]
    fn test_needs_refresh_at_threshold() {
        // Token expiring in exactly 5 minutes (at threshold boundary)
        let now = Utc::now().timestamp();
        let auth = make_auth_storage(Some(now + REFRESH_THRESHOLD_SECS));
        assert!(AuthClient::needs_refresh(&auth));
    }

    #[test]
    fn test_needs_refresh_valid_token() {
        // Token expiring in 1 hour (well beyond threshold)
        let now = Utc::now().timestamp();
        let auth = make_auth_storage(Some(now + 3600));
        assert!(!AuthClient::needs_refresh(&auth));
    }

    #[test]
    fn test_needs_refresh_just_beyond_threshold() {
        // Token expiring in 5 minutes + 1 second (just beyond threshold)
        let now = Utc::now().timestamp();
        let auth = make_auth_storage(Some(now + REFRESH_THRESHOLD_SECS + 1));
        assert!(!AuthClient::needs_refresh(&auth));
    }
}
