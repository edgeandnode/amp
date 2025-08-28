use std::{num::NonZeroUsize, sync::Arc};

use anyhow::{Result, anyhow};
use axum::{
    Json, RequestPartsExt,
    extract::FromRequestParts,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use axum_extra::{TypedHeader, typed_header::TypedHeaderRejectionReason};
use base64::{Engine as _, engine::general_purpose};
use chrono::{Duration, Utc};
use headers::{Authorization, authorization::Bearer};
use jsonwebtoken::{
    Algorithm, DecodingKey, Validation, decode, decode_header,
    jwk::{AlgorithmParameters, Jwk, JwkSet},
};
use lru::LruCache;
use thiserror::Error;
use tokio::sync::RwLock;

mod models;

pub use models::{
    AuthUser, AuthenticatedUser, Claims, EthereumEmbeddedWallet, EthereumLinkedAccount,
};

#[derive(Debug)]
pub struct AuthAnyhowError(anyhow::Error);

impl From<anyhow::Error> for AuthAnyhowError {
    fn from(err: anyhow::Error) -> Self {
        Self(err)
    }
}

impl std::fmt::Display for AuthAnyhowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for AuthAnyhowError {}

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("Missing authorization header")]
    MissingAuthHeader,

    #[error("Invalid authorization header format")]
    InvalidAuthHeaderFormat,

    #[error("Bearer token is empty")]
    EmptyToken,

    #[error("Invalid token: {0}")]
    InvalidToken(String),

    #[error("Token missing 'kid' header")]
    TokenMissingKid,

    #[error("No matching key found in JWKS")]
    NoMatchingKey,

    #[error("Unsupported algorithm")]
    UnsupportedAlgorithm,

    #[error("JWKS fetch error: {0}")]
    JwksFetchError(String),

    #[error("User {0} not found in IDaaS")]
    UserNotFoundInIdaas(String),

    #[error("Auth service not configured")]
    ServiceNotConfigured,

    #[error("Internal authentication error")]
    InternalError(#[from] anyhow::Error),
}

impl AuthError {
    /// Map AuthError variants to appropriate HTTP status codes
    pub fn status_code(&self) -> StatusCode {
        match self {
            AuthError::MissingAuthHeader
            | AuthError::InvalidAuthHeaderFormat
            | AuthError::EmptyToken
            | AuthError::InvalidToken(_)
            | AuthError::TokenMissingKid
            | AuthError::NoMatchingKey
            | AuthError::UnsupportedAlgorithm => StatusCode::UNAUTHORIZED,

            AuthError::UserNotFoundInIdaas(_) => StatusCode::FORBIDDEN,

            AuthError::JwksFetchError(_)
            | AuthError::ServiceNotConfigured
            | AuthError::InternalError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Get user-facing error message (hide internal details)
    pub fn user_message(&self) -> String {
        match self {
            AuthError::UserNotFoundInIdaas(_) => "Access denied".to_string(),
            AuthError::JwksFetchError(_)
            | AuthError::ServiceNotConfigured
            | AuthError::InternalError(_) => "Authentication service error".to_string(),
            _ => self.to_string(),
        }
    }
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let message = self.user_message();

        let body = Json(serde_json::json!({
            "error": message,
        }));

        (status, body).into_response()
    }
}

impl IntoResponse for AuthAnyhowError {
    fn into_response(self) -> Response {
        // Convert anyhow::Error to AuthError for proper handling
        let auth_error = AuthError::InternalError(self.0);
        auth_error.into_response()
    }
}

#[derive(Clone)]
struct CachedJwks {
    jwks: JwkSet,
    expires_at: chrono::DateTime<Utc>,
}

#[derive(Clone)]
struct CachedUser {
    user: Option<AuthUser>,
    expires_at: chrono::DateTime<Utc>,
}

/// Default cache sizes
const DEFAULT_USER_CACHE_SIZE: usize = 1000; // Max 1000 users in cache

#[derive(Clone)]
pub struct AuthService {
    /// URL to the JWKS endpoint used to validate the bearer JWT parsed from the authorization header
    jwks_url: String,
    /// URL to fetch the IDaaS users from. Used to get the wallet address of the authenticated user from the IDaaS DID
    user_api_url: String,
    /// IDaaS app id, used to authenticate the user api requests
    app_id: String,
    /// The base64 encoded basic authorization string from the app_id and app_secret.
    /// Used to make authenticated requests to the IDaaS user api
    user_api_computed_basic_auth: String,
    cache: Arc<RwLock<Option<CachedJwks>>>,
    /// Cache for user data to avoid repeated API calls (LRU with size limit)
    user_cache: Arc<RwLock<LruCache<String, CachedUser>>>,
    cache_duration: Duration,
    validation: Validation,
    /// HTTP client for making requests
    client: reqwest::Client,
}

impl AuthService {
    pub fn new(jwks_url: String, user_api_url: String, app_id: String, app_secret: String) -> Self {
        Self::with_config(
            jwks_url,
            user_api_url,
            app_id,
            app_secret,
            Duration::hours(1),
            Validation::default(),
        )
    }

    pub fn with_config(
        jwks_url: String,
        user_api_url: String,
        app_id: String,
        app_secret: String,
        cache_duration: Duration,
        mut validation: Validation,
    ) -> Self {
        validation.validate_exp = true;
        validation.validate_nbf = true;

        let basic_auth_token =
            general_purpose::STANDARD.encode(format!("{}:{}", app_id, app_secret));

        Self {
            jwks_url,
            user_api_url,
            app_id,
            user_api_computed_basic_auth: basic_auth_token,
            cache: Arc::new(RwLock::new(None)),
            user_cache: Arc::new(RwLock::new(LruCache::new(
                NonZeroUsize::new(DEFAULT_USER_CACHE_SIZE).unwrap(),
            ))),
            cache_duration,
            validation,
            client: reqwest::Client::new(),
        }
    }

    /// Set the cache size for user data (default: 1000 users)
    pub fn with_user_cache_size(mut self, size: usize) -> Self {
        if let Some(cache_size) = NonZeroUsize::new(size) {
            self.user_cache = Arc::new(RwLock::new(LruCache::new(cache_size)));
        }
        self
    }

    pub fn with_audience(mut self, audience: Vec<String>) -> Self {
        self.validation.set_audience(&audience);
        self
    }

    pub fn with_issuer(mut self, issuers: Vec<String>) -> Self {
        self.validation.set_issuer(&issuers);
        self
    }

    async fn fetch_jwks(&self) -> Result<JwkSet> {
        let response = self
            .client
            .get(&self.jwks_url)
            .send()
            .await
            .map_err(|e| anyhow!("JWKS fetch error: {}", e))?;

        if !response.status().is_success() {
            return Err(anyhow!("HTTP {} from JWKS endpoint", response.status()));
        }

        let jwks = response
            .json::<JwkSet>()
            .await
            .map_err(|e| anyhow!("JWKS fetch error: {}", e))?;

        Ok(jwks)
    }

    async fn get_jwks(&self) -> Result<JwkSet> {
        let cache_read = self.cache.read().await;

        if let Some(cached) = &*cache_read
            && cached.expires_at > Utc::now()
        {
            return Ok(cached.jwks.clone());
        }
        drop(cache_read);

        let mut cache_write = self.cache.write().await;

        if let Some(cached) = &*cache_write
            && cached.expires_at > Utc::now()
        {
            return Ok(cached.jwks.clone());
        }

        let jwks = self.fetch_jwks().await?;

        *cache_write = Some(CachedJwks {
            jwks: jwks.clone(),
            expires_at: Utc::now() + self.cache_duration,
        });

        Ok(jwks)
    }

    fn find_key<'a>(&self, jwks: &'a JwkSet, kid: &str) -> Option<&'a Jwk> {
        jwks.keys
            .iter()
            .find(|key| key.common.key_id.as_deref() == Some(kid))
    }

    fn get_decoding_key(jwk: &Jwk) -> Result<DecodingKey, AuthError> {
        match &jwk.algorithm {
            AlgorithmParameters::EllipticCurve(ec) => {
                // Privy uses ES256 (P-256 curve)
                DecodingKey::from_ec_components(&ec.x, &ec.y)
                    .map_err(|e| AuthError::InvalidToken(e.to_string()))
            }
            _ => Err(AuthError::UnsupportedAlgorithm),
        }
    }

    pub async fn validate_token(&self, token: &str) -> Result<Claims, AuthError> {
        let header = decode_header(token).map_err(|e| AuthError::InvalidToken(e.to_string()))?;

        let kid = header.kid.ok_or(AuthError::TokenMissingKid)?;

        let jwks = self
            .get_jwks()
            .await
            .map_err(|e| AuthError::JwksFetchError(e.to_string()))?;

        let jwk = self.find_key(&jwks, &kid).ok_or(AuthError::NoMatchingKey)?;

        let decoding_key = Self::get_decoding_key(jwk)?;

        let mut validation = self.validation.clone();
        // Privy uses ES256 for signing tokens
        validation.algorithms = vec![Algorithm::ES256];

        let token_data = decode::<Claims>(token, &decoding_key, &validation)
            .map_err(|e| AuthError::InvalidToken(e.to_string()))?;

        Ok(token_data.claims)
    }

    /// Fetch the auth IDaaS user record by the id
    pub async fn maybe_fetch_auth_user(
        &self,
        user_id: &str,
    ) -> Result<Option<AuthUser>, AuthError> {
        // Check cache first (requires write lock for LRU's mutable get)
        {
            let mut cache_write = self.user_cache.write().await;
            if let Some(cached) = cache_write.get(user_id)
                && cached.expires_at > Utc::now()
            {
                return Ok(cached.user.clone());
            }
            // If expired, we'll fetch new data below
        }

        // Cache miss or expired - fetch from API
        // send a GET request to: {user_api_url}/{user_id} with the derived basic auth header and auth app id
        // ex: https://api.privy.io/v1/users/cmeizfhee0075jt0cb9sbzvxi
        let response = self
            .client
            .get(format!("{}/{}", &self.user_api_url, user_id))
            .header(
                "Authorization",
                format!("Bearer {}", &self.user_api_computed_basic_auth),
            )
            .header("Content-Type", "application/json")
            .header("privy-app-id", &self.app_id)
            .send()
            .await
            .map_err(|e| AuthError::InternalError(anyhow!("Failed to fetch user: {}", e)))?;

        let user_result = match response.status() {
            reqwest::StatusCode::OK => {
                let user = response.json::<AuthUser>().await.map_err(|e| {
                    AuthError::InternalError(anyhow!("Failed to parse user response: {}", e))
                })?;
                Some(user)
            }
            reqwest::StatusCode::NOT_FOUND => {
                // User not found - return None instead of error
                None
            }
            status => {
                // Other HTTP errors should be treated as actual errors
                let error_text = response.text().await.unwrap_or_default();
                return Err(AuthError::InternalError(anyhow!(
                    "Failed to fetch user {}: HTTP {} - {}",
                    user_id,
                    status,
                    error_text
                )));
            }
        };

        // Cache the result (including None for 404s to avoid repeated lookups)
        {
            let mut cache_write = self.user_cache.write().await;
            cache_write.put(
                user_id.to_string(),
                CachedUser {
                    user: user_result.clone(),
                    expires_at: Utc::now() + self.cache_duration,
                },
            );
        }

        Ok(user_result)
    }

    /// Clear expired entries from the user cache
    /// This can be called periodically to clean up expired entries
    pub async fn clean_expired_cache_entries(&self) {
        let mut cache_write = self.user_cache.write().await;
        let now = Utc::now();

        // Collect keys to remove (can't modify while iterating)
        let expired_keys: Vec<String> = cache_write
            .iter()
            .filter_map(|(key, value)| {
                if value.expires_at <= now {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect();

        // Remove expired entries
        for key in expired_keys {
            cache_write.pop(&key);
        }
    }

    /// Get current cache statistics
    pub async fn cache_stats(&self) -> (usize, usize) {
        let cache_read = self.user_cache.read().await;
        (cache_read.len(), cache_read.cap().into())
    }
}

impl<S> FromRequestParts<S> for AuthenticatedUser
where
    S: Send + Sync,
{
    type Rejection = AuthError;

    async fn from_request_parts(
        parts: &mut http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        let auth_service = parts
            .extensions
            .get::<AuthService>()
            .ok_or(AuthError::ServiceNotConfigured)?
            .clone();

        let TypedHeader(Authorization(bearer)) = parts
            .extract::<TypedHeader<Authorization<Bearer>>>()
            .await
            .map_err(|err| match err.reason() {
                TypedHeaderRejectionReason::Missing => AuthError::MissingAuthHeader,
                TypedHeaderRejectionReason::Error(_) => AuthError::InvalidAuthHeaderFormat,
                _ => AuthError::InvalidToken(err.to_string()),
            })?;

        if bearer.token().is_empty() {
            return Err(AuthError::EmptyToken);
        }

        let claims = auth_service.validate_token(bearer.token()).await?;

        // Extract user ID from claims and fetch user data
        let user_id = claims.user_id();

        let user = auth_service.maybe_fetch_auth_user(&user_id).await?;

        // User must exist in IDaaS for authentication to succeed
        match user {
            Some(user_data) => Ok(AuthenticatedUser {
                claims,
                user: Some(user_data),
            }),
            None => {
                // User not found in IDaaS - reject authentication
                Err(AuthError::UserNotFoundInIdaas(user_id))
            }
        }
    }
}

pub fn auth_layer(
    auth_service: AuthService,
) -> tower::util::MapRequestLayer<
    impl Fn(http::Request<axum::body::Body>) -> http::Request<axum::body::Body> + Clone,
> {
    tower::util::MapRequestLayer::new(move |mut req: http::Request<axum::body::Body>| {
        req.extensions_mut().insert(auth_service.clone());
        req
    })
}
