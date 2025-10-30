//! HTTP client for the Amp admin API.
//!
//! This module provides a typed HTTP client for interacting with the Amp admin API.
//! The client is organized by resource, with submodules for each API endpoint group.

pub mod auth;
pub mod datasets;
pub mod error;
pub mod jobs;
pub mod manifests;
pub mod providers;

use url::Url;

pub use self::{auth::BearerToken, error::ApiError};

/// Create a builder for constructing a client with custom configuration.
///
/// Use this when you need to configure authentication, custom HTTP clients,
/// or other advanced options.
pub fn build(base_url: Url) -> ClientBuilder {
    ClientBuilder::new(base_url)
}

/// HTTP client for the Amp admin API.
///
/// Wraps a reqwest client and provides typed methods for interacting with
/// the admin API endpoints.
#[derive(Debug, Clone)]
pub struct Client {
    /// The underlying HTTP client
    http: reqwest::Client,
    /// Base URL for the admin API
    base_url: Url,
}

impl Client {
    /// Create a new client with the given base URL and no authentication.
    ///
    /// For clients with authentication or custom configuration, use [`build`].
    pub fn new(mut base_url: Url) -> Self {
        // Ensure that no path segments are dropped when joining on this URL.
        if !base_url.path().ends_with('/') {
            base_url = format!("{base_url}/").parse().unwrap();
        }

        Self {
            http: reqwest::Client::new(),
            base_url,
        }
    }

    /// Get a reference to the base URL.
    pub fn base_url(&self) -> &Url {
        &self.base_url
    }

    /// Get a reference to the underlying HTTP client.
    pub fn http(&self) -> &reqwest::Client {
        &self.http
    }

    /// Get a provider client for provider-related operations.
    pub fn providers(&self) -> providers::ProvidersClient<'_> {
        providers::ProvidersClient::new(self)
    }

    /// Get a dataset client for dataset-related operations.
    pub fn datasets(&self) -> datasets::DatasetsClient<'_> {
        datasets::DatasetsClient::new(self)
    }

    /// Get a manifest client for manifest-related operations.
    pub fn manifests(&self) -> manifests::ManifestsClient<'_> {
        manifests::ManifestsClient::new(self)
    }

    /// Get a jobs client for jobs-related operations.
    pub fn jobs(&self) -> jobs::JobsClient<'_> {
        jobs::JobsClient::new(self)
    }
}

/// Builder for constructing a [`Client`] with custom configuration.
///
/// Created via [`build`].
#[derive(Debug)]
pub struct ClientBuilder {
    base_url: Url,
    bearer_token: Option<BearerToken>,
    http_client: Option<reqwest::Client>,
}

impl ClientBuilder {
    /// Create a new builder with the required base URL.
    pub fn new(mut base_url: Url) -> Self {
        // Ensure that no path segments are dropped when joining on this URL.
        if !base_url.path().ends_with('/') {
            base_url = format!("{base_url}/").parse().unwrap();
        }

        Self {
            base_url,
            bearer_token: None,
            http_client: None,
        }
    }

    /// Set a bearer token for authentication.
    ///
    /// The token will be included in all requests as an `Authorization: Bearer <token>` header.
    /// The token must be pre-validated using [`BearerToken::try_from`].
    pub fn with_bearer_token(mut self, token: BearerToken) -> Self {
        self.bearer_token = Some(token);
        self
    }

    /// Use a custom reqwest HTTP client.
    ///
    /// This is useful for advanced configuration like custom timeouts, connection pools, etc.
    ///
    /// **Note**: This cannot be used together with [`with_bearer_token`](Self::with_bearer_token).
    /// If you need both custom client configuration and bearer auth, configure the
    /// auth headers on the reqwest client before passing it here.
    pub fn with_http_client(mut self, client: reqwest::Client) -> Self {
        self.http_client = Some(client);
        self
    }

    /// Build the [`Client`].
    ///
    /// Returns an error if the HTTP client cannot be built.
    ///
    /// # Panics
    ///
    /// Panics in debug mode if both `with_bearer_token` and `with_http_client` are set.
    /// In release mode, the HTTP client takes precedence and the bearer token is ignored.
    pub fn build(self) -> Result<Client, BuildError> {
        debug_assert!(
            !(self.bearer_token.is_some() && self.http_client.is_some()),
            "cannot use both with_bearer_token and with_http_client; configure auth on the reqwest client directly"
        );

        // Build the HTTP client
        let http = if let Some(client) = self.http_client {
            // Use the provided client (takes precedence)
            client
        } else if let Some(token) = self.bearer_token {
            // Build a client with bearer auth
            use reqwest::header::{AUTHORIZATION, HeaderMap};

            let mut headers = HeaderMap::new();
            let auth_value = token.as_header_value();
            headers.insert(AUTHORIZATION, auth_value);

            reqwest::Client::builder()
                .default_headers(headers)
                .build()
                .map_err(BuildError::HttpClientConstruction)?
        } else {
            // Use a default client
            reqwest::Client::new()
        };

        Ok(Client {
            http,
            base_url: self.base_url,
        })
    }
}

/// Errors that can occur when building a [`Client`].
#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    /// Failed to construct HTTP client
    #[error("failed to construct HTTP client")]
    HttpClientConstruction(#[source] reqwest::Error),
}
