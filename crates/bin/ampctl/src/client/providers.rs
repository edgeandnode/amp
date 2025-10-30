//! Provider management API client.
//!
//! Provides methods for interacting with the `/providers` endpoints of the admin API.

use super::{
    Client,
    error::{ApiError, ErrorResponse},
};

/// Build URL path for creating a provider.
///
/// POST `/providers`
fn provider_create() -> &'static str {
    "providers"
}

/// Build URL path for getting a provider by name.
///
/// GET `/providers/{name}`
fn provider_get_by_id(name: &str) -> String {
    format!("providers/{name}", name = urlencoding::encode(name))
}

/// Build URL path for listing all providers.
///
/// GET `/providers`
fn provider_get_all() -> &'static str {
    "providers"
}

/// Build URL path for deleting a provider by name.
///
/// DELETE `/providers/{name}`
fn provider_delete_by_id(name: &str) -> String {
    format!("providers/{name}", name = urlencoding::encode(name))
}

/// Client for provider-related API operations.
///
/// Created via [`Client::providers`](crate::client::Client::providers).
#[derive(Debug)]
pub struct ProvidersClient<'a> {
    client: &'a Client,
}

impl<'a> ProvidersClient<'a> {
    /// Create a new providers client.
    pub(crate) fn new(client: &'a Client) -> Self {
        Self { client }
    }

    /// Register a provider configuration.
    ///
    /// POSTs to `/providers` endpoint with the provider configuration as JSON.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterError`] for network errors, API errors (400/409/500),
    /// or unexpected responses.
    #[tracing::instrument(skip(self, config), fields(name = %name))]
    pub async fn register(
        &self,
        name: &str,
        config: &serde_json::Value,
    ) -> Result<(), RegisterError> {
        let url = self
            .client
            .base_url()
            .join(provider_create())
            .expect("valid URL");

        tracing::debug!("Sending provider registration request");

        // Ensure the name field is set correctly
        let mut config_obj = config
            .as_object()
            .ok_or_else(|| RegisterError::InvalidConfig)?
            .clone();

        config_obj.insert(
            "name".to_string(),
            serde_json::Value::String(name.to_string()),
        );

        let response = self
            .client
            .http()
            .post(url.as_str())
            .json(&config_obj)
            .send()
            .await
            .map_err(|err| RegisterError::Network {
                url: url.to_string(),
                source: err,
            })?;

        let status = response.status();
        tracing::debug!(status = %status, "Received API response");

        match status.as_u16() {
            201 => Ok(()),
            400 | 409 | 500 => {
                let text = response.text().await.map_err(|err| {
                    tracing::error!(status = %status, error = %err, "Failed to read error response");
                    RegisterError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to read error response: {}", err),
                    }
                })?;

                let error_response: ErrorResponse = serde_json::from_str(&text).map_err(|err| {
                    tracing::error!(status = %status, error = %err, "Failed to parse error response");
                    RegisterError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: text.clone(),
                    }
                })?;

                match error_response.error_code.as_str() {
                    "INVALID_REQUEST_BODY" => {
                        Err(RegisterError::InvalidRequestBody(error_response.into()))
                    }
                    "DATA_CONVERSION_ERROR" => {
                        Err(RegisterError::DataConversionError(error_response.into()))
                    }
                    "PROVIDER_CONFLICT" => Err(RegisterError::Conflict(error_response.into())),
                    "STORE_ERROR" => Err(RegisterError::StoreError(error_response.into())),
                    _ => Err(RegisterError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: text,
                    }),
                }
            }
            _ => {
                let text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| String::from("Failed to read response body"));
                Err(RegisterError::UnexpectedResponse {
                    status: status.as_u16(),
                    message: text,
                })
            }
        }
    }

    /// Get a provider configuration by name.
    ///
    /// GETs from `/providers/{name}` endpoint.
    ///
    /// Returns `None` if the provider does not exist (404).
    ///
    /// # Errors
    ///
    /// Returns [`GetError`] for network errors, API errors (400/500),
    /// or unexpected responses.
    #[tracing::instrument(skip(self), fields(name = %name))]
    pub async fn get(&self, name: &str) -> Result<Option<serde_json::Value>, GetError> {
        let url = self
            .client
            .base_url()
            .join(&provider_get_by_id(name))
            .expect("valid URL");

        tracing::debug!("Sending GET request");

        let response = self
            .client
            .http()
            .get(url.as_str())
            .send()
            .await
            .map_err(|err| GetError::Network {
                url: url.to_string(),
                source: err,
            })?;

        let status = response.status();
        tracing::debug!(status = %status, "Received API response");

        match status.as_u16() {
            200 => {
                let provider: serde_json::Value = response.json().await.map_err(|err| {
                    tracing::error!(error = %err, "Failed to parse provider response");
                    GetError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to parse response: {}", err),
                    }
                })?;
                Ok(Some(provider))
            }
            404 => {
                tracing::debug!("Provider not found");
                Ok(None)
            }
            400 | 500 => {
                let text = response.text().await.map_err(|err| {
                    tracing::error!(status = %status, error = %err, "Failed to read error response");
                    GetError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to read error response: {}", err),
                    }
                })?;

                let error_response: ErrorResponse = serde_json::from_str(&text).map_err(|err| {
                    tracing::error!(status = %status, error = %err, "Failed to parse error response");
                    GetError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: text.clone(),
                    }
                })?;

                match error_response.error_code.as_str() {
                    "INVALID_PROVIDER_NAME" => Err(GetError::InvalidName(error_response.into())),
                    "PROVIDER_CONVERSION_ERROR" => {
                        Err(GetError::ConversionError(error_response.into()))
                    }
                    _ => Err(GetError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: text,
                    }),
                }
            }
            _ => {
                let text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| String::from("Failed to read response body"));
                Err(GetError::UnexpectedResponse {
                    status: status.as_u16(),
                    message: text,
                })
            }
        }
    }

    /// List all providers.
    ///
    /// GETs from `/providers` endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`ListError`] for network errors, API errors (500),
    /// or unexpected responses.
    #[tracing::instrument(skip(self))]
    pub async fn list(&self) -> Result<Vec<ProviderInfo>, ListError> {
        let url = self
            .client
            .base_url()
            .join(provider_get_all())
            .expect("valid URL");

        tracing::debug!("Sending GET request");

        let response = self
            .client
            .http()
            .get(url.as_str())
            .send()
            .await
            .map_err(|err| ListError::Network {
                url: url.to_string(),
                source: err,
            })?;

        let status = response.status();
        tracing::debug!(status = %status, "Received API response");

        match status.as_u16() {
            200 => {
                let providers_response =
                    response.json::<ProvidersResponse>().await.map_err(|err| {
                        tracing::error!(error = %err, "Failed to parse providers response");
                        ListError::UnexpectedResponse {
                            status: status.as_u16(),
                            message: format!("Failed to parse response: {}", err),
                        }
                    })?;

                Ok(providers_response.providers)
            }
            500 => {
                let text = response.text().await.map_err(|err| {
                    tracing::error!(status = %status, error = %err, "Failed to read error response");
                    ListError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to read error response: {}", err),
                    }
                })?;

                let _error_response: ErrorResponse = serde_json::from_str(&text).map_err(|err| {
                    tracing::error!(status = %status, error = %err, "Failed to parse error response");
                    ListError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: text.clone(),
                    }
                })?;

                // List providers has no specific error codes on server side (infallible)
                Err(ListError::UnexpectedResponse {
                    status: status.as_u16(),
                    message: text,
                })
            }
            _ => {
                let text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| String::from("Failed to read response body"));
                Err(ListError::UnexpectedResponse {
                    status: status.as_u16(),
                    message: text,
                })
            }
        }
    }

    /// Delete a provider by name.
    ///
    /// DELETEs to `/providers/{name}` endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`DeleteError`] for network errors, API errors (400/404/500),
    /// or unexpected responses.
    #[tracing::instrument(skip(self), fields(name = %name))]
    pub async fn delete(&self, name: &str) -> Result<(), DeleteError> {
        let url = self
            .client
            .base_url()
            .join(&provider_delete_by_id(name))
            .expect("valid URL");

        tracing::debug!("Sending DELETE request");

        let response = self
            .client
            .http()
            .delete(url.as_str())
            .send()
            .await
            .map_err(|err| DeleteError::Network {
                url: url.to_string(),
                source: err,
            })?;

        let status = response.status();
        tracing::debug!(status = %status, "Received API response");

        match status.as_u16() {
            204 => {
                tracing::info!("Provider deleted successfully");
                Ok(())
            }
            400 | 404 | 500 => {
                let text = response.text().await.map_err(|err| {
                    tracing::error!(status = %status, error = %err, "Failed to read error response");
                    DeleteError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to read error response: {}", err),
                    }
                })?;

                let error_response: ErrorResponse = serde_json::from_str(&text).map_err(|err| {
                    tracing::error!(status = %status, error = %err, "Failed to parse error response");
                    DeleteError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: text.clone(),
                    }
                })?;

                match error_response.error_code.as_str() {
                    "INVALID_PROVIDER_NAME" => Err(DeleteError::InvalidName(error_response.into())),
                    "PROVIDER_NOT_FOUND" => Err(DeleteError::NotFound(error_response.into())),
                    "STORE_ERROR" => Err(DeleteError::StoreError(error_response.into())),
                    _ => Err(DeleteError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: text,
                    }),
                }
            }
            _ => {
                let text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| String::from("Failed to read response body"));
                Err(DeleteError::UnexpectedResponse {
                    status: status.as_u16(),
                    message: text,
                })
            }
        }
    }
}

/// Response body for GET /providers endpoint.
#[derive(Debug, serde::Deserialize)]
struct ProvidersResponse {
    providers: Vec<ProviderInfo>,
}

/// Provider information from the API.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ProviderInfo {
    /// Provider name
    pub name: String,
    /// Provider kind (e.g., "evm-rpc", "firehose")
    pub kind: String,
    /// Network identifier
    pub network: String,
    /// Additional provider fields
    #[serde(flatten)]
    pub rest: serde_json::Value,
}

/// Errors that can occur when registering a provider.
#[derive(Debug, thiserror::Error)]
pub enum RegisterError {
    /// Invalid configuration - not a JSON object
    #[error("provider config must be a JSON object")]
    InvalidConfig,

    /// The JSON request body is malformed or invalid (400, INVALID_REQUEST_BODY)
    ///
    /// This occurs when:
    /// - The request body is not valid JSON
    /// - Required fields are missing (name, kind, network)
    /// - Field values have incorrect types
    /// - The JSON structure doesn't match the expected schema
    #[error("invalid request body")]
    InvalidRequestBody(#[source] ApiError),

    /// Failed to convert JSON to TOML format (400, DATA_CONVERSION_ERROR)
    ///
    /// This occurs when the JSON data in the request body cannot be
    /// converted to TOML format, specifically due to:
    /// - **Null values**: TOML doesn't support null/None values
    /// - **Mixed-type arrays**: TOML arrays must contain homogeneous types
    /// - **Complex nested structures**: Some deeply nested JSON structures may not map to TOML
    /// - **Invalid TOML table keys**: Keys that are not valid TOML identifiers
    #[error("data conversion error")]
    DataConversionError(#[source] ApiError),

    /// A provider with the same name already exists (409, PROVIDER_CONFLICT)
    ///
    /// This occurs when:
    /// - Attempting to create a provider configuration with a name that is already in use
    /// - Provider names must be unique within the system
    #[error("provider conflict")]
    Conflict(#[source] ApiError),

    /// Failed to store the provider configuration (500, STORE_ERROR)
    ///
    /// This occurs when:
    /// - The underlying storage operation fails
    /// - Filesystem errors, serialization failures, or other store-level issues occur
    #[error("store error")]
    StoreError(#[source] ApiError),

    /// Network or connection error
    #[error("network error connecting to {url}")]
    Network {
        url: String,
        #[source]
        source: reqwest::Error,
    },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}

/// Errors that can occur when getting a provider.
#[derive(Debug, thiserror::Error)]
pub enum GetError {
    /// The provider name in the URL path is invalid (400, INVALID_PROVIDER_NAME)
    ///
    /// This occurs when:
    /// - The path parameter cannot be extracted properly
    /// - URL encoding issues or empty names
    #[error("invalid provider name")]
    InvalidName(#[source] ApiError),

    /// Failed to convert provider configuration to info format (500, PROVIDER_CONVERSION_ERROR)
    ///
    /// This occurs when:
    /// - The provider configuration cannot be converted to the API response format
    /// - Invalid JSON data in the provider's configuration
    #[error("provider conversion error")]
    ConversionError(#[source] ApiError),

    /// Network or connection error
    #[error("network error connecting to {url}")]
    Network {
        url: String,
        #[source]
        source: reqwest::Error,
    },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}

/// Errors that can occur when listing providers.
#[derive(Debug, thiserror::Error)]
pub enum ListError {
    /// Network or connection error
    #[error("network error connecting to {url}")]
    Network {
        url: String,
        #[source]
        source: reqwest::Error,
    },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}

/// Errors that can occur when deleting a provider.
#[derive(Debug, thiserror::Error)]
pub enum DeleteError {
    /// The provider name in the URL path is invalid (400, INVALID_PROVIDER_NAME)
    ///
    /// This occurs when:
    /// - The path parameter cannot be extracted properly
    /// - URL encoding issues or empty names
    #[error("invalid provider name")]
    InvalidName(#[source] ApiError),

    /// The requested provider was not found in the store (404, PROVIDER_NOT_FOUND)
    ///
    /// This occurs when:
    /// - The provider name is valid but no provider configuration exists with that name
    #[error("provider not found")]
    NotFound(#[source] ApiError),

    /// Failed to delete the provider configuration from the store (500, STORE_ERROR)
    ///
    /// This occurs when:
    /// - The underlying storage operation fails
    /// - Filesystem errors, permission issues, or other store-level problems during deletion
    #[error("store error")]
    StoreError(#[source] ApiError),

    /// Network or connection error
    #[error("network error connecting to {url}")]
    Network {
        url: String,
        #[source]
        source: reqwest::Error,
    },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}
