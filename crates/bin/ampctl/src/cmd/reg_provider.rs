//! Provider configuration registration command.
//!
//! Registers a provider configuration with the admin API by:
//! 1. Loading provider TOML from local or remote storage
//! 2. Converting TOML to JSON format for API
//! 3. POSTing to admin API `/providers` endpoint
//! 4. Handling success (201) or error responses
//!
//! # Supported Storage
//!
//! - Local: `./provider.toml`, `/tmp/provider.toml`, `file:///tmp/provider.toml`
//! - S3: `s3://bucket-name/path/provider.toml`
//! - GCS: `gs://bucket-name/path/provider.toml`
//! - Azure: `az://container/path/provider.toml`
//!
//! # Provider Name Format
//!
//! Provider names must be unique identifiers (e.g., `mainnet-rpc`, `goerli-firehose`)
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use common::store::{ObjectStoreExt as _, ObjectStoreUrl, object_store};
use object_store::path::Path as ObjectStorePath;
use url::Url;

/// Command-line arguments for the `reg-provider` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    /// The URL of the engine admin interface
    #[arg(long, env = "AMP_ADMIN_URL", default_value = "http://localhost:1610", value_parser = clap::value_parser!(Url))]
    pub admin_url: Url,

    /// The name of the provider configuration
    ///
    /// Examples: mainnet-rpc, goerli-firehose, polygon-rpc
    #[arg(value_name = "PROVIDER_NAME", required = true)]
    pub provider_name: String,

    /// Path or URL to the provider TOML file (local path, file://, s3://, gs://, or az://)
    #[arg(value_name = "FILE", required = true, value_parser = clap::value_parser!(ProviderFilePath))]
    pub provider_file: ProviderFilePath,
}

/// Register a provider configuration with the admin API.
///
/// Loads provider TOML content from storage, converts to JSON, and POSTs to `/providers` endpoint.
///
/// # Errors
///
/// Returns [`Error`] for file not found, read failures, invalid paths/URLs,
/// TOML parse errors, API errors (400/409/500), or network failures.
#[tracing::instrument(skip_all, fields(%admin_url, %provider_name))]
pub async fn run(
    Args {
        admin_url,
        provider_name,
        provider_file,
    }: Args,
) -> Result<(), Error> {
    tracing::debug!(
        provider_path = %provider_file,
        provider_name = %provider_name,
        "Loading and registering provider configuration"
    );

    let provider_toml = load_provider(&provider_file).await?;

    register_provider(&admin_url, &provider_name, &provider_toml).await?;

    Ok(())
}

/// Load provider TOML content from the specified file path or URL.
///
/// Uses the object_store crate directly to read the file as UTF-8.
/// The file path is extracted from the URL and passed to the object store.
///
/// Returns [`Error`] for invalid paths, missing files, or read failures.
#[tracing::instrument(skip_all, fields(%provider_path))]
async fn load_provider(provider_path: &ProviderFilePath) -> Result<String, Error> {
    // Create object store from the URL
    let (store, _) =
        object_store(provider_path.as_url()).map_err(|source| Error::ObjectStoreCreation {
            path: provider_path.to_string(),
            source,
        })?;

    // Get the file path from the URL
    let file_path = ObjectStorePath::from(provider_path.0.path());

    // Read the file from the object store
    store.get_string(file_path).await.map_err(|err| {
        if err.is_not_found() {
            tracing::error!("Provider file not found");
            Error::ProviderNotFound {
                path: provider_path.to_string(),
            }
        } else {
            tracing::error!(error = %err, "Failed to read provider file");
            Error::ProviderReadError {
                path: provider_path.to_string(),
                source: err,
            }
        }
    })
}

/// Register the provider configuration with the admin API.
///
/// Parses TOML content, converts to JSON, and POSTs to `/providers` endpoint.
#[tracing::instrument(skip_all)]
pub async fn register_provider(
    admin_url: &Url,
    provider_name: &str,
    provider_toml: &str,
) -> Result<(), Error> {
    let url = admin_url.join("providers").map_err(|err| {
        tracing::error!(admin_url = %admin_url, error = %err, "Invalid admin URL");
        Error::InvalidAdminUrl {
            url: admin_url.to_string(),
            source: err,
        }
    })?;

    // Parse TOML content
    let toml_value: toml::Value = toml::from_str(provider_toml).map_err(|err| {
        tracing::error!(error = %err, "Failed to parse provider TOML");
        Error::TomlParseError { source: err }
    })?;

    // Convert TOML to JSON for API request
    let json_value: serde_json::Value = serde_json::to_value(&toml_value).map_err(|err| {
        tracing::error!(error = %err, "Failed to convert TOML to JSON");
        Error::TomlToJsonConversion { source: err }
    })?;

    // Ensure the provider name is set correctly
    let mut json_obj = json_value
        .as_object()
        .ok_or_else(|| {
            tracing::error!("Provider TOML must be a table/object at the root level");
            Error::InvalidProviderStructure {
                message: "Provider TOML must be a table/object at the root level".to_string(),
            }
        })?
        .clone();

    // Override the name field with the provided provider_name
    json_obj.insert(
        "name".to_string(),
        serde_json::Value::String(provider_name.to_string()),
    );

    tracing::debug!("Sending registration request");

    let client = reqwest::Client::new();
    let response = client
        .post(url.as_str())
        .json(&json_obj)
        .send()
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "Network error during API request");
            Error::NetworkError {
                url: url.to_string(),
                source: err,
            }
        })?;

    let status = response.status();
    tracing::debug!(status = %status, "Received API response");

    match status.as_u16() {
        201 => Ok(()),
        400 | 409 | 500 => {
            let error_response = response.json::<ErrorResponse>().await.map_err(|err| {
                tracing::error!(
                    status = %status,
                    error = %err,
                    "Failed to parse error response from API"
                );
                Error::UnexpectedResponse {
                    status: status.as_u16(),
                    message: format!("Failed to parse error response: {}", err),
                }
            })?;

            tracing::error!(
                status = %status,
                error_code = %error_response.error_code,
                error_message = %error_response.error_message,
                "API returned error response"
            );

            Err(Error::ApiError {
                status: status.as_u16(),
                error_code: error_response.error_code,
                message: error_response.error_message,
            })
        }
        _ => {
            tracing::error!(status = %status, "Unexpected status code from API");
            Err(Error::UnexpectedResponse {
                status: status.as_u16(),
                message: format!("Unexpected status code: {}", status),
            })
        }
    }
}

/// Error response from the admin API (400/409/500 status codes).
#[derive(Debug, serde::Deserialize)]
struct ErrorResponse {
    error_code: String,
    error_message: String,
}

/// Errors for provider registration operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Provider file not found at the specified path
    #[error("provider file not found: {path}")]
    ProviderNotFound { path: String },

    /// Failed to create object store for provider location
    #[error("failed to create object store for provider at {path}")]
    ObjectStoreCreation {
        path: String,
        source: common::store::ObjectStoreCreationError,
    },

    /// Failed to read provider from storage
    #[error("failed to read provider from {path}")]
    ProviderReadError {
        path: String,
        source: common::store::StoreError,
    },

    /// Failed to parse provider TOML
    #[error("failed to parse provider TOML")]
    TomlParseError { source: toml::de::Error },

    /// Failed to convert TOML to JSON
    #[error("failed to convert TOML to JSON")]
    TomlToJsonConversion { source: serde_json::Error },

    /// Invalid provider structure
    #[error("invalid provider structure: {message}")]
    InvalidProviderStructure { message: String },

    /// Invalid admin URL
    #[error("invalid admin URL '{url}'")]
    InvalidAdminUrl {
        url: String,
        source: url::ParseError,
    },

    /// API returned an error response
    #[error("API error ({status}): [{error_code}] {message}")]
    ApiError {
        status: u16,
        error_code: String,
        message: String,
    },

    /// Network or connection error
    #[error("network error connecting to {url}")]
    NetworkError { url: String, source: reqwest::Error },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}

/// Provider file path supporting local and remote storage.
///
/// Wraps [`ObjectStoreUrl`] with validation. Supports local paths, file://, s3://, gs://, and az:// URLs.
///
/// Implements [`std::str::FromStr`] for use with Clap's `value_parser`.
#[derive(Debug, Clone)]
pub struct ProviderFilePath(ObjectStoreUrl);

impl ProviderFilePath {
    /// Get the inner [`ObjectStoreUrl`].
    pub fn as_url(&self) -> &ObjectStoreUrl {
        &self.0
    }
}

impl std::str::FromStr for ProviderFilePath {
    type Err = ProviderFilePathError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ObjectStoreUrl::new(s)
            .map(Self)
            .map_err(|source| ProviderFilePathError {
                path: s.to_string(),
                source,
            })
    }
}

impl std::fmt::Display for ProviderFilePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Error when parsing a provider file path.
#[derive(Debug, thiserror::Error)]
#[error("invalid provider file path '{path}'")]
pub struct ProviderFilePathError {
    path: String,
    source: common::store::InvalidObjectStoreUrlError,
}
