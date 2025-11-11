//! Provider registration command.
//!
//! Registers a provider configuration through the admin API by:
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
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use common::store::{ObjectStoreExt as _, ObjectStoreUrl, object_store};
use monitoring::logging;
use object_store::path::Path as ObjectStorePath;

use crate::args::GlobalArgs;

/// Command-line arguments for the `provider register` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// The name of the provider configuration
    ///
    /// Examples: mainnet-rpc, goerli-firehose, polygon-rpc
    #[arg(value_name = "PROVIDER_NAME", required = true)]
    pub provider_name: String,

    /// Path or URL to the provider TOML file (local path, file://, s3://, gs://, or az://)
    #[arg(value_name = "FILE", required = true, value_parser = clap::value_parser!(ProviderFilePath))]
    pub provider_file: ProviderFilePath,
}

/// Register a provider configuration via the admin API.
///
/// Loads provider TOML content from storage, converts to JSON, and POSTs to `/providers` endpoint.
///
/// # Errors
///
/// Returns [`Error`] for file not found, read failures, invalid paths/URLs,
/// TOML parse errors, API errors (400/409/500), or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, %provider_name))]
pub async fn run(
    Args {
        global,
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

    register_provider(&global, &provider_name, &provider_toml).await?;

    crate::success!("Provider registered successfully");

    Ok(())
}

/// Load provider TOML content from the specified file path or URL.
///
/// Uses the object_store crate directly to read the file as UTF-8.
/// The file path is extracted from the URL and passed to the object store.
///
/// Returns [`Error`] for invalid paths, missing files, or read failures.
#[tracing::instrument(skip_all)]
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
            tracing::error!(path = %provider_path, "Provider file not found");
            Error::ProviderNotFound {
                path: provider_path.to_string(),
            }
        } else {
            tracing::error!(path = %provider_path, error = %err, error_source = logging::error_source(&err), "Failed to read provider");
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
async fn register_provider(
    global: &GlobalArgs,
    provider_name: &str,
    provider_toml: &str,
) -> Result<(), Error> {
    // Parse TOML content
    let toml_value: toml::Value = toml::from_str(provider_toml).map_err(|err| {
        tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to parse provider TOML");
        Error::TomlParseError { source: err }
    })?;

    // Convert TOML to JSON for API request
    let json_value: serde_json::Value = serde_json::to_value(&toml_value).map_err(|err| {
        tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to convert TOML to JSON");
        Error::TomlToJsonConversion { source: err }
    })?;

    // Validate that the JSON is an object
    if !json_value.is_object() {
        tracing::error!("Provider TOML must be a table/object at the root level");
        return Err(Error::InvalidProviderStructure {
            message: "Provider TOML must be a table/object at the root level".to_string(),
        });
    }

    // Create client and register provider
    let client = global.build_client().map_err(Error::ClientBuildError)?;

    client
        .providers()
        .register(provider_name, &json_value)
        .await
        .map_err(|source| Error::ClientError { source })?;

    Ok(())
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

    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuildError(#[source] crate::args::BuildClientError),

    /// Error from the provider client
    #[error("provider registration failed")]
    ClientError {
        #[source]
        source: crate::client::providers::RegisterError,
    },
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
