//! Manifest registration command.
//!
//! Registers a manifest with content-addressable storage through the admin API by:
//! 1. Loading manifest JSON from local or remote storage
//! 2. POSTing to admin API `/manifests` endpoint
//! 3. Receiving and displaying the computed manifest hash
//!
//! # Supported Storage
//!
//! - Local: `./manifest.json`, `/tmp/manifest.json`, `file:///tmp/manifest.json`
//! - S3: `s3://bucket-name/path/manifest.json`
//! - GCS: `gs://bucket-name/path/manifest.json`
//! - Azure: `az://container/path/manifest.json`
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use common::store::{ObjectStoreExt as _, ObjectStoreUrl, object_store};
use datasets_common::hash::Hash;
use object_store::path::Path as ObjectStorePath;
use url::Url;

/// Command-line arguments for the `manifest register` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    /// The URL of the engine admin interface
    #[arg(long, env = "AMP_ADMIN_URL", default_value = "http://localhost:1610", value_parser = clap::value_parser!(Url))]
    pub admin_url: Url,

    /// Path or URL to the manifest file (local path, file://, s3://, gs://, or az://)
    #[arg(value_name = "PATH", required = true, value_parser = clap::value_parser!(ManifestFilePath))]
    pub manifest_file: ManifestFilePath,
}

/// Register a manifest with content-addressable storage via the admin API.
///
/// Loads manifest content from storage and POSTs to `/manifests` endpoint.
///
/// # Errors
///
/// Returns [`Error`] for file not found, read failures, invalid paths/URLs,
/// API errors (400/500), or network failures.
#[tracing::instrument(skip_all, fields(%admin_url))]
pub async fn run(
    Args {
        admin_url,
        manifest_file,
    }: Args,
) -> Result<(), Error> {
    tracing::debug!(
        manifest_path = %manifest_file,
        "Loading and registering manifest"
    );

    let manifest_str = load_manifest(&manifest_file).await?;

    let hash = register_manifest(&admin_url, &manifest_str).await?;

    crate::success!("Manifest registered successfully");
    crate::info!("Manifest hash: {}", hash);

    Ok(())
}

/// Load manifest content from the specified file path or URL.
///
/// Uses the object_store crate directly to read the file as UTF-8.
/// The file path is extracted from the URL and passed to the object store.
///
/// Returns [`Error`] for invalid paths, missing files, or read failures.
#[tracing::instrument(skip_all)]
async fn load_manifest(manifest_path: &ManifestFilePath) -> Result<String, Error> {
    // Create object store from the URL
    let (store, _) =
        object_store(manifest_path.as_url()).map_err(|source| Error::ObjectStoreCreation {
            path: manifest_path.to_string(),
            source,
        })?;

    // Get the file path from the URL
    let file_path = ObjectStorePath::from(manifest_path.0.path());

    // Read the file from the object store
    store.get_string(file_path).await.map_err(|err| {
        if err.is_not_found() {
            tracing::error!(path = %manifest_path, "Manifest file not found");
            Error::ManifestNotFound {
                path: manifest_path.to_string(),
            }
        } else {
            tracing::error!(path = %manifest_path, error = %err, "Failed to read manifest");
            Error::ManifestReadError {
                path: manifest_path.to_string(),
                source: err,
            }
        }
    })
}

/// Register the manifest with the admin API.
///
/// POSTs to `/manifests` endpoint with the manifest JSON content.
/// Returns the computed content-addressable hash.
#[tracing::instrument(skip_all)]
pub async fn register_manifest(admin_url: &Url, manifest_str: &str) -> Result<Hash, Error> {
    let url = admin_url.join("manifests").map_err(|err| {
        tracing::error!(admin_url = %admin_url, error = %err, "Invalid admin URL");
        Error::InvalidAdminUrl {
            url: admin_url.to_string(),
            source: err,
        }
    })?;

    tracing::debug!("Sending registration request");

    // Parse manifest JSON to send as JSON value
    let manifest_json: serde_json::Value = serde_json::from_str(manifest_str).map_err(|err| {
        tracing::error!(error = %err, "Failed to parse manifest JSON");
        Error::InvalidManifestJson { source: err }
    })?;

    let client = reqwest::Client::new();
    let response = client
        .post(url.as_str())
        .json(&manifest_json)
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
        201 => {
            let register_response =
                response
                    .json::<RegisterManifestResponse>()
                    .await
                    .map_err(|err| {
                        tracing::error!(error = %err, "Failed to parse success response from API");
                        Error::UnexpectedResponse {
                            status: status.as_u16(),
                            message: format!("Failed to parse response: {}", err),
                        }
                    })?;
            Ok(register_response.hash)
        }
        400 | 500 => {
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

/// Response body for the POST /manifests endpoint (201 success).
#[derive(Debug, serde::Deserialize)]
struct RegisterManifestResponse {
    hash: Hash,
}

/// Error response from the admin API (400/500 status codes).
#[derive(Debug, serde::Deserialize)]
struct ErrorResponse {
    error_code: String,
    error_message: String,
}

/// Errors for manifest registration operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Manifest file not found at the specified path
    #[error("manifest file not found: {path}")]
    ManifestNotFound { path: String },

    /// Failed to create object store for manifest location
    #[error("failed to create object store for manifest at {path}")]
    ObjectStoreCreation {
        path: String,
        source: common::store::ObjectStoreCreationError,
    },

    /// Failed to read manifest from storage
    #[error("failed to read manifest from {path}")]
    ManifestReadError {
        path: String,
        source: common::store::StoreError,
    },

    /// Invalid manifest JSON content
    #[error("invalid manifest JSON")]
    InvalidManifestJson { source: serde_json::Error },

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

/// Manifest file path supporting local and remote storage.
///
/// Wraps [`ObjectStoreUrl`] with validation. Supports local paths, file://, s3://, gs://, and az:// URLs.
///
/// Implements [`std::str::FromStr`] for use with Clap's `value_parser`.
#[derive(Debug, Clone)]
pub struct ManifestFilePath(ObjectStoreUrl);

impl ManifestFilePath {
    /// Get the inner [`ObjectStoreUrl`].
    pub fn as_url(&self) -> &ObjectStoreUrl {
        &self.0
    }
}

impl std::str::FromStr for ManifestFilePath {
    type Err = ManifestFilePathError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ObjectStoreUrl::new(s)
            .map(Self)
            .map_err(|source| ManifestFilePathError {
                path: s.to_string(),
                source,
            })
    }
}

impl std::fmt::Display for ManifestFilePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Error when parsing a manifest file path.
#[derive(Debug, thiserror::Error)]
#[error("invalid manifest file path '{path}'")]
pub struct ManifestFilePathError {
    path: String,
    source: common::store::InvalidObjectStoreUrlError,
}
