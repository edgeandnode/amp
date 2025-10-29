//! Dataset manifest registration command.
//!
//! Registers a dataset manifest with the admin API by:
//! 1. Loading manifest JSON from local or remote storage
//! 2. POSTing to admin API `/datasets` endpoint
//! 3. Handling success (201) or error responses
//!
//! # Supported Storage
//!
//! - Local: `./manifest.json`, `/tmp/manifest.json`, `file:///tmp/manifest.json`
//! - S3: `s3://bucket-name/path/manifest.json`
//! - GCS: `gs://bucket-name/path/manifest.json`
//! - Azure: `az://container/path/manifest.json`
//!
//! # Dataset Reference Format
//!
//! Pattern: `namespace/name@version` (e.g., `graph/eth_mainnet@1.0.0`)
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use common::store::{ObjectStoreExt as _, ObjectStoreUrl, object_store};
use datasets_common::reference::Reference;
use object_store::path::Path as ObjectStorePath;

use crate::args::GlobalArgs;

/// Command-line arguments for the `reg-manifest` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// The dataset reference in format: namespace/name@version
    ///
    /// Examples: my_namespace/my_dataset@1.0.0, my_namespace/my_dataset@latest
    #[arg(value_name = "REFERENCE", required = true, value_parser = clap::value_parser!(Reference))]
    pub dataset_ref: Reference,

    /// Path or URL to the manifest file (local path, file://, s3://, gs://, or az://)
    #[arg(value_name = "FILE", required = true, value_parser = clap::value_parser!(ManifestFilePath))]
    pub manifest_file: ManifestFilePath,
}

/// Register a dataset manifest with the admin API.
///
/// Loads manifest content from storage and POSTs to `/datasets` endpoint.
///
/// # Errors
///
/// Returns [`Error`] for file not found, read failures, invalid paths/URLs,
/// API errors (400/409/500), or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, %dataset_ref))]
pub async fn run(
    Args {
        global,
        dataset_ref,
        manifest_file,
    }: Args,
) -> Result<(), Error> {
    tracing::debug!(
        manifest_path = %manifest_file,
        dataset_ref = %dataset_ref,
        "Loading and registering manifest"
    );

    let manifest_str = load_manifest(&manifest_file).await?;

    register_manifest(&global, &dataset_ref, &manifest_str).await?;

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
/// Uses the [`DatasetsClient`] to POST to `/datasets` endpoint with namespace, name, version, and manifest content.
#[tracing::instrument(skip_all)]
pub async fn register_manifest(
    global: &GlobalArgs,
    dataset_ref: &Reference,
    dataset_manifest: &str,
) -> Result<(), Error> {
    tracing::debug!("Creating admin API client");

    let client = global.build_client()?;
    let datasets_client = client.datasets();

    tracing::debug!("Sending registration request");

    datasets_client
        .register(dataset_ref, dataset_manifest)
        .await
        .map_err(|err| Error::ClientError { source: err })
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

    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuildError {
        #[from]
        source: crate::args::BuildClientError,
    },

    /// Client error during registration
    #[error("dataset registration failed")]
    ClientError {
        #[source]
        source: crate::client::datasets::RegisterError,
    },
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
