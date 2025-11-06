//! Dataset registration command.
//!
//! Registers a dataset by performing the following steps:
//! 1. Loads manifest JSON from local or remote storage
//! 2. Stores the manifest in content-addressable storage (identified by its content hash)
//! 3. Links the manifest hash to the specified dataset (namespace/name)
//! 4. Tags the dataset revision with a semantic version (if --tag is provided), or updates "dev" by default
//!
//! # What This Command Does
//!
//! - **Registers manifest**: Stores the manifest content in the system's content-addressable storage
//! - **Links to dataset**: Associates the manifest hash with the dataset FQN (namespace/name)
//! - **Tags version**: Applies a semantic version tag to identify this dataset revision (optional)
//!
//! # Supported Storage Backends
//!
//! Manifest files can be loaded from:
//! - Local filesystem: `./manifest.json`, `/path/to/manifest.json`
//! - S3: `s3://bucket-name/path/manifest.json`
//! - GCS: `gs://bucket-name/path/manifest.json`
//! - Azure Blob: `az://container/path/manifest.json`
//! - File URL: `file:///path/to/manifest.json`
//!
//! # Arguments
//!
//! - `FQN`: Fully qualified dataset name in format `namespace/name` (e.g., `graph/eth_mainnet`)
//! - `FILE`: Path or URL to the manifest file
//! - `--tag` / `-t`: Optional semantic version tag (e.g., `1.0.0`, `2.1.3`)
//!   - If not specified, only the "dev" tag is updated to point to this revision
//!   - Cannot use special tags like "latest" or "dev" - these are managed by the system
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use common::store::{ObjectStoreExt as _, ObjectStoreUrl, object_store};
use datasets_common::{fqn::FullyQualifiedName, version::Version};
use object_store::path::Path as ObjectStorePath;
use serde_json::value::RawValue;

use crate::{args::GlobalArgs, client::datasets::HashOrManifestJson};

/// Command-line arguments for the `reg-manifest` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// The fully qualified dataset name in format: namespace/name
    ///
    /// Example: my_namespace/my_dataset
    #[arg(value_name = "FQN", required = true, value_parser = clap::value_parser!(FullyQualifiedName))]
    pub fqn: FullyQualifiedName,

    /// Path or URL to the manifest file (local path, file://, s3://, gs://, or az://)
    #[arg(value_name = "FILE", required = true, value_parser = clap::value_parser!(ManifestFilePath))]
    pub manifest_file: ManifestFilePath,

    /// Optional semantic version tag for the dataset (e.g., 1.0.0, 2.1.3)
    /// Must be a valid semantic version - cannot be "latest" or "dev"
    /// If not specified, only the "dev" tag is updated
    #[arg(short = 't', long = "tag", value_parser = clap::value_parser!(Version))]
    pub tag: Option<Version>,
}

/// Register a dataset manifest with the admin API.
///
/// Loads manifest content from storage and POSTs to `/datasets` endpoint.
///
/// # Errors
///
/// Returns [`Error`] for file not found, read failures, invalid paths/URLs,
/// API errors (400/409/500), or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, fqn = %fqn, tag = ?tag))]
pub async fn run(
    Args {
        global,
        fqn,
        manifest_file,
        tag,
    }: Args,
) -> Result<(), Error> {
    tracing::debug!(
        manifest_path = %manifest_file,
        fqn = %fqn,
        tag = ?tag,
        "Loading and registering manifest"
    );

    let manifest_str = load_manifest(&manifest_file).await?;

    let manifest: Box<RawValue> =
        serde_json::from_str(&manifest_str).map_err(Error::InvalidManifest)?;

    register_manifest(&global, &fqn, tag.as_ref(), manifest).await?;

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
    fqn: &FullyQualifiedName,
    version: Option<&Version>,
    manifest: impl Into<HashOrManifestJson>,
) -> Result<(), Error> {
    tracing::debug!("Creating admin API client");

    let client = global.build_client().map_err(Error::ClientBuildError)?;
    let datasets_client = client.datasets();

    tracing::debug!("Sending registration request");

    datasets_client
        .register(fqn, version, manifest)
        .await
        .map_err(Error::ClientError)
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

    /// Invalid manifest format (not valid JSON or hash)
    #[error("invalid manifest format")]
    InvalidManifest(#[source] serde_json::Error),

    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuildError(#[source] crate::args::BuildClientError),

    /// Client error during registration
    #[error("dataset registration failed")]
    ClientError(#[source] crate::client::datasets::RegisterError),
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
