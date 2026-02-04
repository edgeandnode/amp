//! Dataset registration command.
//!
//! Registers a dataset by performing the following steps:
//! 1. Loads manifest JSON from local or remote storage (or converts from package directory)
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
//! # Supported Input Modes
//!
//! ## 1. Manifest File (Default)
//!
//! Provide a path or URL to a manifest JSON file directly:
//! - Local filesystem: `./manifest.json`, `/path/to/manifest.json`
//! - S3: `s3://bucket-name/path/manifest.json`
//! - GCS: `gs://bucket-name/path/manifest.json`
//! - Azure Blob: `az://container/path/manifest.json`
//! - File URL: `file:///path/to/manifest.json`
//!
//! ## 2. Package Directory (`--package`)
//!
//! Provide a directory containing the build output from `amp dataset build`:
//! - Reads the authoring `manifest.json` from the directory
//! - Converts it to legacy runtime format using the bridge module
//! - Uploads the converted manifest to the admin API
//!
//! ## 3. Auto-Detection (No File Argument)
//!
//! If no file argument is provided:
//! - Looks for `manifest.json` in the current directory
//! - If found, registers it directly (not as a package)
//!
//! # Arguments
//!
//! - `FQN`: Fully qualified dataset name in format `namespace/name` (e.g., `graph/eth_mainnet`)
//! - `FILE`: Optional path or URL to the manifest file (not used with `--package`)
//! - `--package` / `-p`: Directory containing built dataset package (with manifest.json)
//! - `--tag` / `-t`: Optional semantic version tag (e.g., `1.0.0`, `2.1.3`)
//!   - If not specified, only the "dev" tag is updated to point to this revision
//!   - Cannot use special tags like "latest" or "dev" - these are managed by the system
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use std::{fs, path::PathBuf};

use amp_object_store::{
    self as store, ObjectStoreCreationError,
    ext::{ObjectStoreExt as _, ObjectStoreExtError},
    url::{ObjectStoreUrl, ObjectStoreUrlError},
};
use dataset_authoring::{bridge::LegacyBridge, manifest::AuthoringManifest};
use datasets_common::{fqn::FullyQualifiedName, version::Version};
use monitoring::logging;
use object_store::path::Path as ObjectStorePath;
use serde_json::value::RawValue;

use crate::{args::GlobalArgs, client::datasets::HashOrManifestJson};

/// Command-line arguments for the `manifest register` command.
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
    ///
    /// Not used with --package. If neither FILE nor --package is provided,
    /// looks for manifest.json in the current directory.
    #[arg(value_name = "FILE", value_parser = clap::value_parser!(ManifestFilePath), conflicts_with = "package")]
    pub manifest_file: Option<ManifestFilePath>,

    /// Directory containing built dataset package (from `amp dataset build`)
    ///
    /// Reads manifest.json from the directory, converts it to legacy runtime
    /// format, and uploads to the admin API. Cannot be used with FILE argument.
    #[arg(
        short = 'p',
        long = "package",
        value_name = "DIR",
        conflicts_with = "manifest_file"
    )]
    pub package: Option<PathBuf>,

    /// Optional semantic version tag for the dataset (e.g., 1.0.0, 2.1.3)
    /// Must be a valid semantic version - cannot be "latest" or "dev"
    /// If not specified, only the "dev" tag is updated
    #[arg(short = 't', long = "tag", value_parser = clap::value_parser!(Version))]
    pub tag: Option<Version>,
}

/// Register a dataset manifest with the admin API.
///
/// Supports three input modes:
/// 1. `--package <dir>`: Load authoring manifest from directory, convert to legacy format
/// 2. `FILE` argument: Load manifest from file path or URL
/// 3. Neither: Look for `manifest.json` in current directory
///
/// # Errors
///
/// Returns [`Error`] for file not found, read failures, invalid paths/URLs,
/// API errors (400/409/500), network failures, or conversion errors.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, fqn = %fqn, tag = ?tag))]
pub async fn run(
    Args {
        global,
        fqn,
        manifest_file,
        package,
        tag,
    }: Args,
) -> Result<(), Error> {
    let manifest_str = if let Some(package_dir) = package {
        // Mode 1: Package directory - load authoring manifest and convert to legacy
        tracing::debug!(
            package_dir = %package_dir.display(),
            fqn = %fqn,
            tag = ?tag,
            "Loading manifest from package directory and converting to legacy format"
        );
        load_and_convert_package(&package_dir)?
    } else if let Some(ref manifest_path) = manifest_file {
        // Mode 2: Manifest file path or URL
        tracing::debug!(
            manifest_path = %manifest_path,
            fqn = %fqn,
            tag = ?tag,
            "Loading manifest from file"
        );
        load_manifest(manifest_path).await?
    } else {
        // Mode 3: Auto-detect manifest.json in current directory
        let cwd = std::env::current_dir().map_err(Error::CurrentDir)?;
        let default_manifest = cwd.join("manifest.json");
        if !default_manifest.exists() {
            return Err(Error::NoManifestSource);
        }
        tracing::debug!(
            manifest_path = %default_manifest.display(),
            fqn = %fqn,
            tag = ?tag,
            "Loading manifest from current directory"
        );
        let manifest_path: ManifestFilePath =
            default_manifest.to_string_lossy().parse().map_err(|err| {
                Error::InvalidDefaultPath {
                    path: default_manifest,
                    source: err,
                }
            })?;
        load_manifest(&manifest_path).await?
    };

    let manifest: Box<RawValue> =
        serde_json::from_str(&manifest_str).map_err(Error::InvalidManifest)?;

    register_manifest(&global, &fqn, tag.as_ref(), manifest).await?;

    tracing::info!(fqn = %fqn, tag = ?tag, "Manifest registered successfully");
    println!("Registered manifest for {fqn}");
    if let Some(tag) = &tag {
        println!("  Version tag: {tag}");
    }

    Ok(())
}

/// Load an authoring manifest from a package directory and convert to legacy format.
///
/// The package directory should contain `manifest.json` (the authoring manifest)
/// plus `sql/` and optionally `functions/` directories with the referenced files.
fn load_and_convert_package(package_dir: &std::path::Path) -> Result<String, Error> {
    // Verify directory exists
    if !package_dir.is_dir() {
        return Err(Error::PackageNotDirectory {
            path: package_dir.to_path_buf(),
        });
    }

    // Read authoring manifest
    let manifest_path = package_dir.join("manifest.json");
    let manifest_content = fs::read_to_string(&manifest_path).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            Error::PackageManifestNotFound {
                path: manifest_path.clone(),
            }
        } else {
            Error::ReadPackageManifest {
                path: manifest_path.clone(),
                source: err,
            }
        }
    })?;

    // Parse as authoring manifest
    let authoring_manifest: AuthoringManifest =
        serde_json::from_str(&manifest_content).map_err(|err| Error::ParseAuthoringManifest {
            path: manifest_path,
            source: err,
        })?;

    // Convert to legacy format using bridge
    let bridge = LegacyBridge::new(package_dir);
    let legacy_json = bridge
        .to_json(&authoring_manifest)
        .map_err(|err| Error::BridgeConversion { source: err })?;

    tracing::debug!(
        namespace = %authoring_manifest.namespace,
        name = %authoring_manifest.name,
        tables = authoring_manifest.tables.len(),
        "Converted authoring manifest to legacy format"
    );

    Ok(legacy_json)
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
    let store =
        store::new(manifest_path.as_url()).map_err(|source| Error::ObjectStoreCreation {
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
            tracing::error!(path = %manifest_path, error = %err, error_source = logging::error_source(&err), "Failed to read manifest");
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
    /// No manifest source provided and no manifest.json in current directory.
    ///
    /// This occurs when neither a FILE argument nor --package flag is provided,
    /// and no manifest.json exists in the current working directory.
    #[error(
        "no manifest source provided; specify FILE, --package, or ensure manifest.json exists in current directory"
    )]
    NoManifestSource,

    /// Failed to get current directory.
    #[error("failed to get current directory")]
    CurrentDir(#[source] std::io::Error),

    /// Invalid default manifest path.
    ///
    /// This occurs when the auto-detected manifest.json path cannot be parsed
    /// as a valid object store URL.
    #[error("failed to parse default manifest path {}", path.display())]
    InvalidDefaultPath {
        path: PathBuf,
        #[source]
        source: ManifestFilePathError,
    },

    /// Package path is not a directory.
    ///
    /// This occurs when --package is provided but the path does not point to
    /// a valid directory.
    #[error("package path is not a directory: {}", path.display())]
    PackageNotDirectory { path: PathBuf },

    /// Package manifest.json not found.
    ///
    /// This occurs when --package is provided but the directory does not
    /// contain a manifest.json file.
    #[error("manifest.json not found in package directory: {}", path.display())]
    PackageManifestNotFound { path: PathBuf },

    /// Failed to read manifest from package directory.
    #[error("failed to read manifest from package: {}", path.display())]
    ReadPackageManifest {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// Failed to parse authoring manifest.
    ///
    /// This occurs when the manifest.json in the package directory is not
    /// a valid authoring manifest format.
    #[error("failed to parse authoring manifest at {}", path.display())]
    ParseAuthoringManifest {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },

    /// Failed to convert authoring manifest to legacy format.
    ///
    /// This occurs when the legacy bridge fails to convert the authoring
    /// manifest to the runtime format. Common causes include missing SQL
    /// or schema files referenced in the manifest.
    #[error("failed to convert manifest to legacy format")]
    BridgeConversion {
        #[source]
        source: dataset_authoring::bridge::ToJsonError,
    },

    /// Manifest file not found at the specified path
    #[error("manifest file not found: {path}")]
    ManifestNotFound { path: String },

    /// Failed to create object store for manifest location
    #[error("failed to create object store for manifest at {path}")]
    ObjectStoreCreation {
        path: String,
        #[source]
        source: ObjectStoreCreationError,
    },

    /// Failed to read manifest from storage
    #[error("failed to read manifest from {path}")]
    ManifestReadError {
        path: String,
        #[source]
        source: ObjectStoreExtError,
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
#[error("invalid manifest file path: {path}")]
pub struct ManifestFilePathError {
    path: String,
    #[source]
    source: ObjectStoreUrlError,
}
