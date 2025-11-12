//! Manifest inspection command.
//!
//! Retrieves and displays a manifest by its content-addressable hash through the admin API by:
//! 1. Making a GET request to admin API `/manifests/{hash}` endpoint
//! 2. Retrieving the raw manifest JSON
//! 3. Pretty-printing the manifest to stdout
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use datasets_common::hash::Hash;

use crate::args::GlobalArgs;

/// Command-line arguments for the `manifest inspect` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// Manifest content hash to retrieve
    #[arg(value_name = "HASH", required = true, value_parser = clap::value_parser!(Hash))]
    pub hash: Hash,
}

/// Result wrapper for manifest inspect output.
#[derive(serde::Serialize)]
struct InspectResult {
    #[serde(flatten)]
    data: serde_json::Value,
}

impl std::fmt::Display for InspectResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // For manifest inspect, output pretty JSON as manifests are complex nested structures
        let json = serde_json::to_string_pretty(&self.data).map_err(|_| std::fmt::Error)?;
        write!(f, "{}", json)
    }
}

/// Inspect a manifest by retrieving it from content-addressable storage via the admin API.
///
/// Retrieves manifest content and displays it based on the output format.
///
/// # Errors
///
/// Returns [`Error`] for invalid hash, manifest not found (404),
/// API errors (400/500), or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, %hash))]
pub async fn run(Args { global, hash }: Args) -> Result<(), Error> {
    tracing::debug!("Retrieving manifest from admin API");

    let manifest_value = get_manifest_value(&global, &hash).await?;
    let result = InspectResult {
        data: manifest_value,
    };
    global.print(&result).map_err(Error::JsonFormattingError)?;

    Ok(())
}

/// Retrieve the manifest from the admin API.
///
/// GETs from `/manifests/{hash}` endpoint and returns the manifest value.
#[tracing::instrument(skip_all)]
async fn get_manifest_value(global: &GlobalArgs, hash: &Hash) -> Result<serde_json::Value, Error> {
    tracing::debug!("Creating client and retrieving manifest");

    let client = global.build_client().map_err(Error::ClientBuildError)?;

    let manifest_value = client
        .manifests()
        .get(hash)
        .await
        .map_err(Error::ClientError)?;

    // Check if the manifest was found
    let manifest = manifest_value.ok_or_else(|| {
        tracing::error!(%hash, "Manifest not found");
        Error::ManifestNotFound {
            hash: hash.to_string(),
        }
    })?;

    Ok(manifest)
}

/// Errors for manifest inspection operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Manifest not found
    #[error("manifest not found: {hash}")]
    ManifestNotFound { hash: String },

    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuildError(#[source] crate::args::BuildClientError),

    /// Client error from ManifestsClient
    #[error("client error")]
    ClientError(#[source] crate::client::manifests::GetError),

    /// Failed to format JSON for display
    #[error("failed to format manifest JSON")]
    JsonFormattingError(#[source] serde_json::Error),
}
