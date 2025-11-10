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
use monitoring::logging;

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

/// Inspect a manifest by retrieving it from content-addressable storage via the admin API.
///
/// Retrieves manifest content and displays it as pretty-printed JSON.
///
/// # Errors
///
/// Returns [`Error`] for invalid hash, manifest not found (404),
/// API errors (400/500), or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, %hash))]
pub async fn run(Args { global, hash }: Args) -> Result<(), Error> {
    tracing::debug!("Retrieving manifest from admin API");

    let manifest_json = get_manifest(&global, &hash).await?;

    // Pretty-print the manifest JSON to stdout
    println!("{}", manifest_json);

    Ok(())
}

/// Retrieve the manifest from the admin API.
///
/// GETs from `/manifests/{hash}` endpoint and returns the raw manifest JSON.
#[tracing::instrument(skip_all)]
async fn get_manifest(global: &GlobalArgs, hash: &Hash) -> Result<String, Error> {
    tracing::debug!("Creating client and retrieving manifest");

    let client = global
        .build_client()
        .map_err(|source| Error::ClientBuildError { source })?;

    let manifest_value = client
        .manifests()
        .get(hash)
        .await
        .map_err(|err| Error::ClientError { source: err })?;

    // Check if the manifest was found
    let manifest = manifest_value.ok_or_else(|| {
        tracing::error!(%hash, "Manifest not found");
        Error::ManifestNotFound {
            hash: hash.to_string(),
        }
    })?;

    // Pretty-print the manifest JSON
    let pretty_json = serde_json::to_string_pretty(&manifest).map_err(|err| {
        tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to pretty-print manifest JSON");
        Error::JsonFormattingError { source: err }
    })?;

    Ok(pretty_json)
}

/// Errors for manifest inspection operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Manifest not found
    #[error("manifest not found: {hash}")]
    ManifestNotFound { hash: String },

    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuildError {
        #[source]
        source: crate::args::BuildClientError,
    },

    /// Client error from ManifestsClient
    #[error("client error")]
    ClientError {
        #[source]
        source: crate::client::manifests::GetError,
    },

    /// Failed to format JSON for display
    #[error("failed to format manifest JSON")]
    JsonFormattingError {
        #[source]
        source: serde_json::Error,
    },
}
