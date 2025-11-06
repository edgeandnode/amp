//! Dataset manifest retrieval command.
//!
//! Retrieves and displays the manifest JSON for a dataset through the admin API by:
//! 1. Creating a client for the admin API
//! 2. Using the client's dataset get_manifest method
//! 3. Displaying the manifest as pretty-printed JSON
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use datasets_common::reference::Reference;

use crate::{args::GlobalArgs, client};

/// Command-line arguments for the `dataset manifest` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// The dataset reference (format: namespace/name@revision)
    /// If no revision is specified, "latest" is used
    pub reference: Reference,
}

/// Retrieve dataset manifest by fetching it from the admin API.
///
/// Retrieves the manifest JSON and displays it.
///
/// # Errors
///
/// Returns [`Error`] for API errors (404/500) or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, reference = %reference))]
pub async fn run(Args { global, reference }: Args) -> Result<(), Error> {
    tracing::debug!("Retrieving dataset manifest from admin API");

    let manifest = get_manifest(&global, &reference).await?;

    let json = serde_json::to_string_pretty(&manifest).map_err(|err| {
        tracing::error!(error = %err, "Failed to serialize manifest to JSON");
        Error::JsonFormattingError(err)
    })?;
    println!("{}", json);

    Ok(())
}

/// Retrieve a dataset manifest from the admin API.
///
/// Creates a client and uses the dataset get_manifest method.
#[tracing::instrument(skip_all)]
async fn get_manifest(
    global: &GlobalArgs,
    reference: &Reference,
) -> Result<serde_json::Value, Error> {
    let client = global.build_client().map_err(Error::ClientBuildError)?;

    let manifest = client
        .datasets()
        .get_manifest(reference)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "Failed to get manifest");
            Error::ClientError(err)
        })?;

    match manifest {
        Some(manifest) => Ok(manifest),
        None => Err(Error::ManifestNotFound {
            reference: reference.clone(),
        }),
    }
}

/// Errors for dataset manifest operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuildError(#[source] crate::args::BuildClientError),

    /// Client error from the API
    #[error("client error")]
    ClientError(#[source] client::datasets::GetManifestError),

    /// Manifest not found
    #[error("manifest not found: {reference}")]
    ManifestNotFound { reference: Reference },

    /// Failed to format JSON for display
    #[error("failed to format manifest JSON")]
    JsonFormattingError(#[source] serde_json::Error),
}
