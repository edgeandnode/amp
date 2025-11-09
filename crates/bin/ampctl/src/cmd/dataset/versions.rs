//! Dataset versions listing command.
//!
//! Retrieves and displays all versions of a dataset through the admin API by:
//! 1. Creating a client for the admin API
//! 2. Using the client's dataset list_versions method
//! 3. Displaying the versions as JSON
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use datasets_common::fqn::FullyQualifiedName;
use monitoring::logging;

use crate::{args::GlobalArgs, client};

/// Command-line arguments for the `dataset versions` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// The fully qualified dataset name (format: namespace/name)
    pub fqn: FullyQualifiedName,
}

/// List all versions of a dataset by retrieving them from the admin API.
///
/// Retrieves version information and displays it as JSON.
///
/// # Errors
///
/// Returns [`Error`] for API errors (400/500) or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, fqn = %fqn))]
pub async fn run(Args { global, fqn }: Args) -> Result<(), Error> {
    tracing::debug!("Retrieving dataset versions from admin API");

    let versions_response = get_versions(&global, &fqn).await?;

    let json = serde_json::to_string_pretty(&versions_response).map_err(|err| {
        tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to serialize versions to JSON");
        Error::JsonFormattingError(err)
    })?;
    println!("{}", json);

    Ok(())
}

/// Retrieve all versions of a dataset from the admin API.
///
/// Creates a client and uses the dataset list_versions method.
#[tracing::instrument(skip_all)]
async fn get_versions(
    global: &GlobalArgs,
    fqn: &FullyQualifiedName,
) -> Result<client::datasets::VersionsResponse, Error> {
    let client = global.build_client().map_err(Error::ClientBuildError)?;

    let versions_response = client.datasets().list_versions(fqn).await.map_err(|err| {
        tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to list versions");
        Error::ClientError(err)
    })?;

    Ok(versions_response)
}

/// Errors for dataset versions listing operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuildError(#[source] crate::args::BuildClientError),

    /// Client error from the API
    #[error("client error")]
    ClientError(#[source] client::datasets::ListVersionsError),

    /// Failed to format JSON for display
    #[error("failed to format versions JSON")]
    JsonFormattingError(#[source] serde_json::Error),
}
