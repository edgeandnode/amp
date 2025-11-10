//! Manifests listing command.
//!
//! Retrieves and displays all registered manifests through the admin API by:
//! 1. Creating a client for the admin API
//! 2. Using the client's manifests list_all method
//! 3. Displaying the manifests as JSON
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use monitoring::logging;

use crate::{args::GlobalArgs, client};

/// Command-line arguments for the `manifest list` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,
}

/// List all manifests by retrieving them from the admin API.
///
/// Retrieves all manifests and displays them as JSON.
///
/// # Errors
///
/// Returns [`Error`] for API errors (500) or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url))]
pub async fn run(Args { global }: Args) -> Result<(), Error> {
    tracing::debug!("Retrieving manifests from admin API");

    let manifests_response = get_manifests(&global).await?;

    let json = serde_json::to_string_pretty(&manifests_response).map_err(|err| {
        tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to serialize manifests to JSON");
        Error::JsonFormattingError(err)
    })?;
    println!("{}", json);

    Ok(())
}

/// Retrieve all manifests from the admin API.
///
/// Creates a client and uses the manifests list_all method.
#[tracing::instrument(skip_all)]
async fn get_manifests(global: &GlobalArgs) -> Result<client::manifests::ManifestsResponse, Error> {
    let client = global.build_client().map_err(Error::ClientBuildError)?;

    let manifests_response = client.manifests().list_all().await.map_err(|err| {
        tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to list manifests");
        Error::ClientError(err)
    })?;

    Ok(manifests_response)
}

/// Errors for manifests listing operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuildError(#[source] crate::args::BuildClientError),

    /// Client error from the API
    #[error("client error")]
    ClientError(#[source] client::manifests::ListAllError),

    /// Failed to format JSON for display
    #[error("failed to format manifests JSON")]
    JsonFormattingError(#[source] serde_json::Error),
}
