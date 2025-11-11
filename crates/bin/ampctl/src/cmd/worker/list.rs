//! Workers listing command.
//!
//! Retrieves and displays all workers through the admin API by:
//! 1. Creating a client for the admin API
//! 2. Using the client's workers list method
//! 3. Displaying the workers as JSON
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use monitoring::logging;

use crate::{args::GlobalArgs, client};

/// Command-line arguments for the `worker list` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,
}

/// List all workers by retrieving them from the admin API.
///
/// Retrieves all workers and displays them as JSON.
///
/// # Errors
///
/// Returns [`Error`] for API errors (500) or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url))]
pub async fn run(Args { global }: Args) -> Result<(), Error> {
    tracing::debug!("Retrieving workers from admin API");

    let workers_response = get_workers(&global).await?;

    let json = serde_json::to_string_pretty(&workers_response).map_err(|err| {
        tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to serialize workers to JSON");
        Error::JsonFormattingError { source: err }
    })?;
    println!("{}", json);

    Ok(())
}

/// Retrieve all workers from the admin API.
///
/// Creates a client and uses the workers list method.
#[tracing::instrument(skip_all)]
async fn get_workers(global: &GlobalArgs) -> Result<client::workers::WorkersResponse, Error> {
    let client = global.build_client().map_err(Error::ClientBuildError)?;

    let workers_response = client.workers().list().await.map_err(|err| {
        tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to list workers");
        Error::ClientError { source: err }
    })?;

    Ok(workers_response)
}

/// Errors for workers listing operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuildError(#[source] crate::args::BuildClientError),

    /// Client error from the API
    #[error("client error")]
    ClientError { source: client::workers::ListError },

    /// Failed to format JSON for display
    #[error("failed to format workers JSON")]
    JsonFormattingError { source: serde_json::Error },
}
