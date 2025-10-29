//! Job pruning command.
//!
//! Deletes jobs in bulk, optionally filtered by status, through the admin API by:
//! 1. Creating a client for the admin API
//! 2. Calling the delete endpoint with an optional status filter
//! 3. Displaying success message
//!
//! # Pruning Behavior
//!
//! - **Without status filter**: Deletes all jobs in terminal states (Completed, Stopped, Failed)
//! - **With status filter**: Deletes all jobs matching the specified status
//!
//! This command performs bulk cleanup of jobs. The operation is designed for
//! safe cleanup of terminal jobs or specific status categories.
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Status filter: Optional `--status` flag (terminal, completed, stopped, error)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use url::Url;

use crate::client::{self, Client};

/// Command-line arguments for the `jobs prune` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    /// Optional status filter (terminal, complete, stopped, error)
    #[arg(long, short = 's', value_parser = clap::value_parser!(client::jobs::JobStatusFilter))]
    pub status: Option<client::jobs::JobStatusFilter>,

    /// The URL of the engine admin interface
    #[arg(long, env = "AMP_ADMIN_URL", default_value = "http://localhost:1610", value_parser = clap::value_parser!(Url))]
    pub admin_url: Url,

    /// Bearer token for authenticating requests to the admin API
    #[arg(long, env = "AMP_AUTH_TOKEN")]
    pub auth_token: Option<String>,
}

/// Prune jobs via the admin API.
///
/// Deletes jobs in bulk, optionally filtered by status.
///
/// # Errors
///
/// Returns [`Error`] for API errors (400/500) or network failures.
#[tracing::instrument(skip_all, fields(%admin_url, status = ?status))]
pub async fn run(
    Args {
        status,
        admin_url,
        auth_token,
    }: Args,
) -> Result<(), Error> {
    let mut client_builder = crate::client::build(admin_url.clone());

    if let Some(token) = auth_token {
        client_builder = client_builder.with_bearer_token(
            token
                .parse()
                .map_err(|err| Error::InvalidAuthToken { source: err })?,
        );
    }

    let client = client_builder
        .build()
        .map_err(|err| Error::ClientBuildError { source: err })?;

    prune_jobs(&client, status.as_ref()).await?;

    // Display success message based on status filter
    match status {
        None => crate::success!("Terminal jobs pruned"),
        Some(filter) => crate::success!("Jobs with status \"{}\" pruned", filter),
    }

    Ok(())
}

/// Prune jobs from the admin API.
///
/// DELETEs to `/jobs` endpoint with optional status query parameter.
#[tracing::instrument(skip_all)]
async fn prune_jobs(
    client: &Client,
    status: Option<&client::jobs::JobStatusFilter>,
) -> Result<(), Error> {
    tracing::debug!("Pruning jobs via admin API");

    client.jobs().delete(status).await.map_err(|err| {
        tracing::error!(error = %err, "Failed to prune jobs");
        Error::DeleteError(err)
    })?;

    Ok(())
}

/// Errors for job pruning operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid authentication token
    #[error("invalid authentication token")]
    InvalidAuthToken {
        #[source]
        source: crate::client::auth::BearerTokenError,
    },

    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuildError {
        #[source]
        source: crate::client::BuildError,
    },

    /// Error for job pruning operations.
    #[error("failed to prune jobs")]
    DeleteError(#[source] client::jobs::DeleteByStatusError),
}
