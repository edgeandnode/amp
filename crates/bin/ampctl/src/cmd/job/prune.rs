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
}

/// Prune jobs via the admin API.
///
/// Deletes jobs in bulk, optionally filtered by status.
///
/// # Errors
///
/// Returns [`Error`] for API errors (400/500) or network failures.
#[tracing::instrument(skip_all, fields(%admin_url, status = ?status))]
pub async fn run(Args { status, admin_url }: Args) -> Result<(), Error> {
    let client = Client::new(admin_url.clone());

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
        Error(err)
    })?;

    Ok(())
}

/// Error for job pruning operations.
#[derive(Debug, thiserror::Error)]
#[error("failed to prune jobs")]
pub struct Error(#[source] pub client::jobs::DeleteByStatusError);
