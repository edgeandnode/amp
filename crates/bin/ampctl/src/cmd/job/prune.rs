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

use monitoring::logging;

use crate::{args::GlobalArgs, client::Client};

/// Command-line arguments for the `jobs prune` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// Optional status filter (terminal, complete, stopped, error)
    #[arg(long, short = 's', value_parser = clap::value_parser!(crate::client::jobs::JobStatusFilter))]
    pub status: Option<crate::client::jobs::JobStatusFilter>,
}

/// Result of a job pruning operation.
#[derive(serde::Serialize)]
struct PruneResult {
    status_filter: Option<String>,
}

impl std::fmt::Display for PruneResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let message = match &self.status_filter {
            None => "Terminal jobs pruned".to_string(),
            Some(filter) => format!("Jobs with status \"{}\" pruned", filter),
        };
        writeln!(f, "{} {}", console::style("✓").green().bold(), message)
    }
}

/// Prune jobs via the admin API.
///
/// Deletes jobs in bulk, optionally filtered by status.
///
/// # Errors
///
/// Returns [`Error`] for API errors (400/500) or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, status = ?status))]
pub async fn run(Args { global, status }: Args) -> Result<(), Error> {
    let client = global.build_client().map_err(Error::ClientBuildError)?;

    prune_jobs(&client, status.as_ref()).await?;
    let result = PruneResult {
        status_filter: status.as_ref().map(|f| f.to_string()),
    };
    global.print(&result).map_err(Error::JsonSerialization)?;

    Ok(())
}

/// Prune jobs from the admin API.
///
/// DELETEs to `/jobs` endpoint with optional status query parameter.
#[tracing::instrument(skip_all)]
async fn prune_jobs(
    client: &Client,
    status: Option<&crate::client::jobs::JobStatusFilter>,
) -> Result<(), Error> {
    tracing::debug!("Pruning jobs via admin API");

    client.jobs().delete(status).await.map_err(|err| {
        tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to prune jobs");
        Error::DeleteError(err)
    })?;

    Ok(())
}

/// Errors for job pruning operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuildError(#[source] crate::args::BuildClientError),

    /// Error for job pruning operations.
    #[error("failed to prune jobs")]
    DeleteError(#[source] crate::client::jobs::DeleteByStatusError),

    /// Failed to serialize result to JSON
    #[error("failed to serialize result to JSON")]
    JsonSerialization(#[source] serde_json::Error),
}
