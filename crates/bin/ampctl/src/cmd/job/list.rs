//! Jobs listing command.
//!
//! Retrieves and displays all jobs with pagination support through the admin API by:
//! 1. Creating a client for the admin API
//! 2. Using the client's jobs list method with optional limit, pagination, and status filter
//! 3. Displaying the jobs as JSON
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Limit: `--limit` flag (default: 50, max: 1000)
//! - After: `--after` flag for cursor-based pagination
//! - Status: `--status` flag (default: "active" for non-terminal jobs, can be "all" or comma-separated statuses)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use monitoring::logging;
use worker::job::JobId;

use crate::{args::GlobalArgs, client};

/// Command-line arguments for the `jobs list` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// Maximum number of jobs to return (default: 50, max: 1000)
    #[arg(long, short = 'l')]
    pub limit: Option<usize>,

    /// Job identifier to paginate after
    #[arg(long, short = 'a')]
    pub after: Option<JobId>,

    /// Status filter: "active" (default, non-terminal jobs), "all" (all jobs), or comma-separated status values (e.g., "scheduled,running")
    #[arg(long, short = 's', default_value = "active")]
    pub status: String,
}

/// Result wrapper for jobs list output.
#[derive(serde::Serialize)]
struct ListResult {
    #[serde(flatten)]
    data: crate::client::jobs::JobsResponse,
}

impl std::fmt::Display for ListResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.data.jobs.is_empty() {
            writeln!(f, "No jobs found")
        } else {
            writeln!(f, "Jobs:")?;
            for job in &self.data.jobs {
                writeln!(f, "  {} - {} ({})", job.id, job.status, job.node_id)?;
                writeln!(f, "    Created: {}", job.created_at)?;
                writeln!(f, "    Updated: {}", job.updated_at)?;
            }

            if let Some(next_cursor) = &self.data.next_cursor {
                writeln!(f, "\nNext cursor: {}", next_cursor)?;
            }
            Ok(())
        }
    }
}

/// List all jobs by retrieving them from the admin API.
///
/// Retrieves all jobs with pagination and displays them based on the output format.
///
/// # Errors
///
/// Returns [`Error`] for API errors (400/500) or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, limit = ?limit, after = ?after, status = %status))]
pub async fn run(
    Args {
        global,
        limit,
        after,
        status,
    }: Args,
) -> Result<(), Error> {
    tracing::debug!("Retrieving jobs from admin API");

    let jobs_response = get_jobs(&global, limit, after, &status).await?;
    let result = ListResult {
        data: jobs_response,
    };
    global.print(&result).map_err(Error::JsonFormattingError)?;

    Ok(())
}

/// Retrieve all jobs from the admin API.
///
/// Creates a client and uses the jobs list method.
#[tracing::instrument(skip_all)]
async fn get_jobs(
    global: &GlobalArgs,
    limit: Option<usize>,
    after: Option<JobId>,
    status: &str,
) -> Result<client::jobs::JobsResponse, Error> {
    let client = global.build_client().map_err(Error::ClientBuildError)?;

    let jobs_response = client
        .jobs()
        .list(limit, after, Some(status))
        .await
        .map_err(|err| {
            tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to list jobs");
            Error::ClientError(err)
        })?;

    Ok(jobs_response)
}

/// Errors for jobs listing operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuildError(#[source] crate::args::BuildClientError),

    /// Client error from the API
    #[error("client error")]
    ClientError(#[source] client::jobs::ListError),

    /// Failed to format JSON for display
    #[error("failed to format jobs JSON")]
    JsonFormattingError(#[source] serde_json::Error),
}
