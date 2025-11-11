//! Job inspect command.
//!
//! Retrieves and displays detailed information about a specific job through the admin API by:
//! 1. Creating a client for the admin API
//! 2. Using the client's job get method
//! 3. Displaying the job as JSON
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use monitoring::logging;
use worker::job::JobId;

use crate::{args::GlobalArgs, client};

/// Command-line arguments for the `jobs inspect` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// The job identifier to inspect
    pub id: JobId,
}

/// Result wrapper for job inspect output.
#[derive(serde::Serialize)]
struct InspectResult {
    #[serde(flatten)]
    data: client::jobs::JobInfo,
}

impl std::fmt::Display for InspectResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(f, "Job ID: {}", self.data.id)?;
        writeln!(f, "Status: {}", self.data.status)?;
        writeln!(f, "Worker: {}", self.data.node_id)?;
        writeln!(f, "Created: {}", self.data.created_at)?;
        writeln!(f, "Updated: {}", self.data.updated_at)?;
        writeln!(f)?;
        writeln!(f, "Descriptor:")?;
        let descriptor_json =
            serde_json::to_string_pretty(&self.data.descriptor).map_err(|_| std::fmt::Error)?;
        write!(f, "{}", descriptor_json)
    }
}

/// Inspect job details by retrieving them from the admin API.
///
/// Retrieves job information and displays it based on the output format.
///
/// # Errors
///
/// Returns [`Error`] for API errors (400/404/500) or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, job_id = %id))]
pub async fn run(Args { global, id }: Args) -> Result<(), Error> {
    tracing::debug!("Retrieving job from admin API");

    let job = get_job(&global, id).await?;
    let result = InspectResult { data: job };
    global.print(&result).map_err(Error::JsonFormattingError)?;

    Ok(())
}

/// Retrieve a job from the admin API.
///
/// Creates a client and uses the job get method.
#[tracing::instrument(skip_all)]
async fn get_job(global: &GlobalArgs, id: JobId) -> Result<client::jobs::JobInfo, Error> {
    let client = global.build_client().map_err(Error::ClientBuild)?;

    let job = client.jobs().get(&id).await.map_err(|err| {
        tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to get job");
        Error::ClientError(err)
    })?;

    match job {
        Some(job) => Ok(job),
        None => Err(Error::JobNotFound { id }),
    }
}

/// Errors for job inspect operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuild(#[source] crate::args::BuildClientError),

    /// Client error from the API
    #[error("client error")]
    ClientError(#[source] crate::client::jobs::GetError),

    /// Job not found
    #[error("job not found: {id}")]
    JobNotFound { id: JobId },

    /// Failed to format JSON for display
    #[error("failed to format job JSON")]
    JsonFormattingError(#[source] serde_json::Error),
}
