//! Job resume command.
//!
//! Resumes a stopped job through the admin API by:
//! 1. Creating a client for the admin API
//! 2. Using the client's job resume method
//! 3. Displaying success message
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use monitoring::logging;
use worker::job::JobId;

use crate::args::GlobalArgs;

/// Command-line arguments for the `jobs resume` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// The job ID to resume
    pub id: JobId,
}

/// Result of a job resume operation.
#[derive(serde::Serialize)]
struct ResumeResult {
    job_id: JobId,
}

impl std::fmt::Display for ResumeResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(
            f,
            "{} Job {} resume requested",
            console::style("✓").green().bold(),
            self.job_id
        )
    }
}

/// Resume a job by requesting it to resume via the admin API.
///
/// # Errors
///
/// Returns [`Error`] for API errors (400/404/409/500) or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, job_id = %id))]
pub async fn run(Args { global, id }: Args) -> Result<(), Error> {
    let client = global.build_client().map_err(Error::ClientBuildError)?;

    tracing::debug!("Resuming job via admin API");

    client.jobs().resume(&id).await.map_err(|err| {
        tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to resume job");
        match err {
            crate::client::jobs::ResumeError::NotFound(_) => Error::JobNotFound { id },
            _ => Error::ResumeJobError(err),
        }
    })?;
    let result = ResumeResult { job_id: id };
    global.print(&result).map_err(Error::JsonSerialization)?;

    Ok(())
}

/// Errors for job resume operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuildError(#[source] crate::args::BuildClientError),

    /// Job not found
    ///
    /// This occurs when the job ID is valid but no job
    /// record exists with that ID in the metadata database.
    #[error("job not found: {id}")]
    JobNotFound { id: JobId },

    /// Error resuming job via admin API
    ///
    /// This occurs when the resume request fails due to:
    /// - Invalid job ID format
    /// - Network or connection errors
    /// - Metadata database errors
    ///
    /// Note: The resume operation is idempotent - resuming a job that's already
    /// in a Scheduled or Running state returns success.
    #[error("failed to resume job")]
    ResumeJobError(#[source] crate::client::jobs::ResumeError),

    /// Failed to serialize result to JSON
    #[error("failed to serialize result to JSON")]
    JsonSerialization(#[source] serde_json::Error),
}
