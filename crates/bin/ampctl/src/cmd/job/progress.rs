//! Job progress command.
//!
//! Retrieves and displays progress information for all tables written by a job through the
//! admin API by:
//! 1. Creating a client for the admin API
//! 2. Using the client's job get_progress method
//! 3. Displaying the progress as JSON or human-readable output
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use monitoring::logging;
use worker::job::JobId;

use crate::{args::GlobalArgs, client};

/// Command-line arguments for the `jobs progress` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// The job identifier to get progress for
    pub id: JobId,
}

/// Get progress for a job's tables by retrieving it from the admin API.
///
/// Retrieves job progress and displays it based on the output format.
///
/// # Errors
///
/// Returns [`Error`] for API errors (400/404/500) or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, job_id = %id))]
pub async fn run(Args { global, id }: Args) -> Result<(), Error> {
    tracing::debug!("Retrieving job progress from admin API");

    let progress = get_progress(&global, id).await?;
    let result = ProgressResult { data: progress };
    global.print(&result).map_err(Error::JsonFormattingError)?;

    Ok(())
}

/// Retrieve job progress from the admin API.
///
/// Creates a client and uses the job get_progress method.
#[tracing::instrument(skip_all)]
async fn get_progress(global: &GlobalArgs, id: JobId) -> Result<client::jobs::JobProgress, Error> {
    let client = global.build_client().map_err(Error::ClientBuild)?;

    let progress = client.jobs().get_progress(&id).await.map_err(|err| {
        tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to get job progress");
        Error::ClientError(err)
    })?;

    match progress {
        Some(progress) => Ok(progress),
        None => Err(Error::JobNotFound { id }),
    }
}

/// Errors for job progress operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to build admin API client
    ///
    /// This occurs when the client configuration is invalid or the admin URL
    /// is malformed.
    #[error("failed to build admin API client")]
    ClientBuild(#[source] crate::args::BuildClientError),

    /// Client error from the API
    ///
    /// This wraps errors returned by the admin API client, including network
    /// failures, invalid responses, and server errors.
    #[error("client error")]
    ClientError(#[source] crate::client::jobs::GetProgressError),

    /// Job not found in the system
    ///
    /// The specified job ID does not exist in the metadata database.
    /// Verify the job ID is correct using `ampctl job list`.
    #[error("job not found: {id}")]
    JobNotFound { id: JobId },

    /// Failed to format output as JSON
    ///
    /// This occurs when serializing the progress result to JSON fails,
    /// which is unexpected for well-formed data structures.
    #[error("failed to format output")]
    JsonFormattingError(#[source] serde_json::Error),
}

/// Result wrapper for job progress output.
#[derive(serde::Serialize)]
struct ProgressResult {
    #[serde(flatten)]
    data: client::jobs::JobProgress,
}

impl std::fmt::Display for ProgressResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(f, "Job ID: {}", self.data.job_id)?;
        writeln!(f, "Status: {}", self.data.job_status)?;
        writeln!(f)?;

        if self.data.tables.is_empty() {
            writeln!(f, "No tables written yet.")?;
        } else {
            writeln!(f, "Tables:")?;
            let mut sorted_tables: Vec<_> = self.data.tables.iter().collect();
            sorted_tables.sort_by_key(|(name, _)| *name);
            for (name, progress) in sorted_tables {
                writeln!(f, "  {}:", name)?;
                match (progress.start_block, progress.current_block) {
                    (Some(start), Some(end)) => {
                        writeln!(f, "    Block range: {} - {}", start, end)?
                    }
                    _ => writeln!(f, "    Block range: (no data)")?,
                }
                writeln!(f, "    Files: {}", progress.files_count)?;
                writeln!(f, "    Size: {}", format_bytes(progress.total_size_bytes))?;
            }
        }
        Ok(())
    }
}

/// Format bytes into human-readable size.
fn format_bytes(bytes: i64) -> String {
    const KB: i64 = 1024;
    const MB: i64 = KB * 1024;
    const GB: i64 = MB * 1024;
    const TB: i64 = GB * 1024;

    if bytes >= TB {
        format!("{:.1} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}
