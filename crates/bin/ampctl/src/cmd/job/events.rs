//! Job events command.
//!
//! Retrieves and displays lifecycle events for a job through the admin API by:
//! 1. Creating a client for the admin API
//! 2. Using the client's job get_events method
//! 3. Displaying the events as JSON or human-readable output
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use amp_client_admin as client;
use amp_worker_core::jobs::job_id::JobId;
use monitoring::logging;

use crate::args::GlobalArgs;

/// Command-line arguments for the `jobs events` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// The job identifier to get events for
    pub job_id: JobId,
}

/// Get lifecycle events for a job by retrieving them from the admin API.
///
/// Retrieves job events and displays them based on the output format.
///
/// # Errors
///
/// Returns [`Error`] for API errors (400/500) or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, job_id = %job_id))]
pub async fn run(Args { global, job_id }: Args) -> Result<(), Error> {
    tracing::debug!("retrieving job events from admin API");

    let events = get_events(&global, job_id).await?;
    let result = EventsResult { data: events };
    global.print(&result).map_err(Error::JsonFormattingError)?;

    Ok(())
}

/// Retrieve job events from the admin API.
///
/// Creates a client and uses the job get_events method.
#[tracing::instrument(skip_all)]
async fn get_events(
    global: &GlobalArgs,
    job_id: JobId,
) -> Result<client::jobs::JobEventsResponse, Error> {
    let client = global.build_client().map_err(Error::ClientBuild)?;

    let events = client.jobs().get_events(&job_id).await.map_err(|err| {
        tracing::error!(
            error = %err,
            error_source = logging::error_source(&err),
            "failed to get job events"
        );
        Error::ClientError(err)
    })?;

    match events {
        Some(events) => Ok(events),
        None => Err(Error::JobNotFound { job_id }),
    }
}

/// Result wrapper for job events output.
#[derive(serde::Serialize)]
struct EventsResult {
    #[serde(flatten)]
    data: client::jobs::JobEventsResponse,
}

impl std::fmt::Display for EventsResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(f, "Job ID: {}", self.data.job_id)?;
        writeln!(f)?;

        if self.data.events.is_empty() {
            writeln!(f, "No events recorded.")?;
        } else {
            writeln!(f, "Events:")?;
            for event in &self.data.events {
                writeln!(
                    f,
                    "  [{}] {} - {} (node: {})",
                    event.id, event.created_at, event.event_type, event.node_id
                )?;
            }
        }
        Ok(())
    }
}

/// Errors for job events operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to build admin API client
    ///
    /// This occurs when the client configuration is invalid or the admin URL
    /// is malformed.
    #[error("failed to build admin API client")]
    ClientBuild(#[source] crate::args::BuildClientError),

    /// Job not found in the system
    ///
    /// The specified job ID does not exist in the metadata database.
    /// Verify the job ID is correct using `ampctl job list`.
    #[error("job not found: {job_id}")]
    JobNotFound { job_id: JobId },

    /// Client error from the API
    ///
    /// This wraps errors returned by the admin API client, including network
    /// failures, invalid responses, and server errors.
    #[error("client error")]
    ClientError(#[source] amp_client_admin::jobs::GetEventsError),

    /// Failed to format output as JSON
    ///
    /// This occurs when serializing the events result to JSON fails,
    /// which is unexpected for well-formed data structures.
    #[error("failed to format output")]
    JsonFormattingError(#[source] serde_json::Error),
}
