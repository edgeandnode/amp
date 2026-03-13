//! Job events command.
//!
//! Retrieves and displays lifecycle events for a job through the admin API by:
//! 1. Creating a client for the admin API
//! 2. Using the client's job get_events or get_event method
//! 3. Displaying the events as JSON or human-readable output
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use amp_client_admin as client;
use amp_worker_core::jobs::job_id::JobId;
use monitoring::logging;

use crate::{
    args::GlobalArgs,
    ui::{format_error_detail, json_display_value, snake_to_title, write_aligned_fields},
};

/// Command-line arguments for the `jobs events` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// The job identifier to get events for
    pub job_id: JobId,

    /// Optional event ID to retrieve a single event with full details
    #[arg(long)]
    pub event_id: Option<i64>,
}

/// Get lifecycle events for a job by retrieving them from the admin API.
///
/// When `--event-id` is provided, retrieves a single event with full details
/// including the detail payload. Otherwise, retrieves all events for the job.
///
/// # Errors
///
/// Returns [`Error`] for API errors (400/500) or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, job_id = %job_id))]
pub async fn run(
    Args {
        global,
        job_id,
        event_id,
    }: Args,
) -> Result<(), Error> {
    tracing::debug!("retrieving job events from admin API");

    match event_id {
        Some(event_id) => {
            let event = get_event(&global, job_id, event_id).await?;
            let result = SingleEventResult { data: event };
            global.print(&result).map_err(Error::JsonFormattingError)?;
        }
        None => {
            let events = get_events(&global, job_id).await?;
            let result = EventsResult { data: events };
            global.print(&result).map_err(Error::JsonFormattingError)?;
        }
    }

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

/// Retrieve a single job event from the admin API.
///
/// Creates a client and uses the job get_event method.
#[tracing::instrument(skip_all)]
async fn get_event(
    global: &GlobalArgs,
    job_id: JobId,
    event_id: i64,
) -> Result<client::jobs::JobEventDetailResponse, Error> {
    let client = global.build_client().map_err(Error::ClientBuild)?;

    let event = client
        .jobs()
        .get_event(&job_id, event_id)
        .await
        .map_err(|err| {
            tracing::error!(
                error = %err,
                error_source = logging::error_source(&err),
                "failed to get job event"
            );
            Error::GetEventError(err)
        })?;

    match event {
        Some(event) => Ok(event),
        None => Err(Error::EventNotFound { job_id, event_id }),
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

/// Result wrapper for a single event output.
#[derive(serde::Serialize)]
struct SingleEventResult {
    #[serde(flatten)]
    data: client::jobs::JobEventDetailResponse,
}

impl std::fmt::Display for SingleEventResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(f, "Event ID:   {}", self.data.id)?;
        writeln!(f, "Job ID:     {}", self.data.job_id)?;
        writeln!(f, "Created At: {}", self.data.created_at)?;
        writeln!(f, "Node ID:    {}", self.data.node_id)?;
        writeln!(f, "Event Type: {}", self.data.event_type)?;

        if let Some(detail) = &self.data.detail {
            let is_error = self.data.event_type == "ERROR" || self.data.event_type == "FATAL";
            if is_error {
                format_error_detail(f, detail)?;
            } else {
                format_descriptor_detail(f, detail)?;
            }
        }

        Ok(())
    }
}

/// Format descriptor detail as aligned key-value pairs.
fn format_descriptor_detail(
    f: &mut std::fmt::Formatter,
    detail: &serde_json::Value,
) -> std::fmt::Result {
    writeln!(f)?;
    writeln!(f, "Descriptor")?;
    writeln!(f, "──────────")?;
    if let Some(obj) = detail.as_object() {
        let fields: Vec<(String, String)> = obj
            .iter()
            .map(|(key, value)| (snake_to_title(key), json_display_value(value)))
            .collect();
        write_aligned_fields(f, &fields, "")?;
    } else {
        let pretty = serde_json::to_string_pretty(detail).unwrap_or_else(|_| detail.to_string());
        for line in pretty.lines() {
            writeln!(f, "  {line}")?;
        }
    }
    Ok(())
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

    /// Event not found for the given job
    ///
    /// The specified event ID does not exist for the given job.
    #[error("event {event_id} not found for job {job_id}")]
    EventNotFound { job_id: JobId, event_id: i64 },

    /// Client error from the list events API
    ///
    /// This wraps errors returned by the admin API client, including network
    /// failures, invalid responses, and server errors.
    #[error("client error")]
    ClientError(#[source] amp_client_admin::jobs::GetEventsError),

    /// Client error from the get event API
    ///
    /// This wraps errors returned by the admin API client for single event retrieval.
    #[error("client error")]
    GetEventError(#[source] amp_client_admin::jobs::GetEventError),

    /// Failed to format output as JSON
    ///
    /// This occurs when serializing the events result to JSON fails,
    /// which is unexpected for well-formed data structures.
    #[error("failed to format output")]
    JsonFormattingError(#[source] serde_json::Error),
}
