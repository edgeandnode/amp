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

use url::Url;
use worker::JobId;

use crate::client::{self, Client};

/// Command-line arguments for the `jobs inspect` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    /// The job identifier to inspect
    pub id: JobId,

    /// The URL of the engine admin interface
    #[arg(long, env = "AMP_ADMIN_URL", default_value = "http://localhost:1610", value_parser = clap::value_parser!(Url))]
    pub admin_url: Url,
}

/// Inspect job details by retrieving them from the admin API.
///
/// Retrieves job information and displays it as JSON.
///
/// # Errors
///
/// Returns [`Error`] for API errors (400/404/500) or network failures.
#[tracing::instrument(skip_all, fields(%admin_url, job_id = %id))]
pub async fn run(Args { id, admin_url }: Args) -> Result<(), Error> {
    tracing::debug!("Retrieving job from admin API");

    let job = get_job(&admin_url, id).await?;

    let json = serde_json::to_string_pretty(&job).map_err(|err| {
        tracing::error!(error = %err, "Failed to serialize job to JSON");
        Error::JsonFormattingError(err)
    })?;
    println!("{}", json);

    Ok(())
}

/// Retrieve a job from the admin API.
///
/// Creates a client and uses the job get method.
#[tracing::instrument(skip_all)]
async fn get_job(admin_url: &Url, id: JobId) -> Result<client::jobs::JobInfo, Error> {
    let client = Client::new(admin_url.clone());
    let job = client.jobs().get(&id).await.map_err(|err| {
        tracing::error!(error = %err, "Failed to get job");
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
    /// Client error from the API
    #[error("client error")]
    ClientError(#[source] client::jobs::GetError),

    /// Job not found
    #[error("job not found: {id}")]
    JobNotFound { id: JobId },

    /// Failed to format JSON for display
    #[error("failed to format job JSON")]
    JsonFormattingError(#[source] serde_json::Error),
}
