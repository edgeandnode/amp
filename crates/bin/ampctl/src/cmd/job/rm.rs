//! Job rm (remove) command.
//!
//! Removes a single job through the admin API by:
//! 1. Creating a client for the admin API
//! 2. Using the client's delete method to remove the job by ID
//! 3. Displaying success message
//!
//! Note: This operation is idempotent - removing a non-existent job returns success.
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use url::Url;
use worker::JobId;

use crate::client::{self, Client};

/// Command-line arguments for the `jobs rm` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    /// The job ID to remove
    pub id: JobId,

    /// The URL of the engine admin interface
    #[arg(long, env = "AMP_ADMIN_URL", default_value = "http://localhost:1610", value_parser = clap::value_parser!(Url))]
    pub admin_url: Url,
}

/// Remove a job by deleting it via the admin API.
///
/// # Errors
///
/// Returns [`Error`] for API errors (400/409/500) or network failures.
#[tracing::instrument(skip_all, fields(%admin_url, job_id = %id))]
pub async fn run(Args { id, admin_url }: Args) -> Result<(), Error> {
    let client = Client::new(admin_url.clone());

    tracing::debug!("Removing job via admin API");

    client.jobs().delete_by_id(&id).await.map_err(|err| {
        tracing::error!(error = %err, "Failed to remove job");
        Error(err)
    })?;

    crate::success!("Job {} removed", id);

    Ok(())
}

/// Error for job rm operations.
///
/// This error occurs when the deletion request fails due to:
/// - Invalid job ID format
/// - Job exists but is not in a terminal state (cannot be deleted)
/// - Network or connection errors
/// - Metadata database errors
///
/// Note: The delete operation is idempotent - deleting a non-existent job returns success.
#[derive(Debug, thiserror::Error)]
#[error("failed to remove job")]
pub struct Error(#[source] pub client::jobs::DeleteByIdError);
