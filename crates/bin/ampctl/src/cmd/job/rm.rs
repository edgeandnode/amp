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

use worker::JobId;

use crate::{args::GlobalArgs, client};

/// Command-line arguments for the `jobs rm` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// The job ID to remove
    pub id: JobId,
}

/// Remove a job by deleting it via the admin API.
///
/// # Errors
///
/// Returns [`Error`] for API errors (400/409/500) or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, job_id = %id))]
pub async fn run(Args { global, id }: Args) -> Result<(), Error> {
    let client = global.build_client()?;

    tracing::debug!("Removing job via admin API");

    client.jobs().delete_by_id(&id).await.map_err(|err| {
        tracing::error!(error = %err, "Failed to remove job");
        Error::DeleteError(err)
    })?;

    crate::success!("Job {} removed", id);

    Ok(())
}

/// Errors for job rm operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuildError {
        #[from]
        source: crate::args::BuildClientError,
    },

    /// Error for job rm operations.
    ///
    /// This error occurs when the deletion request fails due to:
    /// - Invalid job ID format
    /// - Job exists but is not in a terminal state (cannot be deleted)
    /// - Network or connection errors
    /// - Metadata database errors
    ///
    /// Note: The delete operation is idempotent - deleting a non-existent job returns success.
    #[error("failed to remove job")]
    DeleteError(#[source] client::jobs::DeleteByIdError),
}
