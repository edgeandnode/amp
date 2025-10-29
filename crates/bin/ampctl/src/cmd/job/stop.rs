//! Job stop command.
//!
//! Stops a running job through the admin API by:
//! 1. Creating a client for the admin API
//! 2. Using the client's job stop method
//! 3. Displaying success message
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use url::Url;
use worker::JobId;

use crate::client;

/// Command-line arguments for the `jobs stop` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    /// The job ID to stop
    pub id: JobId,

    /// The URL of the engine admin interface
    #[arg(long, env = "AMP_ADMIN_URL", default_value = "http://localhost:1610", value_parser = clap::value_parser!(Url))]
    pub admin_url: Url,

    /// Bearer token for authenticating requests to the admin API
    #[arg(long, env = "AMP_AUTH_TOKEN")]
    pub auth_token: Option<String>,
}

/// Stop a job by requesting it to stop via the admin API.
///
/// # Errors
///
/// Returns [`Error`] for API errors (400/404/409/500) or network failures.
#[tracing::instrument(skip_all, fields(%admin_url, job_id = %id))]
pub async fn run(
    Args {
        id,
        admin_url,
        auth_token,
    }: Args,
) -> Result<(), Error> {
    let mut client_builder = crate::client::build(admin_url.clone());

    if let Some(token) = auth_token {
        client_builder = client_builder.with_bearer_token(
            token
                .parse()
                .map_err(|err| Error::InvalidAuthToken { source: err })?,
        );
    }

    let client = client_builder
        .build()
        .map_err(|err| Error::ClientBuildError { source: err })?;

    tracing::debug!("Stopping job via admin API");

    client.jobs().stop(&id).await.map_err(|err| {
        tracing::error!(error = %err, "Failed to stop job");
        match err {
            client::jobs::StopError::NotFound(_) => Error::JobNotFound { id },
            _ => Error::StopJobError { source: err },
        }
    })?;

    crate::success!("Job {} stop requested", id);

    Ok(())
}

/// Errors for job stop operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid authentication token
    #[error("invalid authentication token")]
    InvalidAuthToken {
        #[source]
        source: crate::client::auth::BearerTokenError,
    },

    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuildError {
        #[source]
        source: crate::client::BuildError,
    },

    /// Job not found
    ///
    /// This occurs when the job ID is valid but no job
    /// record exists with that ID in the metadata database.
    #[error("job not found: {id}")]
    JobNotFound { id: JobId },

    /// Error stopping job via admin API
    ///
    /// This occurs when the stop request fails due to:
    /// - Invalid job ID format
    /// - Network or connection errors
    /// - Metadata database errors
    ///
    /// Note: The stop operation is idempotent - stopping a job that's already
    /// in a terminal state (Stopped, Completed, Failed) returns success.
    #[error("failed to stop job")]
    StopJobError { source: client::jobs::StopError },
}
