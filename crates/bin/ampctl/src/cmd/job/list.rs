//! Jobs listing command.
//!
//! Retrieves and displays all jobs with pagination support through the admin API by:
//! 1. Creating a client for the admin API
//! 2. Using the client's jobs list method with optional limit and pagination
//! 3. Displaying the jobs as JSON
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Limit: `--limit` flag (default: 50, max: 1000)
//! - After: `--after` flag for cursor-based pagination
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use url::Url;
use worker::JobId;

use crate::client;

/// Command-line arguments for the `jobs list` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    /// Maximum number of jobs to return (default: 50, max: 1000)
    #[arg(long, short = 'l')]
    pub limit: Option<usize>,

    /// Job identifier to paginate after
    #[arg(long, short = 'a')]
    pub after: Option<JobId>,

    /// The URL of the engine admin interface
    #[arg(long, env = "AMP_ADMIN_URL", default_value = "http://localhost:1610", value_parser = clap::value_parser!(Url))]
    pub admin_url: Url,

    /// Bearer token for authenticating requests to the admin API
    #[arg(long, env = "AMP_AUTH_TOKEN")]
    pub auth_token: Option<String>,
}

/// List all jobs by retrieving them from the admin API.
///
/// Retrieves all jobs with pagination and displays them as JSON.
///
/// # Errors
///
/// Returns [`Error`] for API errors (400/500) or network failures.
#[tracing::instrument(skip_all, fields(%admin_url, limit = ?limit, after = ?after))]
pub async fn run(
    Args {
        limit,
        after,
        admin_url,
        auth_token,
    }: Args,
) -> Result<(), Error> {
    tracing::debug!("Retrieving jobs from admin API");

    let jobs_response = get_jobs(&admin_url, auth_token.as_deref(), limit, after).await?;

    let json = serde_json::to_string_pretty(&jobs_response).map_err(|err| {
        tracing::error!(error = %err, "Failed to serialize jobs to JSON");
        Error::JsonFormattingError { source: err }
    })?;
    println!("{}", json);

    Ok(())
}

/// Retrieve all jobs from the admin API.
///
/// Creates a client and uses the jobs list method.
#[tracing::instrument(skip_all)]
async fn get_jobs(
    admin_url: &Url,
    auth_token: Option<&str>,
    limit: Option<usize>,
    after: Option<JobId>,
) -> Result<client::jobs::JobsResponse, Error> {
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

    let jobs_response = client.jobs().list(limit, after).await.map_err(|err| {
        tracing::error!(error = %err, "Failed to list jobs");
        Error::ClientError { source: err }
    })?;

    Ok(jobs_response)
}

/// Errors for jobs listing operations.
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

    /// Client error from the API
    #[error("client error")]
    ClientError { source: client::jobs::ListError },

    /// Failed to format JSON for display
    #[error("failed to format jobs JSON")]
    JsonFormattingError { source: serde_json::Error },
}
