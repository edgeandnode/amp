//! Manifest pruning command.
//!
//! Deletes all orphaned manifests (manifests not linked to any datasets) through the admin API by:
//! 1. Making a DELETE request to admin API `/manifests` endpoint
//! 2. Receiving count of deleted manifests
//!
//! This command performs bulk cleanup of unused manifests.
//! Individual deletion failures are logged but don't fail the entire operation.
//! The operation is idempotent - safe to call repeatedly.
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use url::Url;

use crate::client::manifests::PruneError;

/// Command-line arguments for the `manifest prune` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    /// The URL of the engine admin interface
    #[arg(long, env = "AMP_ADMIN_URL", default_value = "http://localhost:1610", value_parser = clap::value_parser!(Url))]
    pub admin_url: Url,

    /// Bearer token for authenticating requests to the admin API
    #[arg(long, env = "AMP_AUTH_TOKEN")]
    pub auth_token: Option<String>,
}

/// Prune all orphaned manifests via the admin API.
///
/// Deletes all manifests that are not linked to any datasets.
///
/// # Errors
///
/// Returns [`Error`] for API errors (400/500) or network failures.
#[tracing::instrument(skip_all, fields(%admin_url))]
pub async fn run(
    Args {
        admin_url,
        auth_token,
    }: Args,
) -> Result<(), Error> {
    tracing::debug!("Pruning orphaned manifests via admin API");

    let deleted_count = prune_manifests(&admin_url, auth_token.as_deref()).await?;

    crate::success!("Pruned {} orphaned manifest(s)", deleted_count);

    Ok(())
}

/// Prune orphaned manifests from the admin API.
///
/// DELETEs to `/manifests` endpoint using the admin API client.
#[tracing::instrument(skip_all)]
async fn prune_manifests(admin_url: &Url, auth_token: Option<&str>) -> Result<usize, Error> {
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

    let response = match client.manifests().prune().await {
        Ok(response) => response,
        Err(err @ PruneError::ListOrphanedManifestsError(_)) => {
            return Err(Error::ListOrphanedManifestsError(err));
        }
        Err(err @ PruneError::Network { .. }) => return Err(Error::Network(err)),
        Err(err @ PruneError::UnexpectedResponse { .. }) => {
            return Err(Error::UnexpectedResponse(err));
        }
    };

    Ok(response.deleted_count)
}

/// Errors for manifest pruning operations.
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

    /// Failed to list orphaned manifests from the database
    ///
    /// This occurs when:
    /// - Database connection is lost during orphaned manifest query
    /// - Failed to query orphaned manifests from metadata database
    /// - Database permissions prevent the query operation
    /// - SQL execution errors during the orphaned manifest lookup
    #[error("list orphaned manifests error")]
    ListOrphanedManifestsError(#[source] PruneError),

    /// Network or connection error communicating with admin API
    ///
    /// This occurs when:
    /// - Cannot connect to the admin API server
    /// - Network timeout during the prune request
    /// - DNS resolution fails for the admin URL
    /// - TLS/SSL connection issues
    #[error("network error")]
    Network(#[source] PruneError),

    /// Unexpected response from the admin API
    ///
    /// This occurs when:
    /// - API returns an unexpected HTTP status code
    /// - Response body cannot be parsed as expected JSON format
    /// - API responds with an error not covered by specific error cases
    /// - Server returns malformed or incomplete response
    #[error("unexpected response")]
    UnexpectedResponse(#[source] PruneError),
}
