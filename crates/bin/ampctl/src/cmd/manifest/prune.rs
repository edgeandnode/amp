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

use crate::{args::GlobalArgs, client};

/// Command-line arguments for the `manifest prune` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,
}

/// Result of a manifest pruning operation.
#[derive(serde::Serialize)]
struct PruneResult {
    deleted_count: usize,
}

impl std::fmt::Display for PruneResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(
            f,
            "{} Pruned {} orphaned manifest(s)",
            console::style("✓").green().bold(),
            self.deleted_count
        )
    }
}

/// Prune all orphaned manifests via the admin API.
///
/// Deletes all manifests that are not linked to any datasets.
///
/// # Errors
///
/// Returns [`Error`] for API errors (400/500) or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url))]
pub async fn run(Args { global }: Args) -> Result<(), Error> {
    tracing::debug!("Pruning orphaned manifests via admin API");

    let deleted_count = prune_manifests(&global).await?;
    let result = PruneResult { deleted_count };
    global.print(&result).map_err(Error::JsonSerialization)?;

    Ok(())
}

/// Prune orphaned manifests from the admin API.
///
/// DELETEs to `/manifests` endpoint using the admin API client.
#[tracing::instrument(skip_all)]
async fn prune_manifests(global: &GlobalArgs) -> Result<usize, Error> {
    let client = global.build_client().map_err(Error::ClientBuildError)?;

    let response = match client.manifests().prune().await {
        Ok(response) => response,
        Err(err @ client::manifests::PruneError::ListOrphanedManifestsError(_)) => {
            return Err(Error::ListOrphanedManifestsError(err));
        }
        Err(err @ client::manifests::PruneError::Network { .. }) => {
            return Err(Error::Network(err));
        }
        Err(err @ client::manifests::PruneError::UnexpectedResponse { .. }) => {
            return Err(Error::UnexpectedResponse(err));
        }
    };

    Ok(response.deleted_count)
}

/// Errors for manifest pruning operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuildError(#[source] crate::args::BuildClientError),

    /// Failed to list orphaned manifests from the database
    ///
    /// This occurs when:
    /// - Database connection is lost during orphaned manifest query
    /// - Failed to query orphaned manifests from metadata database
    /// - Database permissions prevent the query operation
    /// - SQL execution errors during the orphaned manifest lookup
    #[error("list orphaned manifests error")]
    ListOrphanedManifestsError(#[source] crate::client::manifests::PruneError),

    /// Network or connection error communicating with admin API
    ///
    /// This occurs when:
    /// - Cannot connect to the admin API server
    /// - Network timeout during the prune request
    /// - DNS resolution fails for the admin URL
    /// - TLS/SSL connection issues
    #[error("network error")]
    Network(#[source] crate::client::manifests::PruneError),

    /// Unexpected response from the admin API
    ///
    /// This occurs when:
    /// - API returns an unexpected HTTP status code
    /// - Response body cannot be parsed as expected JSON format
    /// - API responds with an error not covered by specific error cases
    /// - Server returns malformed or incomplete response
    #[error("unexpected response")]
    UnexpectedResponse(#[source] crate::client::manifests::PruneError),

    /// Failed to serialize result to JSON
    #[error("failed to serialize result to JSON")]
    JsonSerialization(#[source] serde_json::Error),
}
