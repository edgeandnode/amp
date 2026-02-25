//! Table revision restore command.
//!
//! Restores a table revision by re-registering its files from object storage:
//! 1. Creating a client for the admin API
//! 2. Using the client's revision restore method
//! 3. Displaying the result
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use monitoring::logging;

use crate::args::GlobalArgs;

/// Command-line arguments for the `table restore` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// The location ID of the revision to restore
    pub location_id: i64,
}

/// Restore a table revision by re-registering its files from object storage.
///
/// Looks up the revision by location ID, lists all files in its object storage
/// directory, reads Parquet metadata, and registers each file in the metadata database.
///
/// # Errors
///
/// Returns [`Error`] for API errors (400/404/500) or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, location_id = %location_id))]
pub async fn run(
    Args {
        global,
        location_id,
    }: Args,
) -> Result<(), Error> {
    let client = global.build_client().map_err(Error::ClientBuildError)?;

    tracing::debug!(%location_id, "restoring table revision via admin API");

    let response = client
        .revisions()
        .restore(location_id)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, error_source = logging::error_source(&err), "failed to restore table revision");
            match err {
                amp_client_admin::revisions::RestoreError::RevisionNotFound(_) => {
                    Error::NotFound { location_id }
                }
                other => Error::RestoreError(other),
            }
        })?;

    let result = RestoreOutput {
        location_id,
        total_files: response.total_files,
    };
    global.print(&result).map_err(Error::JsonSerialization)?;

    Ok(())
}

/// Result of a revision restore operation.
#[derive(serde::Serialize)]
struct RestoreOutput {
    location_id: i64,
    total_files: i32,
}

impl std::fmt::Display for RestoreOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{} Revision {} restored ({} files)",
            console::style("✓").green().bold(),
            self.location_id,
            self.total_files,
        )
    }
}

/// Errors for revision restore operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuildError(#[source] crate::args::BuildClientError),

    /// Revision not found
    ///
    /// This occurs when no revision exists with the specified location ID.
    #[error("revision with location ID '{location_id}' not found")]
    NotFound { location_id: i64 },

    /// Error restoring revision via admin API
    ///
    /// This occurs when the restore request fails due to:
    /// - Invalid location ID format
    /// - Network or connection errors
    /// - File listing or metadata registration errors
    #[error("failed to restore table revision")]
    RestoreError(#[source] amp_client_admin::revisions::RestoreError),

    /// Failed to serialize result to JSON
    #[error("failed to serialize result to JSON")]
    JsonSerialization(#[source] serde_json::Error),
}
