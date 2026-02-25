//! Table revision delete command.
//!
//! Deletes a table revision by its location ID:
//! 1. Creating a client for the admin API
//! 2. Prompting for user confirmation (unless `--force` is set)
//! 3. Using the client's revision delete method
//! 4. Displaying the result
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use monitoring::logging;

use crate::{args::GlobalArgs, ui::prompt_for_confirmation};

/// Command-line arguments for the `table delete` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// The location ID of the revision to delete
    pub location_id: i64,

    /// Skip confirmation prompt
    #[arg(long, short = 'f')]
    pub force: bool,
}

/// Delete a table revision by location ID.
///
/// The revision must be inactive before it can be deleted. Deleting a revision
/// permanently removes it and all associated file metadata from the database.
///
/// # Errors
///
/// Returns [`Error`] for API errors (400/404/409/500) or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, location_id = %location_id))]
pub async fn run(
    Args {
        global,
        location_id,
        force,
    }: Args,
) -> Result<(), Error> {
    let client = global.build_client().map_err(Error::ClientBuildError)?;

    if !force {
        let prompt = format!(
            "{} This revision's dataset can't be queried after deletion. Delete revision {location_id}?",
            console::style("⚠").yellow().bold(),
        );
        let confirmed = prompt_for_confirmation(&prompt).map_err(Error::ConfirmationPrompt)?;

        if !confirmed {
            crate::info!("Deletion cancelled");
            return Ok(());
        }
    }

    tracing::debug!(location_id = %location_id, "delete table revision requested");

    client
        .revisions()
        .delete(location_id)
        .await
        .map_err(|err| {
            tracing::error!(
                error = %err,
                error_source = logging::error_source(&err),
                "failed to delete table revision"
            );
            match err {
                amp_client_admin::revisions::DeleteError::RevisionNotFound(_) => {
                    Error::NotFound { location_id }
                }
                amp_client_admin::revisions::DeleteError::RevisionIsActive(_) => {
                    Error::RevisionIsActive { location_id }
                }
                amp_client_admin::revisions::DeleteError::WriterJobNotTerminal(_) => {
                    Error::WriterJobNotTerminal { location_id }
                }
                other => Error::DeleteError(other),
            }
        })?;

    let result = DeleteOutput { location_id };
    global.print(&result).map_err(Error::JsonSerialization)?;

    Ok(())
}

/// Result of a revision delete operation.
#[derive(serde::Serialize)]
struct DeleteOutput {
    location_id: i64,
}

impl std::fmt::Display for DeleteOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{} Revision {} deleted",
            console::style("✓").green().bold(),
            self.location_id,
        )
    }
}

/// Errors for revision delete operations.
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

    /// Revision is active
    ///
    /// This occurs when the revision is currently active and cannot be deleted.
    /// Deactivate the revision before deleting it.
    #[error("revision with location ID '{location_id}' is active and cannot be deleted")]
    RevisionIsActive { location_id: i64 },

    /// Writer job is not in a terminal state
    ///
    /// This occurs when the revision's writer job is still running.
    /// Stop the job before deleting the revision.
    #[error(
        "revision with location ID '{location_id}' has an active writer job; stop the job before deleting"
    )]
    WriterJobNotTerminal { location_id: i64 },

    /// Error deleting revision via admin API
    ///
    /// This occurs when the delete request fails due to:
    /// - Invalid location ID format
    /// - Network or connection errors
    /// - Database errors
    #[error("failed to delete table revision")]
    DeleteError(#[source] amp_client_admin::revisions::DeleteError),

    /// Failed to serialize result to JSON
    #[error("failed to serialize result to JSON")]
    JsonSerialization(#[source] serde_json::Error),

    /// Failed to show confirmation prompt
    #[error("failed to show confirmation prompt")]
    ConfirmationPrompt(#[source] std::io::Error),
}
