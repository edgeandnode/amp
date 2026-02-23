//! Table revision truncate command.
//!
//! Truncates a table revision by its location ID:
//! 1. Creating a client for the admin API
//! 2. Prompting for user confirmation (unless `--force` is set)
//! 3. Using the client's revision truncate method
//! 4. Displaying the result
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use monitoring::logging;

use crate::args::GlobalArgs;

/// Command-line arguments for the `table truncate` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// The location ID of the revision to truncate
    pub location_id: i64,

    /// Skip confirmation prompt
    #[arg(long, short = 'f')]
    pub force: bool,

    /// Max concurrent file deletions
    #[arg(long, default_value = "10")]
    pub concurrency: usize,
}

/// Truncate a table revision by location ID.
///
/// Deletes all files from object storage and their corresponding metadata,
/// then deletes the revision itself. The revision must be inactive before
/// it can be truncated.
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
        concurrency,
    }: Args,
) -> Result<(), Error> {
    let client = global.build_client().map_err(Error::ClientBuildError)?;

    if !force {
        let confirmed = crate::ui::prompt_for_confirmation(&format!(
            "{} This will permanently delete all files from object storage and metadata for revision {location_id}. Continue?",
            console::style("⚠").yellow().bold(),
        ))
        .map_err(Error::ConfirmationPrompt)?;

        if !confirmed {
            crate::info!("Truncation cancelled");
            return Ok(());
        }
    }

    tracing::debug!(%location_id, "truncating table revision via admin API");

    let response = client
        .revisions()
        .truncate(location_id, Some(concurrency))
        .await
        .map_err(|err| {
            tracing::error!(error = %err, error_source = logging::error_source(&err), "failed to truncate table revision");
            match err {
                crate::client::revisions::TruncateError::RevisionNotFound(_) => {
                    Error::NotFound { location_id }
                }
                crate::client::revisions::TruncateError::RevisionIsActive(_) => {
                    Error::RevisionIsActive { location_id }
                }
                crate::client::revisions::TruncateError::WriterJobNotTerminal(_) => {
                    Error::WriterJobNotTerminal { location_id }
                }
                other => Error::TruncateError(other),
            }
        })?;

    let result = TruncateOutput {
        location_id,
        files_deleted: response.files_deleted,
    };
    global.print(&result).map_err(Error::JsonSerialization)?;

    Ok(())
}

/// Result of a revision truncate operation.
#[derive(serde::Serialize)]
struct TruncateOutput {
    location_id: i64,
    files_deleted: u64,
}

impl std::fmt::Display for TruncateOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{} Revision {} truncated ({} files deleted)",
            console::style("✓").green().bold(),
            self.location_id,
            self.files_deleted,
        )
    }
}

/// Errors for revision truncate operations.
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
    /// This occurs when the revision is currently active and cannot be truncated.
    /// Deactivate the revision before truncating it.
    #[error("revision with location ID '{location_id}' is active and cannot be truncated")]
    RevisionIsActive { location_id: i64 },

    /// Writer job is not in a terminal state
    ///
    /// This occurs when the revision's writer job is still running.
    /// Stop the job before truncating the revision.
    #[error(
        "revision with location ID '{location_id}' has an active writer job; stop the job before truncating"
    )]
    WriterJobNotTerminal { location_id: i64 },

    /// Error truncating revision via admin API
    ///
    /// This occurs when the truncate request fails due to:
    /// - Invalid location ID format
    /// - Network or connection errors
    /// - Database or object store errors
    #[error("failed to truncate table revision")]
    TruncateError(#[source] crate::client::revisions::TruncateError),

    /// Failed to serialize result to JSON
    #[error("failed to serialize result to JSON")]
    JsonSerialization(#[source] serde_json::Error),

    /// Failed to show confirmation prompt
    #[error("failed to show confirmation prompt")]
    ConfirmationPrompt(#[source] std::io::Error),
}
