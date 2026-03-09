//! Table revision prune command.
//!
//! Prunes non-canonical segments from a table revision by its location ID:
//! 1. Creating a client for the admin API
//! 2. Prompting for user confirmation (unless `--force` is set)
//! 3. Using the client's revision prune method
//! 4. Displaying the result
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use std::time::Duration;

use monitoring::logging;

use crate::args::GlobalArgs;

/// Command-line arguments for the `table prune` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// The location ID of the revision to prune
    pub location_id: i64,

    /// Only prune segments ending before this block number
    #[arg(long)]
    pub before_block: Option<u64>,

    /// Delay in seconds before files are eligible for GC deletion (default: 3600)
    #[arg(long, default_value = "3600", value_parser = parse_duration)]
    pub gc_delay: Duration,

    /// Skip confirmation prompt
    #[arg(long, short = 'f')]
    pub force: bool,
}

/// Prune non-canonical segments from a table revision by location ID.
///
/// Identifies and deletes segments that are not part of the canonical chain
/// (due to reorgs, failed compaction, or other reasons) while preserving the
/// revision record and all canonical segments.
///
/// # Errors
///
/// Returns [`Error`] for API errors (400/404/409/500) or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, location_id = %location_id))]
pub async fn run(
    Args {
        global,
        location_id,
        before_block,
        gc_delay,
        force,
    }: Args,
) -> Result<(), Error> {
    let client = global.build_client().map_err(Error::ClientBuildError)?;

    if !force {
        let block_info = if let Some(block) = before_block {
            format!(" ending before block {block}")
        } else {
            String::new()
        };
        let delay_secs = gc_delay.as_secs();
        let gc_info = format!(" (files will be eligible for deletion after {delay_secs}s)");
        let confirmed = crate::ui::prompt_for_confirmation(&format!(
            "{} This will schedule non-canonical segment files{} for garbage collection{}. Continue?",
            console::style("⚠").yellow().bold(),
            block_info,
            gc_info,
        ))
        .map_err(Error::ConfirmationPrompt)?;

        if !confirmed {
            crate::info!("Prune cancelled");
            return Ok(());
        }
    }

    tracing::debug!(%location_id, ?before_block, ?gc_delay, "pruning non-canonical segments via admin API");

    let response = client
        .revisions()
        .prune(location_id, before_block, gc_delay.as_secs())
        .await
        .map_err(|err| {
            tracing::error!(
                location_id = %location_id,
                error = %err,
                error_source = logging::error_source(&err),
                "failed to prune non-canonical segments"
            );
            match err {
                amp_client_admin::revisions::PruneError::RevisionNotFound(_) => {
                    Error::NotFound { location_id }
                }
                amp_client_admin::revisions::PruneError::WriterJobNotTerminal(_) => {
                    Error::WriterJobNotTerminal { location_id }
                }
                other => Error::PruneError(other),
            }
        })?;

    let result = PruneOutput {
        location_id,
        files_scheduled: response.files_scheduled,
        gc_delay_secs: response.gc_delay_secs,
        before_block,
    };
    global.print(&result).map_err(Error::JsonSerialization)?;

    Ok(())
}

/// Parse seconds into Duration
fn parse_duration(s: &str) -> Result<Duration, String> {
    s.parse::<u64>()
        .map(Duration::from_secs)
        .map_err(|e| e.to_string())
}

/// Result of a revision prune operation.
#[derive(serde::Serialize)]
struct PruneOutput {
    location_id: i64,
    files_scheduled: u64,
    gc_delay_secs: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    before_block: Option<u64>,
}

impl std::fmt::Display for PruneOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let block_info = if let Some(block) = self.before_block {
            format!(" ending before block {block}")
        } else {
            String::new()
        };
        write!(
            f,
            "{} Revision {} pruned ({} non-canonical segment files{} scheduled for GC, deletion in {}s)",
            console::style("✓").green().bold(),
            self.location_id,
            self.files_scheduled,
            block_info,
            self.gc_delay_secs,
        )
    }
}

/// Errors for revision prune operations.
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

    /// Writer job is not in a terminal state
    ///
    /// This occurs when the revision's writer job is still running.
    /// Stop the job before pruning the revision.
    #[error(
        "revision with location ID '{location_id}' has an active writer job; stop the job before pruning"
    )]
    WriterJobNotTerminal { location_id: i64 },

    /// Error pruning non-canonical segments via admin API
    ///
    /// This occurs when the prune request fails due to:
    /// - Invalid location ID format
    /// - Network or connection errors
    /// - Database or object store errors
    #[error("failed to prune non-canonical segments")]
    PruneError(#[source] amp_client_admin::revisions::PruneError),

    /// Failed to serialize result to JSON
    #[error("failed to serialize result to JSON")]
    JsonSerialization(#[source] serde_json::Error),

    /// Failed to show confirmation prompt
    #[error("failed to show confirmation prompt")]
    ConfirmationPrompt(#[source] std::io::Error),
}
