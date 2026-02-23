//! Table revisions listing command.
//!
//! Retrieves and displays all table revisions through the admin API by:
//! 1. Creating a client for the admin API
//! 2. Using the client's revisions list method with optional active and limit filters
//! 3. Displaying the revisions based on the output format
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Active: `--active` flag to filter by active status
//! - Limit: `--limit` flag to limit the number of results
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use monitoring::logging;

use crate::{args::GlobalArgs, client};

/// Command-line arguments for the `table list` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// Filter by active status (true or false)
    #[arg(long, short = 'a')]
    pub active: Option<bool>,

    /// Maximum number of revisions to return (default: 100)
    #[arg(long, short = 'l')]
    pub limit: Option<i64>,
}

/// List all table revisions by retrieving them from the admin API.
///
/// Retrieves table revisions with optional filtering and displays them based on
/// the output format.
///
/// # Errors
///
/// Returns [`Error`] for API errors (400/500) or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, active = ?active, limit = ?limit))]
pub async fn run(
    Args {
        global,
        active,
        limit,
    }: Args,
) -> Result<(), Error> {
    tracing::debug!("listing table revisions");

    let revisions = get_revisions(&global, active, limit).await?;
    let result = ListResult { revisions };
    global.print(&result).map_err(Error::JsonFormattingError)?;

    Ok(())
}

/// Retrieve table revisions from the admin API.
///
/// Creates a client and uses the revisions list method.
#[tracing::instrument(skip_all)]
async fn get_revisions(
    global: &GlobalArgs,
    active: Option<bool>,
    limit: Option<i64>,
) -> Result<Vec<client::revisions::RevisionInfo>, Error> {
    let client = global.build_client().map_err(Error::ClientBuildError)?;

    let revisions = client
        .revisions()
        .list(active, limit)
        .await
        .map_err(|err| {
            tracing::error!(
                error = %err,
                error_source = logging::error_source(&err),
                "failed to list table revisions",
            );
            Error::ClientError(err)
        })?;

    Ok(revisions)
}

/// Result of a table list operation.
#[derive(serde::Serialize)]
struct ListResult {
    revisions: Vec<client::revisions::RevisionInfo>,
}

impl std::fmt::Display for ListResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.revisions.is_empty() {
            writeln!(f, "No revisions found")
        } else {
            writeln!(f, "Revisions ({}):", self.revisions.len())?;
            for rev in &self.revisions {
                let meta = &rev.metadata;
                let active_str = if rev.active {
                    console::style("active").green().to_string()
                } else {
                    console::style("inactive").red().to_string()
                };
                writeln!(
                    f,
                    "  {} - {}/{} {} [{}]",
                    rev.id, meta.dataset_namespace, meta.dataset_name, meta.table_name, active_str,
                )?;
            }
            Ok(())
        }
    }
}

/// Errors for table revision listing operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuildError(#[source] crate::args::BuildClientError),

    /// Client error from the API
    #[error("client error")]
    ClientError(#[source] client::revisions::ListError),

    /// Failed to format JSON for display
    #[error("failed to format revisions JSON")]
    JsonFormattingError(#[source] serde_json::Error),
}
