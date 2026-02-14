//! Table revision get command.
//!
//! Retrieves a table revision by location ID through the admin API by:
//! 1. Creating a client for the admin API
//! 2. Using the client's revision get_by_id method
//! 3. Displaying revision details
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use monitoring::logging;

use crate::args::GlobalArgs;

/// Command-line arguments for the `table get` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// The location ID of the revision to retrieve
    pub location_id: i64,
}

/// Retrieve a table revision by location ID via the admin API.
///
/// Retrieves table revision information and displays it based on the output format.
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

    tracing::debug!(%location_id, "Getting table revision via admin API");

    let revision = client
        .revisions()
        .get_by_id(location_id)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to get table revision");
            Error::GetError(err)
        })?
        .ok_or(Error::NotFound { location_id })?;

    let result = GetOutput { revision };
    global.print(&result).map_err(Error::JsonSerialization)?;

    Ok(())
}

/// Result of a revision get operation.
#[derive(serde::Serialize)]
struct GetOutput {
    #[serde(flatten)]
    revision: crate::client::revisions::RevisionInfo,
}

impl std::fmt::Display for GetOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let rev = &self.revision;
        let meta = &rev.metadata;

        writeln!(
            f,
            "{} Revision {}",
            console::style("→").cyan().bold(),
            rev.id,
        )?;
        writeln!(f, "  Path:      {}", rev.path)?;
        writeln!(
            f,
            "  Active:    {}",
            if rev.active {
                console::style("true").green().to_string()
            } else {
                console::style("false").red().to_string()
            }
        )?;
        if let Some(writer) = rev.writer {
            writeln!(f, "  Writer:    {writer}")?;
        }
        writeln!(
            f,
            "  Dataset:   {}/{}",
            meta.dataset_namespace, meta.dataset_name
        )?;
        writeln!(f, "  Manifest:  {}", meta.manifest_hash)?;
        write!(f, "  Table:     {}", meta.table_name)?;

        Ok(())
    }
}

/// Errors for revision get operations.
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

    /// Error getting revision via admin API
    ///
    /// This occurs when the get request fails due to:
    /// - Invalid location ID format
    /// - Network or connection errors
    /// - Metadata database errors
    #[error("failed to get table revision")]
    GetError(#[source] crate::client::revisions::GetByIdError),

    /// Failed to serialize result to JSON
    #[error("failed to serialize result to JSON")]
    JsonSerialization(#[source] serde_json::Error),
}
