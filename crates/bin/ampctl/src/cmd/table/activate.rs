//! Table revision activate command.
//!
//! Activates a table revision through the admin API by:
//! 1. Creating a client for the admin API
//! 2. Using the client's revision activate method
//! 3. Displaying success message
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use monitoring::logging;

use crate::args::GlobalArgs;

/// Command-line arguments for the `table activate` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// The dataset reference (e.g. namespace/name@version)
    pub dataset: String,

    /// The table name
    pub table_name: String,

    /// The location ID of the revision to activate
    pub location_id: u64,
}

/// Activate a table revision via the admin API.
///
/// # Errors
///
/// Returns [`Error`] for API errors (400/404/500) or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, dataset = %dataset, table_name = %table_name, location_id = %location_id))]
pub async fn run(
    Args {
        global,
        dataset,
        table_name,
        location_id,
    }: Args,
) -> Result<(), Error> {
    let client = global.build_client().map_err(Error::ClientBuildError)?;

    tracing::debug!(%dataset, %table_name, %location_id, "Activating table revision via admin API");

    client
        .revisions()
        .activate(location_id, &dataset, &table_name)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to activate table revision");
            match err {
                crate::client::revisions::ActivateError::DatasetNotFound(_) => {
                    Error::DatasetNotFound {
                        dataset: dataset.clone(),
                    }
                }
                crate::client::revisions::ActivateError::TableNotFound(_) => {
                    Error::TableNotFound {
                        table_name: table_name.clone(),
                        dataset: dataset.clone(),
                    }
                }
                _ => Error::ActivateError(err),
            }
        })?;

    let result = ActivateOutput {
        dataset,
        table_name,
        location_id,
    };
    global.print(&result).map_err(Error::JsonSerialization)?;

    Ok(())
}

/// Result of a revision activate operation.
#[derive(serde::Serialize)]
struct ActivateOutput {
    dataset: String,
    table_name: String,
    location_id: u64,
}

impl std::fmt::Display for ActivateOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(
            f,
            "{} Revision {} activated for table {} in dataset {}",
            console::style("✓").green().bold(),
            self.location_id,
            self.table_name,
            self.dataset,
        )
    }
}

/// Errors for revision activate operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuildError(#[source] crate::args::BuildClientError),

    /// Dataset not found
    ///
    /// This occurs when the dataset reference does not resolve
    /// to an existing dataset or revision in the metadata database.
    #[error("dataset not found: {dataset}")]
    DatasetNotFound { dataset: String },

    /// Table not found
    ///
    /// This occurs when the table name does not exist
    /// for the given dataset.
    #[error("table '{table_name}' not found for dataset '{dataset}'")]
    TableNotFound { table_name: String, dataset: String },

    /// Error activating revision via admin API
    ///
    /// This occurs when the activate request fails due to:
    /// - Invalid location ID format
    /// - Network or connection errors
    /// - Metadata database errors
    /// - Failed to resolve revision to manifest hash
    #[error("failed to activate table revision")]
    ActivateError(#[source] crate::client::revisions::ActivateError),

    /// Failed to serialize result to JSON
    #[error("failed to serialize result to JSON")]
    JsonSerialization(#[source] serde_json::Error),
}
