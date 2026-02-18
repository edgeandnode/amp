//! Table revision register command.
//!
//! Registers a table revision through the admin API by:
//! 1. Creating a client for the admin API
//! 2. Using the client's revision register method
//! 3. Displaying the assigned location ID
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use monitoring::logging;

use crate::args::GlobalArgs;

/// Command-line arguments for the `table register` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// The dataset reference (e.g. namespace/name@version)
    pub dataset: String,

    /// The table name
    pub table_name: String,

    /// The relative path to the storage location for this revision
    pub path: String,
}

/// Register a table revision via the admin API.
///
/// # Errors
///
/// Returns [`Error`] for API errors (404/500) or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, dataset = %dataset, table_name = %table_name, path = %path))]
pub async fn run(
    Args {
        global,
        dataset,
        table_name,
        path,
    }: Args,
) -> Result<(), Error> {
    let client = global.build_client().map_err(Error::ClientBuildError)?;

    tracing::debug!(%dataset, %table_name, %path, "Registering table revision via admin API");

    let response = client
        .revisions()
        .register(&dataset, &table_name, &path)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to register table revision");
            match err {
                crate::client::revisions::RegisterError::DatasetNotFound(_) => {
                    Error::DatasetNotFound {
                        dataset: dataset.clone(),
                    }
                }
                crate::client::revisions::RegisterError::ResolveRevision(_) => {
                    Error::ResolveRevision(err)
                }
                _ => Error::RegisterError(err),
            }
        })?;

    let result = RegisterOutput {
        location_id: response.location_id,
        dataset,
        table_name,
        path,
    };
    global.print(&result).map_err(Error::JsonSerialization)?;

    Ok(())
}

/// Result of a revision register operation.
#[derive(serde::Serialize)]
struct RegisterOutput {
    location_id: i64,
    dataset: String,
    table_name: String,
    path: String,
}

impl std::fmt::Display for RegisterOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(
            f,
            "{} Revision registered for table {} in dataset {} (location_id: {})",
            console::style("✓").green().bold(),
            self.table_name,
            self.dataset,
            self.location_id,
        )
    }
}

/// Errors for revision register operations.
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

    /// Failed to resolve revision
    ///
    /// This occurs when the dataset reference cannot be resolved
    /// to a manifest hash.
    #[error("failed to resolve revision")]
    ResolveRevision(#[source] crate::client::revisions::RegisterError),

    /// Error registering revision via admin API
    ///
    /// This occurs when the register request fails due to:
    /// - Network or connection errors
    /// - Metadata database errors
    /// - Failed to register the table revision record
    #[error("failed to register table revision")]
    RegisterError(#[source] crate::client::revisions::RegisterError),

    /// Failed to serialize result to JSON
    #[error("failed to serialize result to JSON")]
    JsonSerialization(#[source] serde_json::Error),
}
