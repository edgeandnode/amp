//! Dataset restore command.
//!
//! Restores dataset physical table locations from object storage into the metadata database by:
//! 1. Parsing dataset reference (namespace/name@version)
//! 2. POSTing to admin API `/datasets/{namespace}/{name}/versions/{version}/restore` endpoint
//! 3. Returning information about the restored tables
//!
//! # Dataset Reference Format
//!
//! `namespace/name@version` (e.g., `graph/eth_mainnet@1.0.0`)
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use datasets_common::reference::Reference;

use crate::{args::GlobalArgs, client::datasets::RestoredTableInfo};

/// Command-line arguments for the `restore-dataset` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// The dataset reference in format: namespace/name@version
    ///
    /// Examples: my_namespace/my_dataset@1.0.0, my_namespace/my_dataset@latest
    #[arg(value_name = "REFERENCE", required = true, value_parser = clap::value_parser!(Reference))]
    pub dataset_ref: Reference,
}

/// Result of a dataset restore operation.
#[derive(serde::Serialize)]
struct RestoreResult {
    tables: Vec<RestoredTableInfo>,
}

impl std::fmt::Display for RestoreResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(
            f,
            "{} Dataset restored successfully",
            console::style("✓").green().bold()
        )?;
        writeln!(
            f,
            "{} Restored {} table(s):",
            console::style("→").cyan(),
            self.tables.len()
        )?;

        for table in &self.tables {
            writeln!(
                f,
                "  {} {} (location_id: {}, url: {})",
                console::style("•").dim(),
                console::style(&table.table_name).yellow(),
                table.location_id,
                table.url
            )?;
        }

        Ok(())
    }
}

/// Restore dataset physical tables from object storage.
///
/// Re-indexes physical table metadata from storage into the database.
///
/// # Errors
///
/// Returns [`Error`] for invalid paths/URLs, API errors (400/404/500), or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, %dataset_ref))]
pub async fn run(
    Args {
        global,
        dataset_ref,
    }: Args,
) -> Result<(), Error> {
    tracing::debug!(%dataset_ref, "Restoring dataset");

    let tables = restore_dataset(&global, &dataset_ref).await?;
    let result = RestoreResult { tables };
    global.print(&result).map_err(Error::JsonSerialization)?;

    Ok(())
}

/// Restore dataset tables via the admin API.
///
/// POSTs to the `/datasets/{namespace}/{name}/versions/{version}/restore` endpoint
/// and returns the list of restored tables.
#[tracing::instrument(skip_all, fields(%dataset_ref))]
async fn restore_dataset(
    global: &GlobalArgs,
    dataset_ref: &Reference,
) -> Result<Vec<RestoredTableInfo>, Error> {
    let client = global.build_client().map_err(Error::ClientBuild)?;
    let response = client
        .datasets()
        .restore(dataset_ref)
        .await
        .map_err(Error::Restore)?;

    Ok(response.tables)
}

/// Errors for dataset restore operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuild(#[source] crate::args::BuildClientError),

    /// Restore error from the client
    #[error("restore failed")]
    Restore(#[source] crate::client::datasets::RestoreError),

    /// Failed to serialize result to JSON
    #[error("failed to serialize result to JSON")]
    JsonSerialization(#[source] serde_json::Error),
}
