//! Dataset restore command.
//!
//! Restores dataset physical table locations from object storage into the metadata database by:
//! 1. Parsing dataset reference (namespace/name@version)
//! 2. POSTing to admin API `/datasets/{namespace}/{name}/versions/{version}/restore` endpoint
//! 3. Returning information about the restored tables
//!
//! Optionally targets a single table with `--table` and can activate a specific revision
//! with `--location-id`.
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

    /// Restore a specific table only (instead of all tables)
    #[arg(long, value_name = "TABLE")]
    pub table: Option<String>,

    /// Activate a specific location ID for the table (requires --table)
    #[arg(long, value_name = "ID", requires = "table")]
    pub location_id: Option<i64>,
}

/// Restore dataset physical tables from object storage.
///
/// Re-indexes physical table metadata from storage into the database.
/// Optionally targets a single table with `--table`, and can activate
/// a specific revision with `--location-id`.
///
/// # Errors
///
/// Returns [`Error`] for invalid paths/URLs, API errors (400/404/500), or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, %dataset_ref))]
pub async fn run(
    Args {
        global,
        dataset_ref,
        table,
        location_id,
    }: Args,
) -> Result<(), Error> {
    let client = global.build_client().map_err(Error::ClientBuild)?;

    if let Some(table_name) = table {
        tracing::debug!(%dataset_ref, %table_name, ?location_id, "restoring single table");

        client
            .datasets()
            .restore_table(&dataset_ref, &table_name, location_id)
            .await
            .map_err(Error::RestoreTable)?;

        let result = RestoreTableResult {
            table: table_name,
            location_id,
        };
        global.print(&result).map_err(Error::JsonSerialization)?;
    } else {
        tracing::debug!(%dataset_ref, "restoring dataset");

        let response = client
            .datasets()
            .restore(&dataset_ref)
            .await
            .map_err(Error::Restore)?;

        let result = RestoreResult {
            tables: response.tables,
        };
        global.print(&result).map_err(Error::JsonSerialization)?;
    }

    Ok(())
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

/// Result of a single table restore operation.
#[derive(serde::Serialize)]
struct RestoreTableResult {
    table: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    location_id: Option<i64>,
}

impl std::fmt::Display for RestoreTableResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(
            f,
            "{} Table '{}' restored successfully",
            console::style("✓").green().bold(),
            console::style(&self.table).yellow(),
        )?;
        if let Some(id) = self.location_id {
            writeln!(
                f,
                "{} Activated location_id: {}",
                console::style("→").cyan(),
                id
            )?;
        }
        Ok(())
    }
}

/// Errors for dataset restore operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuild(#[source] crate::args::BuildClientError),

    /// Restore error from the client (all tables)
    #[error("restore failed")]
    Restore(#[source] crate::client::datasets::RestoreError),

    /// Restore table error from the client (single table)
    #[error("restore table failed")]
    RestoreTable(#[source] crate::client::datasets::RestoreTableError),

    /// Failed to serialize result to JSON
    #[error("failed to serialize result to JSON")]
    JsonSerialization(#[source] serde_json::Error),
}
