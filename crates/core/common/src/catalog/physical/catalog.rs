use std::sync::Arc;

use crate::{
    BlockNum,
    catalog::logical::LogicalCatalog,
    physical_table::{MultiNetworkSegmentsError, SnapshotError, table::PhysicalTable},
    sql::TableReference,
};

#[derive(Debug, Clone)]
pub struct Catalog {
    /// The logical catalog describing dataset schemas and metadata.
    logical: LogicalCatalog,
    /// The physical catalog entries, each pairing a physical table with SQL naming.
    entries: Vec<CatalogTable>,
}

impl Catalog {
    /// Creates a new catalog from the given entries and logical catalog.
    pub fn new(logical: LogicalCatalog, entries: Vec<CatalogTable>) -> Self {
        Catalog { logical, entries }
    }

    /// Returns the catalog entries.
    pub fn entries(&self) -> &[CatalogTable] {
        &self.entries
    }

    /// Convenience iterator returning physical tables for consumers that only need
    /// physical storage access (e.g. compaction, garbage collection).
    pub fn physical_tables(&self) -> impl Iterator<Item = &Arc<PhysicalTable>> {
        self.entries.iter().map(|entry| entry.physical_table())
    }

    /// Returns a reference to the logical catalog.
    pub fn logical(&self) -> &LogicalCatalog {
        &self.logical
    }

    /// Consumes the catalog, returning its entries and logical catalog.
    pub fn into_parts(self) -> (Vec<CatalogTable>, LogicalCatalog) {
        (self.entries, self.logical)
    }

    /// Returns the earliest synced block number across all tables in this catalog.
    ///
    /// Snapshots each table and inspects its `synced_range()`. Returns `None`
    /// when no table has synced data.
    pub async fn earliest_block(&self) -> Result<Option<BlockNum>, EarliestBlockError> {
        let mut earliest = None;
        for entry in &self.entries {
            let snapshot = entry
                .physical_table()
                .snapshot(false)
                .await
                .map_err(EarliestBlockError::Snapshot)?;
            let synced_range = snapshot
                .synced_range()
                .map_err(EarliestBlockError::MultiNetworkSegments)?;
            match (earliest, &synced_range) {
                (None, Some(range)) => earliest = Some(range.start()),
                _ => earliest = earliest.min(synced_range.map(|range| range.start())),
            }
        }
        Ok(earliest)
    }
}

/// Failed to compute the earliest block across catalog tables
///
/// This error covers failures during `Catalog::earliest_block()`.
#[derive(Debug, thiserror::Error)]
pub enum EarliestBlockError {
    /// Failed to create a table snapshot for earliest-block computation
    ///
    /// This occurs when fetching segment metadata or computing the canonical
    /// chain for a physical table fails during the snapshot step.
    #[error("failed to snapshot table for earliest block computation")]
    Snapshot(#[source] SnapshotError),

    /// A table contains segments spanning multiple networks
    ///
    /// `synced_range()` can only express a single-network block range.
    /// This error is returned when a table violates that invariant.
    #[error("table has multi-network segments")]
    MultiNetworkSegments(#[source] MultiNetworkSegmentsError),
}

/// A catalog entry that pairs a physical table with SQL naming information.
///
/// `PhysicalTable` represents pure physical storage (revision, segments, canonical chains,
/// file access). `CatalogTable` adds the SQL catalog identity — the schema string under
/// which the table is registered for SQL queries.
///
/// This separation allows physical-only consumers (compaction, garbage collection, parquet
/// writing) to work with `PhysicalTable` without carrying SQL naming concerns.
#[derive(Debug, Clone)]
pub struct CatalogTable {
    /// The underlying physical table providing storage access (segments, snapshots, file I/O).
    physical_table: Arc<PhysicalTable>,

    /// The dataset reference portion of SQL table references.
    ///
    /// SQL table references have the format `<dataset_ref>.<table>` (e.g., `anvil_rpc.blocks`).
    /// This field stores the string form of the `<dataset_ref>` portion.
    sql_schema_name: String,
}

impl CatalogTable {
    /// Creates a new catalog table entry pairing a physical table with its SQL schema name.
    pub fn new(physical_table: Arc<PhysicalTable>, sql_schema_name: String) -> Self {
        Self {
            physical_table,
            sql_schema_name,
        }
    }

    /// Returns a reference to the underlying physical table.
    pub fn physical_table(&self) -> &Arc<PhysicalTable> {
        &self.physical_table
    }

    /// Returns the dataset reference portion for SQL table references.
    pub fn sql_schema_name(&self) -> &str {
        &self.sql_schema_name
    }

    /// Qualified table reference in the format `dataset_name.table_name`.
    pub fn table_ref(&self) -> TableReference {
        TableReference::partial(
            self.sql_schema_name.clone(),
            self.physical_table.table_name().clone(),
        )
    }
}
