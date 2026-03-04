use std::{collections::BTreeMap, sync::Arc};

use datafusion::logical_expr::ScalarUDF;
use datasets_common::hash_reference::HashReference;

use crate::{
    BlockNum,
    catalog::logical::LogicalTable,
    physical_table::{MultiNetworkSegmentsError, SnapshotError, table::PhysicalTable},
};

#[derive(Debug, Clone, Default)]
pub struct Catalog {
    /// Logical tables describing dataset schemas and metadata.
    tables: Vec<LogicalTable>,
    /// UDFs specific to the datasets corresponding to the resolved tables.
    udfs: Vec<ScalarUDF>,
    /// The physical catalog entries, each pairing a physical table with SQL naming.
    entries: Vec<(Arc<PhysicalTable>, Arc<str>)>,
    /// Dependency alias to hash reference mappings for lazy resolution.
    dep_aliases: BTreeMap<String, HashReference>,
}

impl Catalog {
    /// Creates a new catalog from the given entries, logical tables, and UDFs.
    pub fn new(
        tables: Vec<LogicalTable>,
        udfs: Vec<ScalarUDF>,
        entries: Vec<(Arc<PhysicalTable>, Arc<str>)>,
        dep_aliases: BTreeMap<String, HashReference>,
    ) -> Self {
        Catalog {
            tables,
            udfs,
            entries,
            dep_aliases,
        }
    }

    /// Returns the dependency alias to hash reference mappings.
    pub fn dep_aliases(&self) -> &BTreeMap<String, HashReference> {
        &self.dep_aliases
    }

    /// Returns the catalog entries.
    pub fn entries(&self) -> &[(Arc<PhysicalTable>, Arc<str>)] {
        &self.entries
    }

    /// Convenience iterator returning physical tables for consumers that only need
    /// physical storage access (e.g. compaction, garbage collection).
    pub fn physical_tables(&self) -> impl Iterator<Item = &Arc<PhysicalTable>> {
        self.entries
            .iter()
            .map(|(physical_table, _)| physical_table)
    }

    /// Returns the logical tables.
    pub fn tables(&self) -> &[LogicalTable] {
        &self.tables
    }

    /// Returns the user-defined functions.
    pub fn udfs(&self) -> &[ScalarUDF] {
        &self.udfs
    }

    /// Consumes the catalog, returning its entries, logical tables, and UDFs.
    #[expect(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        Vec<(Arc<PhysicalTable>, Arc<str>)>,
        Vec<LogicalTable>,
        Vec<ScalarUDF>,
    ) {
        (self.entries, self.tables, self.udfs)
    }

    /// Returns the earliest synced block number across all tables in this catalog.
    ///
    /// Snapshots each table and inspects its `synced_range()`. Returns `None`
    /// when no table has synced data.
    pub async fn earliest_block(&self) -> Result<Option<BlockNum>, EarliestBlockError> {
        let mut earliest = None;
        for (physical_table, _) in &self.entries {
            let snapshot = physical_table
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

impl crate::retryable::RetryableErrorExt for EarliestBlockError {
    fn is_retryable(&self) -> bool {
        match self {
            Self::Snapshot(err) => err.is_retryable(),
            Self::MultiNetworkSegments(_) => false,
        }
    }
}
