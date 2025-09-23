//! Snapshot context fixture for test data comparison.
//!
//! This fixture provides a [`SnapshotContext`] for creating dataset snapshots in tests.
//! It wraps a QueryContext to enable creation of snapshots from dataset snapshot reference data
//! or fresh data extractions.
//!
//! The fixture focuses on snapshot construction only. For comparing snapshots,
//! use the assertion functions in [`testlib::helpers`](crate::testlib::helpers) such as
//! [`assert_snapshots_eq`](crate::testlib::helpers::assert_snapshots_eq) and
//! [`assert_snapshot_block_ranges_eq`](crate::testlib::helpers::assert_snapshot_block_ranges

use std::sync::Arc;

use common::{
    BoxError, LogicalCatalog, QueryContext,
    catalog::physical::{Catalog, PhysicalTable},
    config::Config,
};

/// Snapshot context fixture for comparing dataset snapshots in tests.
///
/// This fixture wraps a QueryContext to provide snapshot comparison capabilities
/// for testing blockchain data extraction and processing. It enables comparisons
/// between dataset snapshot reference data and fresh extractions to ensure data integrity.
pub struct SnapshotContext {
    query_ctx: QueryContext,
}

impl SnapshotContext {
    /// Create a snapshot from dataset tables.
    ///
    /// This method creates a snapshot from physical tables, which can be obtained
    /// from dataset snapshot reference data or fresh ETL extraction pipeline dumps.
    pub async fn from_tables(
        config: &Arc<Config>,
        tables: Vec<Arc<PhysicalTable>>,
    ) -> Result<Self, BoxError> {
        let logical = LogicalCatalog::from_tables(tables.iter().map(|t| t.table()));
        let catalog = Catalog::new(tables, logical);
        let query_ctx = QueryContext::for_catalog(catalog, config.make_query_env()?, false).await?;

        Ok(Self { query_ctx })
    }

    /// Get a reference to the underlying QueryContext.
    ///
    /// This provides access to the wrapped QueryContext for advanced operations
    /// that require direct query execution capabilities.
    pub fn query_context(&self) -> &QueryContext {
        &self.query_ctx
    }

    /// Get a reference to the physical tables in this snapshot.
    ///
    /// This convenience method provides direct access to the physical tables
    /// without needing to go through the query context and catalog.
    pub fn physical_tables(&self) -> impl Iterator<Item = &Arc<PhysicalTable>> {
        self.query_ctx.catalog().physical_tables()
    }
}
