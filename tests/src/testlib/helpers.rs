//! Helper functions for testlib test environments.
//!
//! This module provides convenience functions for common test operations like
//! dataset extraction, data validation, and test setup tasks that are frequently
//! needed across multiple test scenarios.

use std::{collections::BTreeMap, sync::Arc};

use ampd::dump_cmd::dump;
use common::{
    BoxError, LogicalCatalog,
    arrow::array::RecordBatch,
    catalog::{
        JobLabels,
        physical::{Catalog, PhysicalTable},
    },
    config::Config,
    metadata::segments::BlockRange,
    query_context::parse_sql,
};
use dataset_store::DatasetStore;
use datasets_common::{
    name::Name, partial_reference::PartialReference, reference::Reference, table_name::TableName,
};
use dump::{EndBlock, consistency_check};
use metadata_db::MetadataDb;

use super::fixtures::SnapshotContext;

/// Extract blockchain data from a dataset source and save as Parquet files.
///
/// This function runs the full ETL extraction pipeline for a specified dataset,
/// extracting data from the configured source (RPC, Firehose, etc.) and saving
/// it as Parquet files in the test environment's data directory.
///
/// Returns the physical tables that were created during the dump process.
pub async fn dump_dataset(
    config: &Arc<Config>,
    metadata_db: &MetadataDb,
    dataset: Reference,
    end: u64,
    max_writers: u16,
    microbatch_max_interval: impl Into<Option<u64>>,
) -> Result<Vec<Arc<PhysicalTable>>, BoxError> {
    dump(
        config.clone(),
        metadata_db.clone(),
        dataset,
        true,
        EndBlock::Absolute(end),
        max_writers,
        None,
        microbatch_max_interval.into(),
        None,
        false,
        None,
    )
    .await
}

/// Run consistency check on a physical table.
///
/// This function validates the integrity of a physical table by checking that
/// all registered files in the metadata database actually exist in the object
/// store, and that no extra files are present. This helps detect data corruption
/// or synchronization issues between metadata and storage.
pub async fn check_table_consistency(table: &Arc<PhysicalTable>) -> Result<(), BoxError> {
    consistency_check(table).await.map_err(Into::into)
}

/// Restore dataset snapshot from previously saved snapshot files.
///
/// This function loads a dataset definition and restores the physical tables
/// from their snapshot state. Dataset snapshots are reference data
/// that have been validated and saved as the expected baseline for tests.
/// This is typically used to set up known-good data for comparison testing.
pub async fn restore_dataset_snapshot(
    config: &Arc<Config>,
    metadata_db: &MetadataDb,
    dataset_store: &Arc<DatasetStore>,
    dataset_ref: &Reference,
) -> Result<Vec<Arc<PhysicalTable>>, BoxError> {
    let dataset = dataset_store.get_dataset(dataset_ref).await?;

    let job_labels = JobLabels {
        dataset_namespace: dataset_ref.namespace().clone(),
        dataset_name: dataset_ref.name().clone(),
        manifest_hash: dataset.manifest_hash().clone(),
    };
    let mut tables = Vec::<Arc<PhysicalTable>>::new();

    for table in Arc::new(dataset).resolved_tables(dataset_ref.clone().into()) {
        let physical_table = PhysicalTable::restore_latest_revision(
            &table,
            config.data_store.clone(),
            metadata_db.clone(),
            &job_labels,
        )
        .await?
        .unwrap_or_else(|| {
            panic!(
                "Failed to restore snapshot table '{}.{}'. This is likely due to \
            the dataset or table being deleted. \n\
            Create the dataset snapshot again by running \
            `cargo run -p tests -- bless {} <start_block> <end_block>`",
                dataset_ref.name(),
                table.name(),
                dataset_ref,
            )
        });

        tables.push(physical_table.into());
    }

    Ok(tables)
}

/// Get block ranges for all files in a physical table.
///
/// This helper extracts the block ranges covered by a physical table,
/// which is used for validating that snapshots cover the same data ranges.
/// Returns a vector of BlockRange structs representing the ranges covered by the table's files.
pub async fn get_table_block_ranges(table: &Arc<PhysicalTable>) -> Vec<BlockRange> {
    let files = table.files().await.expect("Failed to get table files");

    let mut ranges = Vec::new();
    for mut file in files {
        assert_eq!(
            file.parquet_meta.ranges.len(),
            1,
            "Expected exactly one range per file"
        );

        ranges.push(file.parquet_meta.ranges.remove(0));
    }
    ranges
}

/// Assert that block ranges match between two snapshots.
///
/// This function ensures that both snapshots cover exactly the same
/// block ranges for all tables. This is a prerequisite for meaningful
/// data comparison between snapshots.
///
/// Panics if block ranges don't match between snapshots.
pub async fn assert_snapshot_block_ranges_eq(left: &SnapshotContext, right: &SnapshotContext) {
    let mut right_block_ranges: BTreeMap<TableName, Vec<BlockRange>> = BTreeMap::new();

    for table in right.physical_tables() {
        let ranges = get_table_block_ranges(table).await;
        right_block_ranges.insert(table.table_name().clone(), ranges);
    }

    for table in left.physical_tables() {
        let table_name = table.table_name();
        let mut expected_ranges = get_table_block_ranges(table).await;
        expected_ranges.sort_by_key(|r| *r.numbers.start());

        let Some(actual_ranges) = right_block_ranges.get_mut(table_name) else {
            panic!("Table {} not found in right snapshot", table_name);
        };
        actual_ranges.sort_by_key(|r| *r.numbers.start());

        let table_qualified = table.table_ref().to_string();
        assert_eq!(
            expected_ranges, *actual_ranges,
            "Block range mismatch in table {}: expected {:?}, got {:?}",
            table_qualified, expected_ranges, actual_ranges
        );
    }
}

/// Assert that two snapshots are equal, comparing both block ranges and row data.
///
/// This function performs a comprehensive comparison between two snapshots:
/// 1. Validates that both snapshots cover the same block ranges
/// 2. Compares the actual row data across all tables
///
/// The comparison orders data by block_num for consistent results and
/// uses Apache Arrow RecordBatch for efficient data comparison.
///
/// Panics if the snapshots differ in any way.
pub async fn assert_snapshots_eq(left: &SnapshotContext, right: &SnapshotContext) {
    // First check that block ranges match
    assert_snapshot_block_ranges_eq(left, right).await;

    // Then compare row data for each table
    for table in left.physical_tables() {
        let query = parse_sql(&format!(
            "select * from {} order by block_num",
            table.table_ref().to_quoted_string()
        ))
        .expect("Failed to parse SQL query");

        let left_rows: RecordBatch = left
            .query_context()
            .execute_and_concat(
                left.query_context()
                    .plan_sql(query.clone())
                    .await
                    .expect("Failed to plan SQL query for left snapshot"),
            )
            .await
            .expect("Failed to execute query for left snapshot");

        let right_rows: RecordBatch = right
            .query_context()
            .execute_and_concat(
                right
                    .query_context()
                    .plan_sql(query.clone())
                    .await
                    .expect("Failed to plan SQL query for right snapshot"),
            )
            .await
            .expect("Failed to execute query for right snapshot");

        // Use arrow's built-in equality comparison
        assert_eq!(
            left_rows,
            right_rows,
            "Data mismatch in table {}: snapshots contain different row data",
            table.table_ref()
        );
    }
}

/// Create a catalog from a dataset for table operations and queries.
///
/// This function loads a dataset definition and creates a Catalog containing
/// all the physical tables associated with the dataset. The catalog provides
/// access to both logical and physical table representations needed for
/// compaction, collection, and other table lifecycle operations.
pub async fn catalog_for_dataset(
    dataset_name: &str,
    dataset_store: &Arc<DatasetStore>,
    metadata_db: &MetadataDb,
) -> Result<Catalog, BoxError> {
    let dataset_ref = PartialReference::new(
        None,
        Name::try_from(dataset_name.to_string()).unwrap(),
        None,
    );
    let dataset = dataset_store.get_dataset(dataset_ref.clone()).await?;
    let mut tables: Vec<Arc<PhysicalTable>> = Vec::new();
    for table in dataset.resolved_tables(dataset_ref) {
        // Unwrap: we just dumped the dataset, so it must have an active physical table.
        let physical_table = PhysicalTable::get_active(&table, metadata_db.clone())
            .await?
            .unwrap();
        tables.push(physical_table.into());
    }
    let logical = LogicalCatalog::from_tables(tables.iter().map(|t| t.table()));
    Ok(Catalog::new(tables, logical))
}

/// Create a test metrics context for validating metrics collection.
///
/// This helper creates a `TestMetricsContext` that can be used to collect and validate
/// metrics during test execution. The context uses an in-memory exporter that captures
/// all recorded metrics without requiring external observability infrastructure.
///
/// # Example
///
/// ```ignore
/// let metrics = test_helpers::create_test_metrics_context();
/// let test_ctx = TestCtxBuilder::new("my_test")
///     .with_meter(metrics.meter().clone())
///     .build()
///     .await?;
///
/// // ... perform operations that should record metrics ...
///
/// let metrics = metrics.collect().await;
/// let counter = find_counter(&metrics, "my_metric_name").unwrap();
/// assert_eq!(counter.value, 1);
/// ```
pub fn create_test_metrics_context() -> crate::testlib::metrics::TestMetricsContext {
    crate::testlib::metrics::TestMetricsContext::new()
}
