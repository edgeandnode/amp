//! Helper functions for testlib test environments.
//!
//! This module provides convenience functions for common test operations like
//! dataset extraction, data validation, and test setup tasks that are frequently
//! needed across multiple test scenarios.

pub mod forge;
pub mod git;

use std::{collections::BTreeMap, sync::Arc};

use common::{
    BoxError, CachedParquetData, LogicalCatalog, ParquetFooterCache,
    arrow::array::RecordBatch,
    catalog::physical::{Catalog, PhysicalTable},
    metadata::segments::BlockRange,
    sql,
    sql_str::SqlStr,
};
use dataset_store::{DatasetKind, DatasetStore};
use datasets_common::{reference::Reference, table_name::TableName};
use dump::{EndBlock, compaction::AmpCompactor, consistency_check};
use metadata_db::{MetadataDb, notification_multiplexer};
use worker::config::Config as WorkerConfig;

use super::fixtures::SnapshotContext;

/// Internal dump orchestration function for tests.
///
/// This function orchestrates the full extraction pipeline for a single dataset,
/// including physical table setup and calling the extraction functions. This
/// provides test access to the internal dump functionality used by the
/// worker-based extraction system.
///
/// Most test code should use the simpler `dump_dataset` function instead.
/// This function is only exposed for advanced test scenarios that need
/// fine-grained control over dump parameters (end_block type, max_writers,
/// microbatch interval).
pub async fn dump_internal(
    config: WorkerConfig,
    metadata_db: MetadataDb,
    dataset_store: DatasetStore,
    dataset_ref: Reference,
    end_block: EndBlock,
    max_writers: u16,
    microbatch_max_interval_override: Option<u64>,
) -> Result<Vec<Arc<PhysicalTable>>, BoxError> {
    let hash_reference = dataset_store
        .resolve_revision(&dataset_ref)
        .await?
        .ok_or_else(|| format!("Dataset '{}' not found", dataset_ref))?;

    // Get dataset using hash reference
    let dataset = dataset_store.get_dataset(&hash_reference).await?;

    // Parse dataset kind
    let kind: DatasetKind = dataset.kind.parse()?;

    let mut tables = Vec::with_capacity(dataset.tables.len());

    let data_store = config.data_store.clone();
    let opts = dump::parquet_opts(&config.parquet);
    let cache = ParquetFooterCache::builder(opts.cache_size_mb)
        .with_weighter(|_k, v: &CachedParquetData| v.metadata.memory_size())
        .build();
    for table in dataset.resolved_tables(dataset_ref.clone().into()) {
        let db = metadata_db.clone();
        // Always reuse existing physical tables in test scenarios (fresh = false)
        let physical_table: Arc<PhysicalTable> =
            match PhysicalTable::get_active(&table, metadata_db.clone()).await? {
                Some(physical_table) => physical_table,
                None => {
                    PhysicalTable::next_revision(&table, &data_store, db, true, &hash_reference)
                        .await?
                }
            }
            .into();
        let compactor = AmpCompactor::start(
            metadata_db.clone(),
            cache.clone(),
            opts.clone(),
            physical_table.clone(),
            None,
        )
        .into();
        tables.push((physical_table, compactor));
    }

    let notification_multiplexer = Arc::new(notification_multiplexer::spawn(metadata_db.clone()));

    let all_tables: Vec<Arc<PhysicalTable>> = tables.iter().map(|(t, _)| t).cloned().collect();

    let ctx = dump::Ctx {
        config: config.dump_config(),
        metadata_db: metadata_db.clone(),
        dataset_store: dataset_store.clone(),
        data_store: data_store.clone(),
        notification_multiplexer: notification_multiplexer.clone(),
        metrics: None,
    };

    dump::dump_tables(
        ctx,
        &hash_reference,
        kind,
        &tables,
        max_writers,
        microbatch_max_interval_override.unwrap_or(config.microbatch_max_interval),
        end_block,
    )
    .await?;

    Ok(all_tables)
}

/// Extract blockchain data from a dataset source and save as Parquet files.
///
/// This is the primary function for dumping datasets in tests. It runs the full
/// ETL extraction pipeline for a specified dataset, extracting data from the
/// configured source (RPC, Firehose, etc.) and saving it as Parquet files.
///
/// This function uses sensible defaults for test scenarios:
/// - Dumps only the specified dataset (dependencies are not resolved)
/// - Uses single-writer mode (max_writers = 1) for deterministic results
/// - Does not create fresh table revisions (reuses existing when possible)
pub async fn dump_dataset(
    config: WorkerConfig,
    metadata_db: MetadataDb,
    dataset_store: DatasetStore,
    dataset: Reference,
    end_block: u64,
) -> Result<Vec<Arc<PhysicalTable>>, BoxError> {
    dump_internal(
        config,
        metadata_db,
        dataset_store,
        dataset,
        EndBlock::Absolute(end_block), // end_block
        1,                             // max_writers (single-writer for determinism)
        None,                          // microbatch_max_interval_override
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
/// from their snapshot state via the Admin API. Dataset snapshots are reference data
/// that have been validated and saved as the expected baseline for tests.
///
/// The function:
/// 1. Calls the Admin API to restore physical table metadata from storage
/// 2. Loads the restored tables as `Arc<PhysicalTable>` objects for test verification
///
/// This is typically used to set up known-good data for comparison testing.
pub async fn restore_dataset_snapshot(
    ampctl: &super::fixtures::Ampctl,
    dataset_store: &DatasetStore,
    metadata_db: &MetadataDb,
    dataset_ref: &Reference,
) -> Result<Vec<Arc<PhysicalTable>>, BoxError> {
    // 1. Restore via Admin API (indexes files into metadata DB)
    let restored_info = ampctl.restore_dataset(dataset_ref).await?;

    tracing::debug!(
        %dataset_ref,
        tables = restored_info.len(),
        "Restored tables via Admin API, loading PhysicalTable objects"
    );

    // 2. Load the dataset to get ResolvedTables
    let hash_ref = dataset_store
        .resolve_revision(dataset_ref)
        .await?
        .ok_or_else(|| format!("dataset '{}' not found", dataset_ref))?;
    let dataset = dataset_store.get_dataset(&hash_ref).await?;

    // 3. Load PhysicalTable objects for each restored table
    let mut tables = Vec::<Arc<PhysicalTable>>::new();

    for table in Arc::new(dataset).resolved_tables(dataset_ref.clone().into()) {
        // Verify this table was restored
        let table_name = table.name().to_string();
        let restored = restored_info
            .iter()
            .find(|info| info.table_name == table_name)
            .ok_or_else(|| {
                format!(
                    "Table '{}' not found in restored tables for dataset '{}'",
                    table_name, dataset_ref
                )
            })?;

        tracing::debug!(
            %dataset_ref,
            %table_name,
            location_id = restored.location_id,
            "Loading PhysicalTable from metadata DB"
        );

        // Load the PhysicalTable using get_active (it was just marked active by restore)
        let physical_table = PhysicalTable::get_active(&table, metadata_db.clone())
            .await?
            .ok_or_else(|| {
                format!(
                    "Failed to load active PhysicalTable for '{}' after restoration. \
                    This indicates the restore operation did not properly activate the table.",
                    table_name
                )
            })?;

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
        let sql_string = format!(
            "select * from {} order by block_num",
            table.table_ref().to_quoted_string()
        );

        // SAFETY: Validation is deferred to the SQL parser which will return appropriate errors
        // for empty or invalid SQL. The format! macro ensures non-empty output.
        let sql_str = SqlStr::new_unchecked(sql_string);
        let query = sql::parse(sql_str).expect("Failed to parse SQL query");

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
    dataset_store: &DatasetStore,
    metadata_db: &MetadataDb,
) -> Result<Catalog, BoxError> {
    let dataset_ref: Reference = format!("_/{dataset_name}@latest")
        .parse()
        .expect("should be valid reference");
    let hash_ref = dataset_store
        .resolve_revision(&dataset_ref)
        .await?
        .ok_or_else(|| format!("dataset '{}' not found", dataset_ref))?;
    let dataset = dataset_store.get_dataset(&hash_ref).await?;
    let mut tables: Vec<Arc<PhysicalTable>> = Vec::new();
    for table in dataset.resolved_tables(dataset_ref.into()) {
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
pub fn create_test_metrics_context() -> crate::testlib::metrics::TestMetricsContext {
    crate::testlib::metrics::TestMetricsContext::new()
}
