//! Helper functions for testlib test environments.
//!
//! This module provides convenience functions for common test operations like
//! dataset extraction, data validation, and test setup tasks that are frequently
//! needed across multiple test scenarios.

use std::{collections::BTreeMap, fs, sync::Arc};

use common::{
    BoxError, LogicalCatalog, ParquetFooterCache, Store, arrow::array::RecordBatch, catalog::{
        JobLabels,
        physical::{Catalog, PhysicalTable},
    }, config::Config, metadata::segments::BlockRange, parquet::file::metadata::ParquetMetaData, sql, store::ObjectStoreUrl
};
use dataset_store::{
    DatasetStore, dataset_and_dependencies, manifests::DatasetManifestsStore,
    providers::ProviderConfigsStore,
};
use datasets_common::{reference::Reference, table_name::TableName};
use datasets_derived::sql_str::SqlStr;
use dump::{EndBlock, compaction::AmpCompactor, consistency_check};
use metadata_db::{MetadataDb, notification_multiplexer};
use monitoring::telemetry::metrics::Meter;

use super::fixtures::SnapshotContext;

/// Internal dump orchestration function for tests.
///
/// This function orchestrates the full extraction pipeline, including dataset
/// dependency resolution, physical table setup, and calling the extraction
/// functions. It was previously part of the `ampd dump` CLI command but is
/// now only used internally by tests.
///
/// **Note**: This is an internal function with full control over all parameters.
/// Most test code should use the simpler `dump_dataset` or `dump_dataset_continuous`
/// functions instead. This function is only exposed for advanced test scenarios that
/// need fine-grained control over dump behavior.
#[expect(clippy::too_many_arguments)]
pub async fn dump_internal(
    config: Arc<Config>,
    metadata_db: MetadataDb,
    dataset: Reference,
    ignore_deps: bool,
    end_block: EndBlock,
    max_writers: u16,
    run_every_mins: Option<u64>,
    microbatch_max_interval_override: Option<u64>,
    new_location: Option<String>,
    fresh: bool,
    meter: Option<Meter>,
) -> Result<Vec<Arc<PhysicalTable>>, BoxError> {
    let opts = dump::parquet_opts(&config.parquet);
    let cache = ParquetFooterCache::builder(opts.cache_size_mb)
                .with_weighter(|_k, v: &Arc<ParquetMetaData>| v.memory_size())
                .build();
    let data_store = match new_location {
        Some(location) => {
            let data_path = fs::canonicalize(&location)
                .map_err(|e| format!("Failed to canonicalize path '{}': {}", location, e))?;
            let base = data_path.parent();
            Arc::new(Store::new(ObjectStoreUrl::new_with_base(location, base)?)?)
        }
        None => config.data_store.clone(),
    };
    let dataset_store = {
        let provider_configs_store =
            ProviderConfigsStore::new(config.providers_store.prefixed_store());
        let dataset_manifests_store =
            DatasetManifestsStore::new(config.manifests_store.prefixed_store());
        DatasetStore::new(
            metadata_db.clone(),
            provider_configs_store,
            dataset_manifests_store,
        )
    };
    let run_every =
        run_every_mins.map(|mins| tokio::time::interval(std::time::Duration::from_secs(mins * 60)));

    let datasets = match ignore_deps {
        true => vec![dataset],
        false => {
            let datasets = dataset_and_dependencies(&dataset_store, dataset).await?;
            let dump_order: Vec<String> = datasets.iter().map(ToString::to_string).collect();
            tracing::info!("dump order: {}", dump_order.join(", "));
            datasets
        }
    };

    let mut physical_datasets = vec![];
    for dataset_ref in datasets {
        let dataset = dataset_store.get_dataset(&dataset_ref).await?;

        let job_labels = JobLabels {
            dataset_namespace: dataset_ref.namespace().clone(),
            dataset_name: dataset_ref.name().clone(),
            manifest_hash: dataset.manifest_hash().clone(),
        };

        // Create metrics registry if meter is available
        let metrics = meter
            .as_ref()
            .map(|m| Arc::new(dump::metrics::MetricsRegistry::new(m, job_labels.clone())));

        let mut tables = Vec::with_capacity(dataset.tables.len());

        if matches!(dataset.kind.as_str(), "sql" | "manifest") {
            let table_names: Vec<String> = dataset
                .tables
                .iter()
                .map(|t| t.name().to_string())
                .collect();
            tracing::info!(
                "Table dump order for dataset {}: {:?}",
                dataset_ref,
                table_names
            );
        }

        for table in dataset.resolved_tables(dataset_ref.clone().into()) {
            let db = metadata_db.clone();
            let physical_table = if fresh {
                PhysicalTable::next_revision(&table, &data_store, db, true, &job_labels).await?
            } else {
                match PhysicalTable::get_active(&table, metadata_db.clone()).await? {
                    Some(physical_table) => physical_table,
                    None => {
                        PhysicalTable::next_revision(&table, &data_store, db, true, &job_labels)
                            .await?
                    }
                }
            }.into();
            let compactor = AmpCompactor::start(&physical_table, cache.clone(), &opts, None).into();
            tables.push((physical_table, compactor));
        }
        physical_datasets.push((tables, metrics));
    }

    let notification_multiplexer = Arc::new(notification_multiplexer::spawn(metadata_db.clone()));

    let ctx = dump::Ctx {
        config: config.clone(),
        metadata_db: metadata_db.clone(),
        dataset_store: dataset_store.clone(),
        data_store: data_store.clone(),
        notification_multiplexer,
        meter,
    };

    let all_tables: Vec<Arc<PhysicalTable>> = physical_datasets
        .iter()
        .flat_map(|(tables, _)| tables.iter().map(|(t, _)| t))
        .cloned()
        .collect();

    match run_every {
        None => {
            for (tables, metrics) in &physical_datasets {
                dump::dump_tables(
                    ctx.clone(),
                    tables,
                    max_writers,
                    microbatch_max_interval_override.unwrap_or(config.microbatch_max_interval),
                    end_block,
                    metrics.clone(),
                )
                .await?
            }
        }
        Some(mut run_every) => loop {
            run_every.tick().await;

            for (tables, metrics) in &physical_datasets {
                dump::dump_tables(
                    ctx.clone(),
                    tables,
                    max_writers,
                    microbatch_max_interval_override.unwrap_or(config.microbatch_max_interval),
                    end_block,
                    metrics.clone(),
                )
                .await?;
            }
        },
    }

    Ok(all_tables)
}

/// Extract blockchain data from a dataset source and save as Parquet files.
///
/// This is the primary function for dumping datasets in tests. It runs the full
/// ETL extraction pipeline for a specified dataset, extracting data from the
/// configured source (RPC, Firehose, etc.) and saving it as Parquet files.
///
/// This function uses sensible defaults for test scenarios:
/// - Ignores dataset dependencies (dumps only the specified dataset)
/// - Uses single-writer mode (max_writers = 1) for deterministic results
/// - Does not use continuous dumping mode
/// - Does not create fresh table revisions (reuses existing when possible)
pub async fn dump_dataset(
    config: Arc<Config>,
    metadata_db: MetadataDb,
    dataset: Reference,
    end_block: u64,
) -> Result<Vec<Arc<PhysicalTable>>, BoxError> {
    dump_internal(
        config,
        metadata_db,
        dataset,
        true,                          // ignore_deps
        EndBlock::Absolute(end_block), // end_block
        1,                             // max_writers (single-writer for determinism)
        None,                          // run_every_mins (no continuous mode)
        None,                          // microbatch_max_interval_override
        None,                          // new_location
        false,                         // fresh
        None,                          // meter
    )
    .await
}

/// Extract blockchain data in continuous mode, polling at regular intervals.
///
/// This function is similar to `dump_dataset` but runs in continuous mode,
/// repeatedly polling for new data at the specified interval. This is useful
/// for testing reorg detection and continuous extraction scenarios.
///
/// The function will run indefinitely until an error occurs or the task is cancelled.
pub async fn dump_dataset_continuous(
    config: Arc<Config>,
    metadata_db: MetadataDb,
    dataset: Reference,
    run_every_mins: u64,
) -> Result<Vec<Arc<PhysicalTable>>, BoxError> {
    dump_internal(
        config,
        metadata_db,
        dataset,
        true,                 // ignore_deps
        EndBlock::None,       // end_block (continuous mode)
        1,                    // max_writers
        Some(run_every_mins), // run_every_mins (enable continuous mode)
        None,                 // microbatch_max_interval_override
        None,                 // new_location
        false,                // fresh
        None,                 // meter
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
    dataset_store: &Arc<DatasetStore>,
    metadata_db: &MetadataDb,
) -> Result<Catalog, BoxError> {
    let dataset_ref: Reference = format!("_/{dataset_name}@latest")
        .parse()
        .expect("should be valid reference");
    let dataset = dataset_store.get_dataset(&dataset_ref).await?;
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
