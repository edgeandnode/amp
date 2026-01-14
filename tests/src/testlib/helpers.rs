//! Helper functions for testlib test environments.
//!
//! This module provides convenience functions for common test operations like
//! dataset extraction, data validation, and test setup tasks that are frequently
//! needed across multiple test scenarios.

pub mod forge;
pub mod git;

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use amp_data_store::DataStore;
use amp_dataset_store::DatasetStore;
use common::{
    BoxError, LogicalCatalog,
    arrow::array::RecordBatch,
    catalog::physical::{Catalog, PhysicalTable},
    metadata::segments::BlockRange,
    sql,
    sql_str::SqlStr,
};
use datasets_common::{reference::Reference, table_name::TableName};
use dump::consistency_check;
use worker::job::JobId;

use super::fixtures::SnapshotContext;

/// Wait for a job to reach a completion state.
///
/// Polls the job status via the Admin API at regular intervals until the job
/// reaches the appropriate state or the timeout expires.
///
/// # Behavior
///
/// - **Finalized jobs** (`is_continuous = false`): Waits for terminal states
///   (COMPLETED, STOPPED, or FAILED)
/// - **Continuous jobs** (`is_continuous = true`): Returns as soon as the job
///   reaches RUNNING state, since continuous jobs never complete
///
/// # Important
///
/// For continuous jobs, returning on RUNNING does **not** guarantee any data
/// has been processed yet. Tests should either:
/// - Add an appropriate sleep after this function returns
/// - Query the dataset to verify expected data exists
///
/// # Arguments
///
/// * `ampctl` - The Admin API client fixture
/// * `job_id` - The ID of the job to monitor
/// * `is_continuous` - If true, return on RUNNING state; if false, wait for terminal state
/// * `timeout` - Maximum time to wait for the expected state
/// * `poll_interval` - Time between status checks
///
/// # Returns
///
/// Returns the `JobInfo` when the job reaches the expected state, or an error
/// if the timeout expires or the job cannot be found.
pub async fn wait_for_job_completion(
    ampctl: &super::fixtures::Ampctl,
    job_id: JobId,
    is_continuous: bool,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<ampctl::client::jobs::JobInfo, BoxError> {
    let start = tokio::time::Instant::now();

    loop {
        let job_info = ampctl
            .jobs()
            .get(&job_id)
            .await?
            .ok_or_else(|| format!("Job {} not found", job_id))?;

        match job_info.status.as_str() {
            "RUNNING" if is_continuous => return Ok(job_info),
            "COMPLETED" | "STOPPED" | "FAILED" => return Ok(job_info),
            _ => {
                if start.elapsed() > timeout {
                    return Err(format!(
                        "Timeout waiting for job {} to complete (status: {})",
                        job_id, job_info.status
                    )
                    .into());
                }
                tokio::time::sleep(poll_interval).await;
            }
        }
    }
}

/// Deploy a dataset via Admin API and wait for job completion.
///
/// This function schedules a dataset extraction job via the worker/scheduler
/// infrastructure and waits for it to complete. It exercises the full production
/// code path including:
/// - Job scheduling via the Admin API
/// - PostgreSQL LISTEN/NOTIFY for job notifications
/// - Worker job execution
/// - Job status tracking
///
/// # Arguments
///
/// * `ampctl` - The Admin API client fixture
/// * `dataset_ref` - Reference to the dataset to deploy (namespace/name@version)
/// * `end_block` - The block number to extract up to
/// * `timeout` - Maximum time to wait for job completion
///
/// # Returns
///
/// Returns the final `JobInfo` when the job completes, or an error if the job
/// fails, times out, or cannot be scheduled.
///
/// # Example
///
/// ```ignore
/// let ampctl = ctx.new_ampctl();
/// let dataset_ref: Reference = "_/eth_rpc@0.0.0".parse().unwrap();
///
/// let job_info = deploy_and_wait(&ampctl, &dataset_ref, 1000, Duration::from_secs(60))
///     .await
///     .expect("Failed to deploy dataset");
///
/// assert_eq!(job_info.status, "COMPLETED");
/// ```
pub async fn deploy_and_wait(
    ampctl: &super::fixtures::Ampctl,
    dataset_ref: &Reference,
    end_block: Option<u64>,
    timeout: Duration,
) -> Result<ampctl::client::jobs::JobInfo, BoxError> {
    let is_continuous = end_block.is_none();
    let job_id = ampctl
        .dataset_deploy(
            &dataset_ref.to_string(),
            end_block,
            Some(1), // parallelism
            None,    // worker_id (auto-select)
        )
        .await?;

    wait_for_job_completion(
        ampctl,
        job_id,
        is_continuous,
        timeout,
        Duration::from_millis(100),
    )
    .await
}

/// Load physical tables for a dataset from the data store.
///
/// This function resolves a dataset reference to its physical tables, which
/// represent the actual Parquet file storage locations for each table in the
/// dataset. Physical tables contain metadata about file paths, schemas, and
/// block ranges.
///
/// # Arguments
///
/// * `dataset_store` - The dataset store for resolving dataset metadata
/// * `data_store` - The object store containing the physical Parquet files
/// * `dataset_ref` - Reference to the dataset (namespace/name@version)
///
/// # Returns
///
/// Returns a vector of physical tables, one for each table defined in the
/// dataset manifest.
///
/// # Panics
///
/// Panics if:
/// - The dataset reference cannot be resolved
/// - The dataset metadata cannot be loaded
/// - Any physical table is not found in the data store
pub async fn load_physical_tables(
    dataset_store: &DatasetStore,
    data_store: &DataStore,
    dataset_ref: &Reference,
) -> Result<Vec<Arc<PhysicalTable>>, BoxError> {
    let hash_ref = dataset_store
        .resolve_revision(&dataset_ref)
        .await
        .expect("Failed to resolve dataset")
        .expect("Dataset not found");

    let dataset = dataset_store
        .get_dataset(&hash_ref)
        .await
        .expect("Failed to get dataset");

    let mut dumped_tables: Vec<Arc<PhysicalTable>> = Vec::new();
    for table in dataset.resolved_tables(dataset_ref.clone().into()) {
        let revision = data_store
            .get_table_active_revision(dataset.reference(), table.name())
            .await
            .expect("Failed to get active revision")
            .expect("Active revision not found");

        let physical_table =
            PhysicalTable::from_active_revision(data_store.clone(), table, revision);
        dumped_tables.push(physical_table.into());
    }

    Ok(dumped_tables)
}

/// Run consistency check on a physical table.
///
/// This function validates the integrity of a physical table by checking that
/// all registered files in the metadata database actually exist in the object
/// store, and that no extra files are present. This helps detect data corruption
/// or synchronization issues between metadata and storage.
pub async fn check_table_consistency(
    table: &Arc<PhysicalTable>,
    store: &DataStore,
) -> Result<(), BoxError> {
    consistency_check(table, store).await.map_err(Into::into)
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
    data_store: &DataStore,
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

    for table in dataset.resolved_tables(dataset_ref.clone().into()) {
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

        // Load the PhysicalTable using the active revision (it was just marked active by restore)
        let revision = data_store
            .get_table_active_revision(dataset.reference(), table.name())
            .await?
            .ok_or_else(|| {
                format!(
                    "Failed to load active PhysicalTable for '{}' after restoration. \
                    This indicates the restore operation did not properly activate the table.",
                    table_name
                )
            })?;

        let physical_table =
            PhysicalTable::from_active_revision(data_store.clone(), table, revision);
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
    data_store: &DataStore,
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
        let revision = data_store
            .get_table_active_revision(dataset.reference(), table.name())
            .await?
            .expect("Active revision must exist after dump");

        let physical_table =
            PhysicalTable::from_active_revision(data_store.clone(), table, revision);
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
