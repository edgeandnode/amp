//! # SQL Datasets dump implementation
//!
//! This module implements the core logic for dumping SQL-based datasets to Parquet files.
//! Unlike raw dataset dumps that extract blockchain data directly, SQL dataset dumps execute
//! user-defined SQL queries against existing datasets to create derived or transformed datasets.
//!
//! ## Overview
//!
//! SQL datasets allow users to define custom transformations and aggregations over blockchain
//! data using SQL queries. Each SQL dataset consists of one or more tables, where each table
//! is defined by a SQL query that can reference other datasets as data sources. The dump
//! process executes these queries and materializes the results as Parquet files.
//!
//! ## Dataset Types
//!
//! SQL datasets can be either incremental or non-incremental:
//!
//! - **Incremental Datasets**: Process data in block ranges and can be updated incrementally
//!   as new blocks become available. These datasets maintain state about which block ranges
//!   have been processed and only compute results for new or missing ranges.
//!
//! - **Non-Incremental Datasets**: Recompute the entire dataset on each dump operation.
//!   These are typically used for queries that require global aggregations or when the
//!   result depends on the complete dataset state.
//!
//! ## Dump Process
//!
//! The SQL dataset dump process follows these main steps:
//!
//! 1. **Query Analysis**: Analyzes each SQL query to determine if it's incremental and
//!    identifies the maximum end block from its dependencies. This helps establish the
//!    available data range for processing.
//!
//! 2. **Block Range Resolution**: Determines the actual block range to process, either
//!    from explicit parameters or by resolving relative block numbers against the
//!    maximum available block from query dependencies.
//!
//! 3. **Parallel Table Processing**: Spawns separate tasks for each table in the dataset,
//!    allowing multiple SQL queries to be executed concurrently for better performance.
//!
//! 4. **Incremental vs Full Processing**:
//!    - For incremental datasets: Identifies unprocessed block ranges and processes them
//!      in configurable batch sizes to manage memory usage and query complexity.
//!    - For non-incremental datasets: Creates a new table revision and processes the
//!      entire query result in one operation.
//!
//! 5. **Query Execution**: Executes the SQL query using DataFusion's query engine,
//!    streaming results to avoid loading large datasets entirely into memory.
//!
//! 6. **Result Materialization**: Writes query results to Parquet files with proper
//!    metadata tracking for incremental processing capabilities.
//!
//! ## Incremental Processing Strategy
//!
//! For incremental datasets, the dump process implements sophisticated block range management:
//!
//! - **Gap Detection**: Identifies which block ranges haven't been processed yet by
//!   computing the complement of already-processed ranges within the target range.
//!
//! - **Batch Processing**: Splits large unprocessed ranges into smaller batches based
//!   on the configured `input_batch_size_blocks` parameter. This prevents memory
//!   exhaustion and allows for better progress tracking.
//!
//! - **Sequential Batch Execution**: Processes batches sequentially within each table
//!   to maintain data consistency and avoid overwhelming the query engine.
//!
//! - **Metadata Updates**: Records successful processing of each batch in the metadata
//!   database, enabling resume capabilities and preventing duplicate work.
//!
//! ## Query Context Management
//!
//! The module manages multiple query contexts for different purposes:
//!
//! - **Source Context**: Created specifically for executing the SQL query with access
//!   to the required datasets and proper configuration for the query environment.
//!
//! - **Destination Context**: Manages the target dataset structure and metadata for
//!   writing results to the appropriate location.
//!
//! - **Environment Isolation**: Each query execution uses an isolated runtime
//!   environment to prevent interference between concurrent operations.
//!
//! ## Error Handling and Reliability
//!
//! The SQL dataset dump process includes several reliability features:
//!
//! - **Dependency Validation**: Checks that all required source datasets have data
//!   available before attempting to execute queries, preventing unnecessary work.
//!
//! - **Atomic Batch Processing**: Each batch is processed atomically, ensuring that
//!   partial results are not recorded in case of failures.
//!
//! - **Parallel Task Management**: Uses a specialized join set (`DumpPartitionTasksJoinSet`)
//!   to coordinate concurrent table processing tasks, ensuring that if any table processing
//!   fails, all remaining tasks are immediately terminated to prevent partial dumps.
//!
//! - **Metadata Consistency**: Commits metadata updates only after successful
//!   completion of each batch, maintaining consistency between data files and
//!   processing state.
//!
//! ## Performance Considerations
//!
//! The implementation includes several optimizations for handling complex SQL workloads:
//!
//! - **Streaming Execution**: Uses streaming query execution to process large result
//!   sets without loading everything into memory simultaneously.
//!
//! - **Configurable Batch Sizes**: Allows tuning of batch sizes to balance memory
//!   usage, query complexity, and processing throughput.
//!
//! - **Parallel Table Processing**: Processes multiple tables concurrently when they
//!   don't have interdependencies, maximizing resource utilization.
//!
//! - **Incremental Updates**: Avoids reprocessing already-computed data by maintaining
//!   precise tracking of processed block ranges per table.

use std::{collections::BTreeMap, sync::Arc};

use common::{
    catalog::physical::PhysicalTable,
    multirange::MultiRange,
    query_context::{QueryContext, QueryEnv},
    BlockNum, BoxError,
};
use dataset_store::{
    sql_datasets::{is_incremental, max_end_block, SqlDataset},
    DatasetStore,
};
use futures::TryStreamExt as _;
use tracing::instrument;

use super::{block_ranges, tasks::FailFastJoinSet, Ctx};
use crate::parquet_writer::{commit_metadata, ParquetFileWriter, ParquetWriterProperties};

/// Dumps a SQL dataset
#[instrument(skip_all, fields(dataset = %dataset.name()), err)]
pub async fn dump(
    ctx: Ctx,
    query_ctx: Arc<QueryContext>,
    dataset: SqlDataset,
    env: &QueryEnv,
    block_ranges_by_table: BTreeMap<String, MultiRange>,
    parquet_opts: &ParquetWriterProperties,
    input_batch_size_blocks: u64,
    (start, end): (i64, Option<i64>),
) -> Result<(), BoxError> {
    let dataset_name = dataset.dataset.name.as_str();

    let mut join_set = FailFastJoinSet::<Result<(), BoxError>>::new();
    for (table, query) in dataset.queries {
        let dataset_store = ctx.dataset_store.clone();
        let data_store = ctx.data_store.clone();
        let query_ctx = query_ctx.clone();
        let env = env.clone();
        let block_ranges_by_table = block_ranges_by_table.clone();
        let parquet_opts = parquet_opts.clone();

        join_set.spawn(async move {
            let physical_table = {
                let tables = query_ctx.catalog().tables();
                tables.iter().find(|t| t.table_name() == table).unwrap()
            };

            let src_ctx: Arc<QueryContext> = dataset_store
                .clone()
                .ctx_for_sql(&query, env.clone())
                .await?
                .into();
            let plan = src_ctx.plan_sql(query.clone()).await?;
            let is_incr = is_incremental(&plan)?;
            let (start, end) = match (start, end) {
                (start, Some(end)) if start >= 0 && end >= 0 => {
                    (start as BlockNum, end as BlockNum)
                }
                _ => {
                    match max_end_block(&plan, &src_ctx).await? {
                        Some(max_end_block) => {
                            block_ranges::resolve_relative(start, end, max_end_block)?
                        }
                        None => {
                            // If the dependencies have synced nothing, we have nothing to do.
                            tracing::warn!("no blocks to dump for {table}, dependencies are empty");
                            return Ok::<(), BoxError>(());
                        }
                    }
                }
            };

            if is_incr {
                let ranges_to_scan = block_ranges_by_table[&table].complement(start, end);
                for (start, end) in ranges_to_scan.ranges {
                    let mut start = start;
                    while start <= end {
                        let batch_end = std::cmp::min(start + input_batch_size_blocks - 1, end);
                        tracing::info!(
                            "dumping {} between blocks {start} and {batch_end}",
                            physical_table.table_ref()
                        );

                        dump_sql_query(
                            &dataset_store,
                            &query,
                            &env,
                            start,
                            batch_end,
                            physical_table,
                            &parquet_opts,
                        )
                        .await?;
                        start = batch_end + 1;
                    }
                }
            } else {
                let physical_table = PhysicalTable::next_revision(
                    physical_table.table(),
                    &data_store,
                    dataset_store.metadata_db.clone(),
                    false,
                )
                .await?;
                tracing::info!(
                    "dumping entire {} to {}",
                    physical_table.table_ref(),
                    physical_table.url()
                );
                dump_sql_query(
                    &dataset_store,
                    &query,
                    &env,
                    start,
                    end,
                    &physical_table,
                    &parquet_opts,
                )
                .await?;
            }

            Ok(())
        });
    }

    // Wait for all the jobs to finish, returning an error if any job panics or fails
    if let Err(err) = join_set.try_wait_all().await {
        tracing::error!(dataset=%dataset_name, error=%err, "dataset dump failed");
        return Err(err.into_box_error());
    }

    Ok(())
}

#[instrument(skip_all, err)]
async fn dump_sql_query(
    dataset_store: &Arc<DatasetStore>,
    query: &datafusion::sql::parser::Statement,
    env: &QueryEnv,
    start: BlockNum,
    end: BlockNum,
    physical_table: &common::catalog::physical::PhysicalTable,
    parquet_opts: &ParquetWriterProperties,
) -> Result<(), BoxError> {
    use dataset_store::sql_datasets::execute_query_for_range;

    dbg!(physical_table.table());
    let store = dataset_store.clone();
    let mut stream = execute_query_for_range(query.clone(), store, env.clone(), start, end).await?;
    let mut writer = ParquetFileWriter::new(physical_table.clone(), parquet_opts.clone(), start)?;

    while let Some(batch) = stream.try_next().await? {
        writer.write(&batch).await?;
    }

    let (parquet_meta, object_meta) = writer.close(end).await?;
    commit_metadata(
        &dataset_store.metadata_db,
        parquet_meta,
        object_meta,
        physical_table.location_id(),
    )
    .await
}
