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

use std::{collections::BTreeSet, sync::Arc};

use common::{
    BlockNum, BoxError, Dataset,
    catalog::physical::PhysicalTable,
    metadata::range::{BlockRange, missing_block_ranges},
    plan_visitors::is_incremental,
    query_context::{QueryContext, QueryEnv, parse_sql},
    streaming_query::{StreamState, StreamingQuery, watermark_updates},
};
use datafusion::{common::cast::as_fixed_size_binary_array, sql::parser::Statement};
use dataset_store::{DatasetStore, sql_datasets::SqlDataset};
use futures::TryStreamExt as _;
use tracing::instrument;

use super::{Ctx, block_ranges, tasks::FailFastJoinSet};
use crate::parquet_writer::{
    CompletedFile, ParquetFileWriter, ParquetWriterProperties, commit_metadata,
};

/// Dumps a SQL dataset table
#[instrument(skip_all, fields(dataset = %dataset.name()), err)]
pub async fn dump_table(
    ctx: Ctx,
    dataset: SqlDataset,
    env: &QueryEnv,
    table: Arc<PhysicalTable>,
    parquet_opts: &ParquetWriterProperties,
    input_batch_size_blocks: u64,
    (start, end): (i64, Option<i64>),
) -> Result<(), BoxError> {
    let dataset_name = dataset.dataset.name.as_str();
    let table_name = table.table_name().to_string();
    let query = dataset
        .queries
        .get(&table_name)
        .ok_or_else(|| {
            format!(
                "table `{}` not found in dataset `{}`",
                table_name, dataset_name
            )
        })?
        .clone();

    let mut join_set = FailFastJoinSet::<Result<(), BoxError>>::new();
    let dataset_store = ctx.dataset_store.clone();
    let data_store = ctx.data_store.clone();
    let env = env.clone();
    let parquet_opts = parquet_opts.clone();

    join_set.spawn(async move {
        let src_ctx: Arc<QueryContext> = dataset_store
            .clone()
            .ctx_for_sql(&query, env.clone())
            .await?
            .into();
        let src_datasets: BTreeSet<&str> = src_ctx
            .catalog()
            .tables()
            .iter()
            .map(|t| t.dataset().name.as_str())
            .collect();

        let plan = src_ctx.plan_sql(query.clone()).await?;
        let is_incr = is_incremental(&plan)?;
        let (start, end) = match (start, end) {
            (start, Some(end)) if start >= 0 && end >= 0 => (start as BlockNum, end as BlockNum),
            _ => {
                match src_ctx.max_end_block(&plan).await? {
                    Some(max_end_block) => {
                        block_ranges::resolve_relative(start, end, max_end_block)?
                    }
                    None => {
                        // If the dependencies have synced nothing, we have nothing to do.
                        tracing::warn!(
                            "no blocks to dump for {table_name}, dependencies are empty"
                        );
                        return Ok::<(), BoxError>(());
                    }
                }
            }
        };

        if is_incr {
            let synced_range = table.synced_range().await?;
            if let Some(range) = synced_range.as_ref() {
                tracing::info!(
                    "table `{}` has scanned block range [{}-{}]",
                    table_name,
                    range.start(),
                    range.end(),
                );
            }
            let ranges_to_scan = synced_range
                .map(|synced| missing_block_ranges(synced, start..=end))
                .unwrap_or(vec![start..=end]);
            for range in ranges_to_scan {
                let (mut start, end) = range.into_inner();
                while start <= end {
                    let batch_end = std::cmp::min(start + input_batch_size_blocks - 1, end);
                    tracing::info!("dumping {table_name} between blocks {start} and {batch_end}");

                    let range = resolve_block_range(
                        env.clone(),
                        &dataset_store,
                        &src_datasets,
                        table.network().to_string(),
                        start,
                        batch_end,
                    )
                    .await?;
                    dump_sql_query(
                        &dataset_store,
                        &query,
                        &env,
                        range,
                        table.clone(),
                        &parquet_opts,
                    )
                    .await?;
                    start = batch_end + 1;
                }
            }
        } else {
            let physical_table: Arc<PhysicalTable> = PhysicalTable::next_revision(
                table.table(),
                &data_store,
                dataset_store.metadata_db.clone(),
                false,
            )
            .await?
            .into();
            tracing::info!(
                "dumping entire {} to {}",
                physical_table.table_ref(),
                physical_table.url()
            );
            let range = resolve_block_range(
                env.clone(),
                &dataset_store,
                &src_datasets,
                physical_table.network().to_string(),
                start,
                end,
            )
            .await?;
            dump_sql_query(
                &dataset_store,
                &query,
                &env,
                range,
                physical_table,
                &parquet_opts,
            )
            .await?;
        }

        Ok(())
    });

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
    query: &Statement,
    env: &QueryEnv,
    range: BlockRange,
    physical_table: Arc<PhysicalTable>,
    parquet_opts: &ParquetWriterProperties,
) -> Result<(), BoxError> {
    let (start, end) = range.numbers.clone().into_inner();
    let mut stream = {
        let ctx = Arc::new(dataset_store.ctx_for_sql(&query, env.clone()).await?);
        let plan = ctx.plan_sql(query.clone()).await?;
        let initial_state = StreamState::new(
            watermark_updates(ctx.clone(), physical_table.metadata_db.clone()).await?,
            start,
        );
        StreamingQuery::spawn(initial_state, ctx, plan, Some(end), true)
            .await?
            .as_record_batch_stream()
    };
    let mut writer = ParquetFileWriter::new(physical_table.clone(), parquet_opts.clone(), start)?;

    while let Some(batch) = stream.try_next().await? {
        writer.write(&batch).await?;
    }

    let CompletedFile {
        parquet_meta,
        object_meta,
        footer_bytes,
        location_id,
    } = writer.close(range).await?;

    commit_metadata(
        &dataset_store.metadata_db,
        parquet_meta,
        object_meta,
        footer_bytes,
        location_id,
    )
    .await
}

/// Derive the BlockRange for the dense block number range [start, end] from the `blocks` table
/// of the raw dataset this table is derived from. For now, we only support materializing SQL
/// queries depending on block data from a single network.
async fn resolve_block_range(
    env: QueryEnv,
    dataset_store: &Arc<DatasetStore>,
    src_datasets: &BTreeSet<&str>,
    network: String,
    start: BlockNum,
    end: BlockNum,
) -> Result<BlockRange, BoxError> {
    let dataset = {
        let mut datasets: Vec<Dataset> = dataset_store
            .all_datasets()
            .await?
            .into_iter()
            .filter(|d| d.network == network)
            .collect();

        if datasets.is_empty() {
            return Err(format!("no provider found for network {network}").into());
        }

        match datasets
            .iter()
            .position(|d| src_datasets.contains(d.name.as_str()))
        {
            Some(index) => datasets.remove(index),
            None => {
                // Make sure fallback provider selection is deterministic.
                datasets.sort_by(|a, b| a.name.cmp(&b.name));
                if datasets.len() > 1 {
                    tracing::debug!(
                        "selecting provider {} for network {}",
                        datasets[0].name,
                        network
                    );
                }
                datasets.remove(0)
            }
        }
    };

    assert!(dataset.tables.iter().any(|t| t.name() == "blocks"));
    let blocks_table = format!("{}.blocks", dataset.name);
    let query = parse_sql(&format!(
        r#"
        SELECT * FROM
            (SELECT hash FROM {blocks_table} WHERE block_num = {end}),
            (SELECT parent_hash FROM {blocks_table} WHERE block_num = {start})
        "#
    ))?;
    let ctx = dataset_store.ctx_for_sql(&query, env).await?;
    let plan = ctx.plan_sql(query).await?;
    let results = ctx.execute_and_concat(plan).await?;
    if results.num_rows() != 1 {
        return Err(format!(
            "failed to resolve block metadata from {} for range {} to {}",
            blocks_table, start, end,
        )
        .into());
    }
    let hash = as_fixed_size_binary_array(results.column_by_name("hash").unwrap())
        .unwrap()
        .value(0)
        .try_into()
        .unwrap();
    let prev_hash = Some(
        as_fixed_size_binary_array(results.column_by_name("parent_hash").unwrap())
            .unwrap()
            .value(0)
            .try_into()
            .unwrap(),
    );
    Ok(BlockRange {
        numbers: start..=end,
        network,
        hash,
        prev_hash,
    })
}
