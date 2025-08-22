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
//!   on the configured `microbatch_max_interval` parameter. This prevents memory
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

use std::{ops::RangeInclusive, sync::Arc};

use common::{
    BlockNum, BoxError, QueryContext,
    catalog::physical::{Catalog, PhysicalTable},
    notification_multiplexer::NotificationMultiplexerHandle,
    query_context::{DetachedLogicalPlan, PlanningContext, QueryEnv},
};
use dataset_store::{DatasetStore, sql_datasets::SqlDataset};
use futures::StreamExt as _;
use tracing::instrument;

use super::{Ctx, block_ranges, tasks::FailFastJoinSet};
use crate::{
    parquet_writer::{ParquetFileWriter, ParquetWriterProperties, commit_metadata},
    streaming_query::{QueryMessage, StreamingQuery},
};

/// Dumps a SQL dataset table
#[instrument(skip_all, fields(dataset = %dataset.name()), err)]
pub async fn dump_table(
    ctx: Ctx,
    dataset: SqlDataset,
    env: &QueryEnv,
    table: Arc<PhysicalTable>,
    parquet_opts: &ParquetWriterProperties,
    microbatch_max_interval: u64,
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
        let catalog = dataset_store
            .clone()
            .catalog_for_sql(&query, env.clone())
            .await?;
        let planning_ctx = PlanningContext::new(catalog.logical().clone());

        let plan = planning_ctx.plan_sql(query.clone()).await?;
        let is_incr = plan.is_incremental()?;

        let (start, end) = match (start, end) {
            (start, Some(end)) if start >= 0 && end >= 0 => (start as BlockNum, end as BlockNum),
            _ => {
                let ctx = QueryContext::for_catalog(catalog.clone(), env.clone(), false).await?;
                match ctx.max_end_block(&plan.clone().attach_to(&ctx)?).await? {
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

        let start_block = start
            .try_into()
            .map_err(|e| format!("start_block value {} is out of range: {}", start, e))?;

        if is_incr {
            ctx.metadata_db
                .check_start_block(table.location_id(), start_block)
                .await?;

            for range in table.missing_ranges(start..=end).await? {
                dump_sql_query(
                    &dataset_store,
                    &env,
                    &catalog,
                    plan.clone(),
                    range,
                    table.clone(),
                    &parquet_opts,
                    microbatch_max_interval,
                    &ctx.notification_multiplexer,
                )
                .await?;
            }
        } else {
            let physical_table: Arc<PhysicalTable> = PhysicalTable::next_revision(
                table.table(),
                &data_store,
                dataset_store.metadata_db.clone(),
                false,
                start_block as i64,
            )
            .await?
            .into();
            dump_sql_query(
                &dataset_store,
                &env,
                &catalog,
                plan.clone(),
                start..=end,
                physical_table,
                &parquet_opts,
                microbatch_max_interval,
                &ctx.notification_multiplexer,
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
    env: &QueryEnv,
    catalog: &Catalog,
    query: DetachedLogicalPlan,
    range: RangeInclusive<BlockNum>,
    physical_table: Arc<PhysicalTable>,
    parquet_opts: &ParquetWriterProperties,
    microbatch_max_interval: u64,
    notification_multiplexer: &Arc<NotificationMultiplexerHandle>,
) -> Result<(), BoxError> {
    tracing::info!(
        "dumping {} [{}-{}]",
        physical_table.table_ref(),
        range.start(),
        range.end(),
    );
    let mut stream = {
        StreamingQuery::spawn(
            env.clone(),
            catalog.clone(),
            dataset_store.clone(),
            query,
            *range.start(),
            Some(*range.end()),
            notification_multiplexer,
            true,
            microbatch_max_interval,
        )
        .await?
        .as_stream()
    };

    let mut microbatch_start = *range.start();
    let mut writer = ParquetFileWriter::new(
        physical_table.clone(),
        parquet_opts.clone(),
        microbatch_start,
    )?;

    // Receive data from the query stream, commiting a file on every watermark update received. The
    // `microbatch_max_interval` parameter controls the frequency of these updates.
    while let Some(message) = stream.next().await {
        let message = message?;
        match message {
            QueryMessage::MicrobatchStart(_) => (),
            QueryMessage::Data(batch) => {
                writer.write(&batch).await?;
            }
            QueryMessage::MicrobatchEnd(range) => {
                let microbatch_end = range.end();
                // Close current file and commit metadata
                let (parquet_meta, object_meta, footer) = writer.close(range).await?;
                commit_metadata(
                    &dataset_store.metadata_db,
                    parquet_meta,
                    object_meta,
                    physical_table.location_id(),
                    footer,
                )
                .await?;

                // Open new file for next chunk
                microbatch_start = microbatch_end + 1;
                writer = ParquetFileWriter::new(
                    physical_table.clone(),
                    parquet_opts.clone(),
                    microbatch_start,
                )?;
            }
        }
    }
    assert!(microbatch_start == range.end() + 1);

    Ok(())
}
