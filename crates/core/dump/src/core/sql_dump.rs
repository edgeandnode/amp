//! # SQL dump implementation
//!
//! This module implements the core logic for dumping SQL-based datasets to Parquet files.
//! Unlike raw dataset dumps that extract blockchain data directly, SQL dataset dumps execute
//! user-defined SQL queries against existing datasets to create derived or transformed datasets.
//!
//! ## Overview
//!
//! Derived datasets allow users to define custom transformations and aggregations over blockchain
//! data using SQL queries. Each derived dataset consists of one or more tables, where each table
//! is defined by a SQL query that can reference other datasets as data sources. The dump
//! process executes these queries and materializes the results as Parquet files.
//!
//! ## Table Types
//!
//! Tables can be either incremental or non-incremental:
//!
//! - **Incremental Tables**: Process data in block ranges and can be updated incrementally
//!   as new blocks become available. These tables maintain state about which block ranges
//!   have been processed and only compute results for new or missing ranges.
//!
//! - **Non-Incremental Tables**: Recompute the entire table on each dump operation.
//!   These are typically used for queries that require global aggregations or joins.
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
//! For incremental tables, the dump process implements sophisticated block range management:
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
//! - **Parallel Task Management**: Uses a specialized join set (`FailFastJoinSet`)
//!   to coordinate concurrent table processing tasks, ensuring that if any table processing
//!   fails, all remaining tasks are immediately terminated to prevent partial dumps.
//!
//! - **Metadata Consistency**: Commits metadata updates only after successful
//!   completion of each batch, maintaining consistency between data files and
//!   processing state.

use std::{sync::Arc, time::Instant};

use common::{
    BlockNum, BoxError, DetachedLogicalPlan, PlanningContext, QueryContext,
    catalog::physical::{Catalog, PhysicalTable},
    metadata::{Generation, segments::ResumeWatermark},
    plan_visitors::IncrementalCheck,
    query_context::{QueryEnv, parse_sql},
};
use datasets_derived::{Manifest as DerivedManifest, manifest::TableInput};
use futures::StreamExt as _;
use metadata_db::NotificationMultiplexerHandle;
use tracing::instrument;

use super::{Ctx, EndBlock, ResolvedEndBlock, tasks::FailFastJoinSet};
use crate::{
    WriterProperties,
    compaction::AmpCompactor,
    metrics,
    parquet_writer::{ParquetFileWriter, ParquetFileWriterOutput, commit_metadata},
    streaming_query::{QueryMessage, StreamingQuery},
};

/// Dumps a derived dataset table
#[instrument(skip_all, fields(dataset = %manifest.name), err)]
#[allow(clippy::too_many_arguments)]
pub async fn dump_table(
    ctx: Ctx,
    manifest: DerivedManifest,
    env: &QueryEnv,
    table: Arc<PhysicalTable>,
    opts: &Arc<WriterProperties>,
    microbatch_max_interval: u64,
    end: EndBlock,
    metrics: Option<Arc<metrics::MetricsRegistry>>,
) -> Result<(), BoxError> {
    let dump_start_time = Instant::now();

    let dataset_name = manifest.name.clone();
    let table_name = table.table_name().to_string();

    // Clone values needed for metrics after async block
    let dataset_name_for_metrics = dataset_name.clone();
    let table_name_for_metrics = table_name.clone();
    let dataset_version = table.dataset().dataset_version().unwrap_or_default();
    let metrics_for_after = metrics.clone();

    // Get the table definition from the manifest
    let table_def = manifest.tables.get(&table_name).ok_or_else(|| {
        format!(
            "table `{}` not found in dataset `{}`",
            table_name, dataset_name
        )
    })?;

    // Extract SQL query from the table input
    let query_sql = match &table_def.input {
        TableInput::View(view) => &view.sql,
    };

    // Parse the SQL query
    let query = parse_sql(query_sql)?;

    let mut join_set = FailFastJoinSet::<Result<(), BoxError>>::new();
    let dataset_store = ctx.dataset_store.clone();
    let env = env.clone();
    let opts = opts.clone();

    join_set.spawn(async move {
        let catalog = dataset_store
            .clone()
            .catalog_for_sql(&query, env.clone())
            .await?;
        let planning_ctx = PlanningContext::new(catalog.logical().clone());

        let plan = planning_ctx.plan_sql(query.clone()).await?;
        let incremental_check = plan.is_incremental()?;

        let Some(start) = catalog.earliest_block().await? else {
            // If the dependencies have synced nothing, we have nothing to do.
            tracing::warn!("no blocks to dump for {table_name}, dependencies are empty");
            return Ok::<(), BoxError>(());
        };

        let resolved = end
            .resolve(start, async {
                let query_ctx =
                    QueryContext::for_catalog(catalog.clone(), env.clone(), false).await?;
                query_ctx
                    .max_end_block(&plan.clone().attach_to(&query_ctx)?)
                    .await
            })
            .await?;

        let end = match resolved {
            ResolvedEndBlock::Continuous => None,
            ResolvedEndBlock::NoDataAvailable => {
                tracing::warn!("no blocks to dump for {table_name}, dependencies are empty");
                return Ok::<(), BoxError>(());
            }
            ResolvedEndBlock::Block(block) => Some(block),
        };

        if let IncrementalCheck::NonIncremental(op) = incremental_check {
            return Err(format!(
                "syncing non-incremental table is not supported: {}.{} (contains non-incremental operation: {})",
                table_name, dataset_name, op
            )
            .into());
        }

        let latest_range = table.canonical_chain().await?.map(|c| c.last().clone());
        let resume_watermark = latest_range.map(|r| ResumeWatermark::from_ranges(vec![r]));
        dump_sql_query(
            &ctx,
            &env,
            &catalog,
            plan.clone(),
            start,
            end,
            resume_watermark,
            table.clone(),
            &opts,
            microbatch_max_interval,
            &ctx.notification_multiplexer,
            metrics.clone(),
        )
        .await?;

        Ok(())
    });

    // Wait for all the jobs to finish, returning an error if any job panics or fails
    if let Err(err) = join_set.try_wait_all().await {
        tracing::error!(dataset=%manifest.name, error=%err, "dataset dump failed");

        // Record error metrics
        if let Some(ref metrics) = metrics_for_after {
            metrics.record_dump_error(
                dataset_name_for_metrics.to_string(),
                dataset_version,
                table_name_for_metrics.to_string(),
            );
        }

        return Err(err.into_box_error());
    }

    // Record dump duration on successful completion
    if let Some(ref metrics) = metrics_for_after {
        let duration_millis = dump_start_time.elapsed().as_millis() as f64;
        let job_id = format!("{}_{}", dataset_name_for_metrics, table_name_for_metrics);
        metrics.record_dump_duration(
            duration_millis,
            dataset_name_for_metrics.to_string(),
            dataset_version,
            table_name_for_metrics.to_string(),
            job_id,
        );
    }

    Ok(())
}

#[instrument(skip_all, err)]
#[allow(clippy::too_many_arguments)]
async fn dump_sql_query(
    ctx: &Ctx,
    env: &QueryEnv,
    catalog: &Catalog,
    query: DetachedLogicalPlan,
    start: BlockNum,
    end: Option<BlockNum>,
    resume_watermark: Option<ResumeWatermark>,
    physical_table: Arc<PhysicalTable>,
    opts: &Arc<WriterProperties>,
    microbatch_max_interval: u64,
    notification_multiplexer: &Arc<NotificationMultiplexerHandle>,
    metrics: Option<Arc<metrics::MetricsRegistry>>,
) -> Result<(), BoxError> {
    tracing::info!(
        "dumping {} [{}-{}]",
        physical_table.table_ref(),
        start,
        end.map(|e| e.to_string()).unwrap_or_default(),
    );
    let mut stream = {
        StreamingQuery::spawn(
            env.clone(),
            catalog.clone(),
            ctx.dataset_store.clone(),
            query,
            start,
            end,
            resume_watermark,
            notification_multiplexer,
            Some(physical_table.clone()),
            microbatch_max_interval,
        )
        .await?
        .as_stream()
    };

    let mut microbatch_start = start;
    let mut writer = ParquetFileWriter::new(physical_table.clone(), opts, microbatch_start)?;

    let dataset_name = physical_table.dataset().name.clone();
    let table_name = physical_table.table_name();
    let location_id = *physical_table.location_id();

    let mut compactor = AmpCompactor::start(
        physical_table.clone(),
        env.parquet_footer_cache.clone(),
        opts.clone(),
        metrics.clone(),
    );

    // Receive data from the query stream, commiting a file on every watermark update received. The
    // `microbatch_max_interval` parameter controls the frequency of these updates.
    while let Some(message) = stream.next().await {
        let message = message?;
        match message {
            QueryMessage::MicrobatchStart {
                range: _,
                is_reorg: _,
            } => (),
            QueryMessage::Data(batch) => {
                writer.write(&batch).await?;

                if let Some(ref metrics) = metrics {
                    let num_rows: u64 = batch.num_rows().try_into().unwrap();
                    let num_bytes: u64 = batch.get_array_memory_size().try_into().unwrap();
                    let dataset_version = physical_table
                        .dataset()
                        .dataset_version()
                        .unwrap_or_default();
                    metrics.record_ingestion_rows(
                        num_rows,
                        dataset_name.to_string(),
                        dataset_version.clone(),
                        table_name.to_string(),
                        location_id,
                    );
                    metrics.record_ingestion_bytes(
                        num_bytes,
                        dataset_name.to_string(),
                        dataset_version,
                        table_name.to_string(),
                        location_id,
                    );
                }
            }
            QueryMessage::BlockComplete(_) => {
                // TODO: Check if file should be closed early
            }
            QueryMessage::MicrobatchEnd(range) => {
                let microbatch_end = range.end();
                // Close current file and commit metadata
                let ParquetFileWriterOutput {
                    parquet_meta,
                    object_meta,
                    footer,
                    ..
                } = writer.close(range, vec![], Generation::default()).await?;

                commit_metadata(
                    &ctx.metadata_db,
                    parquet_meta,
                    object_meta,
                    physical_table.location_id(),
                    footer,
                )
                .await?;

                compactor.try_run();

                // Open new file for next chunk
                microbatch_start = microbatch_end + 1;
                writer = ParquetFileWriter::new(physical_table.clone(), opts, microbatch_start)?;

                if let Some(ref metrics) = metrics {
                    let dataset_version = physical_table
                        .dataset()
                        .dataset_version()
                        .unwrap_or_default();
                    metrics.record_file_written(
                        dataset_name.to_string(),
                        dataset_version,
                        table_name.to_string(),
                        location_id,
                    );
                }
            }
        }
    }

    Ok(())
}
