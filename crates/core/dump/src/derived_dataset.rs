//! # Derived dataset dump implementation
//!
//! This module implements the core logic for dumping derived datasets to Parquet files.
//! Unlike raw dataset dumps that extract blockchain data directly, derived dataset dumps execute
//! SQL queries against existing datasets to create transformed datasets.
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
//! The derived dataset dump process follows these main steps:
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
//! The derived dataset dump process includes several reliability features:
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

use std::{collections::BTreeMap, sync::Arc, time::Instant};

use amp_data_store::file_name::FileName;
use common::{
    BlockNum, BoxError, DetachedLogicalPlan, PlanningContext, QueryContext,
    catalog::physical::{Catalog, PhysicalTable},
    metadata::{Generation, segments::ResumeWatermark},
    query_context::QueryEnv,
};
use datasets_common::{deps::alias::DepAlias, hash_reference::HashReference};
use datasets_derived::{
    Manifest as DerivedManifest, catalog::catalog_for_sql_with_deps, manifest::TableInput,
};
use futures::StreamExt as _;
use metadata_db::NotificationMultiplexerHandle;
use tracing::{Instrument, instrument};

use crate::{
    Ctx, EndBlock, ResolvedEndBlock, WriterProperties,
    check::consistency_check,
    compaction::AmpCompactor,
    metrics,
    parquet_writer::{ParquetFileWriter, ParquetFileWriterOutput, commit_metadata},
    streaming_query::{QueryMessage, StreamingQuery},
    tasks,
};

/// Dumps a set of derived dataset tables. All tables must belong to the same dataset.
pub async fn dump(
    ctx: Ctx,
    dataset_ref: &HashReference,
    microbatch_max_interval: u64,
    end: EndBlock,
    writer: impl Into<Option<metadata_db::JobId>>,
) -> Result<(), Error> {
    let writer = writer.into();

    // Resolve manifest once using the provided hash reference
    let manifest = ctx
        .dataset_store
        .get_derived_manifest(dataset_ref.hash())
        .await
        .map(Arc::new)
        .map_err(Error::GetDerivedManifest)?;

    let parquet_opts = crate::parquet_opts(&ctx.config.parquet);

    // Get dataset for table resolution
    let dataset = ctx
        .dataset_store
        .get_dataset(dataset_ref)
        .await
        .map_err(Error::GetDataset)?;

    // Initialize physical tables and compactors
    let mut tables: Vec<(Arc<PhysicalTable>, Arc<AmpCompactor>)> = vec![];
    for table in dataset.resolved_tables(dataset_ref.to_reference().into()) {
        // Try to get existing active physical table (handles retry case)
        let physical_table: Arc<PhysicalTable> =
            match PhysicalTable::get_active(ctx.data_store.clone(), table.clone())
                .await
                .map_err(Error::GetActivePhysicalTable)?
            {
                // Reuse existing table (retry scenario)
                Some(pt) => pt,
                // Create new table (initial attempt)
                None => common::catalog::physical::register_new_table_revision(
                    ctx.data_store.clone(),
                    dataset_ref.clone(),
                    table,
                )
                .await
                .map_err(Error::RegisterNewPhysicalTable)?,
            }
            .into();

        let compactor = AmpCompactor::start(
            ctx.metadata_db.clone(),
            ctx.data_store.clone(),
            parquet_opts.clone(),
            physical_table.clone(),
            ctx.metrics.clone(),
        )
        .into();

        tables.push((physical_table, compactor));
    }

    if tables.is_empty() {
        return Ok(());
    }

    // Assign job writer if provided (locks tables to job)
    if let Some(writer) = writer {
        let location_ids: Vec<_> = tables.iter().map(|(pt, _)| pt.location_id()).collect();
        metadata_db::physical_table::assign_job_writer(&ctx.metadata_db, &location_ids, writer)
            .await
            .map_err(Error::AssignJobWriter)?;
    }

    // Pre-check all tables for consistency before spawning tasks
    for (table, _) in &tables {
        consistency_check(table, &ctx.data_store)
            .await
            .map_err(|err| Error::ConsistencyCheck {
                table_name: table.table_name().to_string(),
                source: err,
            })?;
    }

    // Process all tables in parallel using FailFastJoinSet
    let mut join_set = tasks::FailFastJoinSet::<Result<(), BoxError>>::new();

    let env = ctx.config.make_query_env().map_err(Error::CreateQueryEnv)?;
    for (table, compactor) in &tables {
        let ctx = ctx.clone();
        let env = env.clone();
        let table = Arc::clone(table);
        let compactor = Arc::clone(compactor);
        let opts = parquet_opts.clone();
        let metrics = ctx.metrics.clone();
        let manifest = manifest.clone();

        join_set.spawn(
            async move {
                let table_name = table.table_name().to_string();

                dump_table(
                    ctx,
                    &manifest,
                    env.clone(),
                    table.clone(),
                    compactor,
                    opts.clone(),
                    microbatch_max_interval,
                    end,
                    metrics,
                )
                .await
                .map_err(|err| -> BoxError {
                    Error::DumpTable {
                        table_name: table_name.clone(),
                        source: err,
                    }
                    .into()
                })?;

                tracing::info!("dump of `{}` completed successfully", table_name);
                Ok(())
            }
            .in_current_span(),
        );
    }

    // Wait for all tables to complete with fail-fast behavior
    join_set
        .try_wait_all()
        .await
        .map_err(|err| Error::ParallelTasksFailed(err.into_box_error()))?;

    Ok(())
}

/// Errors that occur during derived dataset dump operations
///
/// This error type is used by the `dump()` function to report issues encountered
/// when dumping derived datasets to Parquet files.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to get dataset from dataset store
    ///
    /// This occurs when retrieving the dataset instance from the dataset store fails.
    /// The dataset store loads dataset manifests and parses them into Dataset instances.
    ///
    /// Common causes:
    /// - Dataset not found in metadata database
    /// - Manifest file not accessible in object store
    /// - Invalid or corrupted manifest content
    /// - Manifest parsing errors
    /// - Missing required manifest fields
    #[error("Failed to get dataset")]
    GetDataset(#[source] amp_dataset_store::GetDatasetError),

    /// Failed to get active physical table
    ///
    /// This error occurs when querying for an active physical table fails.
    /// This typically happens due to database connection issues.
    ///
    /// Note: This wraps BoxError because PhysicalTable::get_active currently
    /// returns BoxError. This should be replaced with a concrete error type.
    #[error("Failed to get active physical table")]
    GetActivePhysicalTable(#[source] BoxError),

    /// Failed to register physical table revision
    ///
    /// This error occurs when registering a new physical table revision fails,
    /// typically due to storage configuration issues, database connection problems,
    /// or invalid URL construction.
    #[error("Failed to register new physical table")]
    RegisterNewPhysicalTable(#[source] common::catalog::physical::RegisterNewTableRevisionError),

    /// Failed to assign job writer
    ///
    /// This error occurs when assigning the job as the writer for physical
    /// table locations fails, typically due to database connection issues.
    #[error("Failed to assign job writer")]
    AssignJobWriter(#[source] metadata_db::Error),

    /// Failed to create query environment from configuration
    ///
    /// This occurs when the query environment cannot be initialized, typically due to:
    /// - Invalid configuration parameters
    /// - Missing required environment settings
    /// - Resource allocation failures
    ///
    /// The dump operation cannot proceed without a valid query environment.
    #[error("Failed to create query environment")]
    CreateQueryEnv(#[source] datafusion::error::DataFusionError),

    /// Failed consistency check for table
    ///
    /// This occurs when the consistency check detects issues between metadata database
    /// and object store for a table. Common causes:
    /// - Missing registered files (data corruption)
    /// - Object store connectivity issues
    /// - Metadata database query failures
    ///
    /// The table must pass consistency checks before dump can proceed.
    #[error("Consistency check failed for table '{table_name}'")]
    ConsistencyCheck {
        table_name: String,
        #[source]
        source: crate::check::ConsistencyError,
    },

    /// Failed to retrieve derived dataset manifest
    ///
    /// This occurs when the manifest for a derived dataset cannot be fetched from
    /// the dataset store. Common causes:
    /// - Manifest hash not found in store
    /// - Object store connectivity issues
    /// - Corrupted manifest data
    ///
    /// The manifest is required to determine table definitions and SQL queries.
    #[error("Failed to get derived manifest")]
    GetDerivedManifest(#[source] amp_dataset_store::GetDerivedManifestError),

    /// Failed to dump individual table
    ///
    /// This occurs when the table-level dump operation fails. This wraps errors
    /// from the `dump_table()` function which handles SQL execution, query planning,
    /// and Parquet file writing.
    ///
    /// Common causes:
    /// - SQL query execution failures
    /// - Dependency resolution errors
    /// - Parquet file writing errors
    /// - Metadata database update failures
    #[error("Failed to dump table '{table_name}'")]
    DumpTable {
        table_name: String,
        #[source]
        source: BoxError,
    },

    /// Parallel task execution failed
    ///
    /// This occurs when one or more parallel table dump tasks fail or panic.
    /// The fail-fast join set ensures that if any task fails, all remaining
    /// tasks are immediately terminated to prevent partial dumps.
    ///
    /// When this error occurs, the entire dump operation is aborted and no
    /// partial results are committed. The operation is safe to retry from
    /// the beginning.
    #[error("Parallel table dump tasks failed")]
    ParallelTasksFailed(#[source] BoxError),

    /// Start block before dependencies
    ///
    /// This occurs when the start block of the derived dataset is before the earliest block of the dependencies.
    #[error(
        "derived dataset start_block ({dataset_start_block}) is before dependency's earliest available block ({dependency_earliest_block})"
    )]
    StartBlockBeforeDependencies {
        dataset_start_block: BlockNum,
        dependency_earliest_block: BlockNum,
    },
}

/// Dumps a derived dataset table
#[instrument(skip_all, fields(table = %table.table_name()), err)]
#[expect(clippy::too_many_arguments)]
async fn dump_table(
    ctx: Ctx,
    manifest: &DerivedManifest,
    env: QueryEnv,
    table: Arc<PhysicalTable>,
    compactor: Arc<AmpCompactor>,
    opts: Arc<WriterProperties>,
    microbatch_max_interval: u64,
    end: EndBlock,
    metrics: Option<Arc<metrics::MetricsRegistry>>,
) -> Result<(), BoxError> {
    let dump_start_time = Instant::now();

    let table_name = table.table_name().to_string();

    // Clone values needed for metrics after async block
    let table_name_for_metrics = table_name.clone();
    let metrics_for_after = metrics.clone();

    // Get the table definition from the manifest
    let table_def = manifest
        .tables
        .get(table.table_name())
        .ok_or_else(|| format!("table `{}` not found in dataset", table_name))?;

    // Extract SQL query from the table input
    let query_sql = match &table_def.input {
        TableInput::View(view) => &view.sql,
    };

    // Parse the SQL query
    let query = common::sql::parse(query_sql)?;

    // Resolve all dependencies from the manifest to HashReference
    // This ensures SQL schema references map to the exact dataset versions
    // specified in the manifest's dependencies
    let mut dependencies: BTreeMap<DepAlias, HashReference> = BTreeMap::new();

    for (alias, dep_reference) in &manifest.dependencies {
        // Convert DepReference to Reference for resolution
        let reference = dep_reference.to_reference();

        // Resolve reference to its manifest hash
        let hash_reference = ctx
            .dataset_store
            .resolve_revision(&reference)
            .await
            .map_err(|err| {
                format!(
                    "Failed to resolve dependency '{}' (reference: {}): {}",
                    alias, reference, err
                )
            })?
            .ok_or_else(|| {
                format!(
                    "Dependency '{}' not found (reference: {})",
                    alias, reference
                )
            })?;

        dependencies.insert(alias.clone(), hash_reference);
    }

    let mut join_set = tasks::FailFastJoinSet::<Result<(), BoxError>>::new();

    let catalog = catalog_for_sql_with_deps(
        &ctx.dataset_store,
        &ctx.data_store,
        &query,
        &env,
        &dependencies,
        &manifest.functions,
    )
    .await?;
    let planning_ctx = PlanningContext::new(catalog.logical().clone());
    let manifest_start_block = manifest.start_block;

    join_set.spawn(
        async move {
            let plan = planning_ctx.plan_sql(query.clone()).await?;
            if let Err(err) = plan.is_incremental() {
                return Err(format!("syncing table {table_name} is not supported: {err}",).into());
            }

            let Some(dependency_earliest_block) = catalog.earliest_block().await? else {
                // If the dependencies have synced nothing, we have nothing to do.
                tracing::warn!("no blocks to dump for {table_name}, dependencies are empty");
                return Ok::<(), BoxError>(());
            };

            // If derived dataset has a start_block, validate it
            if let Some(dataset_start_block) = &manifest_start_block
                && dataset_start_block < &dependency_earliest_block
            {
                return Err(Error::StartBlockBeforeDependencies {
                    dataset_start_block: *dataset_start_block,
                    dependency_earliest_block,
                }
                .into());
            }
            let start = manifest_start_block.unwrap_or(dependency_earliest_block);

            let resolved = end
                .resolve(start, async {
                    let query_ctx = QueryContext::for_catalog(
                        catalog.clone(),
                        env.clone(),
                        ctx.data_store.clone(),
                        false,
                    )
                    .await?;
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

            let latest_range = table.canonical_chain().await?.map(|c| c.last().clone());
            let resume_watermark = latest_range.map(|r| ResumeWatermark::from_ranges(&[r]));
            dump_sql_query(
                &ctx,
                &env,
                &catalog,
                plan.clone(),
                start,
                end,
                resume_watermark,
                table.clone(),
                compactor,
                &opts,
                microbatch_max_interval,
                &ctx.notification_multiplexer,
                metrics.clone(),
            )
            .await?;

            Ok(())
        }
        .in_current_span(),
    );

    // Wait for all the jobs to finish, returning an error if any job panics or fails
    if let Err(err) = join_set.try_wait_all().await {
        tracing::error!(table=%table_name_for_metrics, error=%err, "dataset dump failed");

        // Record error metrics
        if let Some(ref metrics) = metrics_for_after {
            metrics.record_dump_error(table_name_for_metrics.to_string());
        }

        return Err(err.into_box_error());
    }

    // Record dump duration on successful completion
    if let Some(ref metrics) = metrics_for_after {
        let duration_millis = dump_start_time.elapsed().as_millis() as f64;
        let job_id = table_name_for_metrics.clone();
        metrics.record_dump_duration(duration_millis, table_name_for_metrics.to_string(), job_id);
    }

    Ok(())
}

#[instrument(skip_all, err)]
#[expect(clippy::too_many_arguments)]
async fn dump_sql_query(
    ctx: &Ctx,
    env: &QueryEnv,
    catalog: &Catalog,
    query: DetachedLogicalPlan,
    start: BlockNum,
    end: Option<BlockNum>,
    resume_watermark: Option<ResumeWatermark>,
    physical_table: Arc<PhysicalTable>,
    compactor: Arc<AmpCompactor>,
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
    let keep_alive_interval = ctx.config.keep_alive_interval;
    let mut stream = {
        StreamingQuery::spawn(
            env.clone(),
            catalog.clone(),
            &ctx.dataset_store,
            ctx.data_store.clone(),
            query,
            start,
            end,
            resume_watermark,
            notification_multiplexer,
            Some(physical_table.clone()),
            microbatch_max_interval,
            keep_alive_interval,
        )
        .await?
        .into_stream()
    };

    let mut microbatch_start = start;
    let mut filename = FileName::new_with_random_suffix(microbatch_start);
    let mut buf_writer = ctx
        .data_store
        .create_revision_file_writer(physical_table.path(), &filename);
    let mut writer = ParquetFileWriter::new(
        ctx.data_store.clone(),
        buf_writer,
        filename,
        physical_table.clone(),
        opts.max_row_group_bytes,
        opts.parquet.clone(),
    )?;

    let table_name = physical_table.table_name();
    let location_id = *physical_table.location_id();

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
                    metrics.record_ingestion_rows(num_rows, table_name.to_string(), location_id);
                    metrics.record_write_call(num_bytes, table_name.to_string(), location_id);
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
                    url,
                    ..
                } = writer.close(range, vec![], Generation::default()).await?;

                commit_metadata(
                    &ctx.metadata_db,
                    parquet_meta,
                    object_meta,
                    physical_table.location_id(),
                    &url,
                    footer,
                )
                .await?;

                compactor.try_run()?;

                // Open new file for next chunk
                microbatch_start = microbatch_end + 1;
                filename = FileName::new_with_random_suffix(microbatch_start);
                buf_writer = ctx
                    .data_store
                    .create_revision_file_writer(physical_table.path(), &filename);
                writer = ParquetFileWriter::new(
                    ctx.data_store.clone(),
                    buf_writer,
                    filename,
                    physical_table.clone(),
                    opts.max_row_group_bytes,
                    opts.parquet.clone(),
                )?;

                if let Some(ref metrics) = metrics {
                    metrics.record_file_written(table_name.to_string(), location_id);
                }
            }
        }
    }

    Ok(())
}
