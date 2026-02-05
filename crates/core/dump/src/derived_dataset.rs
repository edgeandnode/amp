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
use amp_dataset_store::ResolveRevisionError;
use common::{
    BlockNum, DetachedLogicalPlan, PlanningContext, QueryContext, ResumeWatermark,
    catalog::{
        logical::for_dump as logical_catalog,
        physical::{
            CanonicalChainError, Catalog, EarliestBlockError, PhysicalTable,
            for_dump as physical_catalog,
        },
    },
    metadata::Generation,
    parquet::errors::ParquetError,
    query_context::QueryEnv,
    sql::{
        ParseSqlError, ResolveFunctionReferencesError, ResolveTableReferencesError,
        resolve_function_references, resolve_table_references,
    },
};
use datasets_common::hash_reference::HashReference;
use datasets_derived::{
    Manifest as DerivedManifest,
    deps::{DepAlias, DepAliasError, DepAliasOrSelfRef, DepAliasOrSelfRefError},
    manifest::TableInput,
};
use futures::StreamExt as _;
use metadata_db::NotificationMultiplexerHandle;
use tracing::{Instrument, instrument};

use crate::{
    Ctx, EndBlock, ResolvedEndBlock, WriterProperties,
    block_ranges::{GetLatestBlockError, ResolutionError},
    check::consistency_check,
    compaction::{AmpCompactor, AmpCompactorTaskError},
    metrics,
    parquet_writer::{
        CommitMetadataError, ParquetFileWriter, ParquetFileWriterCloseError,
        ParquetFileWriterOutput, commit_metadata,
    },
    streaming_query::{
        QueryMessage, StreamingQuery, message_stream_with_block_complete::MessageStreamError,
    },
    tasks::{self, TryWaitAllError},
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
    for table_def in dataset.tables() {
        let sql_table_ref_schema = dataset.reference().to_string();

        // Try to get existing active physical table revision (handles retry case)
        let revision = match ctx
            .data_store
            .get_table_active_revision(dataset.reference(), table_def.name())
            .await
            .map_err(Error::GetActivePhysicalTable)?
        {
            // Reuse existing table (retry scenario)
            Some(revision) => revision,
            // Create new table (initial attempt)
            None => ctx
                .data_store
                .create_new_table_revision(dataset.reference(), table_def.name())
                .await
                .map_err(Error::RegisterNewPhysicalTable)?,
        };

        let physical_table = Arc::new(PhysicalTable::from_revision(
            ctx.data_store.clone(),
            dataset.reference().clone(),
            dataset.start_block(),
            table_def.clone(),
            revision,
            sql_table_ref_schema,
        ));

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
        let tables_revs = tables.iter().map(|(pt, _)| pt.revision());
        ctx.data_store
            .lock_revisions_for_writer(tables_revs, writer)
            .await
            .map_err(Error::LockRevisionsForWriter)?;
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
    let mut join_set = tasks::FailFastJoinSet::<Result<(), DumpTableError>>::new();

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
                .await?;

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
        .map_err(Error::ParallelTasksFailed)?;

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
    /// This error occurs when querying for an active physical table revision fails.
    /// This typically happens due to database connection issues.
    #[error("Failed to get active physical table revision")]
    GetActivePhysicalTable(#[source] amp_data_store::GetTableActiveRevisionError),

    /// Failed to register physical table revision
    ///
    /// This error occurs when creating a new physical table revision fails,
    /// typically due to storage configuration issues, database connection problems,
    /// or invalid URL construction.
    #[error("Failed to create new physical table")]
    RegisterNewPhysicalTable(#[source] amp_data_store::CreateNewTableRevisionError),

    /// Failed to lock revisions for writer
    ///
    /// This error occurs when assigning the job as the writer for physical
    /// table locations fails, typically due to database connection issues.
    #[error("Failed to lock revisions for writer")]
    LockRevisionsForWriter(#[source] amp_data_store::LockRevisionsForWriterError),

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
        source: DumpTableError,
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
    ParallelTasksFailed(#[source] TryWaitAllError<DumpTableError>),

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
) -> Result<(), DumpTableError> {
    let dump_start_time = Instant::now();

    let table_name = table.table_name().to_string();

    // Clone values needed for metrics after async block
    let table_name_for_metrics = table_name.clone();
    let metrics_for_after = metrics.clone();

    // Get the table definition from the manifest
    let table_def = manifest
        .tables
        .get(table.table_name())
        .ok_or_else(|| DumpTableError::TableNotFound(table_name.clone()))?;

    // Extract SQL query from the table input
    let query_sql = match &table_def.input {
        TableInput::View(view) => &view.sql,
    };

    // Parse the SQL query
    let query = common::sql::parse(query_sql).map_err(DumpTableError::ParseSql)?;

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
            .map_err(DumpTableError::ResolveRevision)?
            .ok_or_else(|| DumpTableError::DependencyNotFound {
                alias: alias.to_string(),
                reference: reference.to_string(),
            })?;

        dependencies.insert(alias.clone(), hash_reference);
    }

    let mut join_set = tasks::FailFastJoinSet::<Result<(), DumpTableSpawnError>>::new();

    let catalog = {
        let table_refs = resolve_table_references::<DepAlias>(&query)
            .map_err(DumpTableError::ResolveTableReferences)?;
        let func_refs = resolve_function_references::<DepAliasOrSelfRef>(&query)
            .map_err(DumpTableError::ResolveFunctionReferences)?;
        let logical = logical_catalog::create(
            &ctx.dataset_store,
            &env.isolate_pool,
            &dependencies,
            &manifest.functions,
            (table_refs, func_refs),
        )
        .await
        .map_err(DumpTableError::CreateCatalog)?;
        physical_catalog::create(&ctx.dataset_store, &ctx.data_store, logical)
            .await
            .map_err(DumpTableError::CreatePhysicalCatalog)?
    };
    let planning_ctx = PlanningContext::new(catalog.logical().clone());
    let manifest_start_block = manifest.start_block;

    join_set.spawn(
        async move {
            let plan = planning_ctx
                .plan_sql(query.clone())
                .await
                .map_err(DumpTableSpawnError::PlanSql)?;
            if let Err(err) = plan.is_incremental() {
                return Err(DumpTableSpawnError::NonIncrementalQuery {
                    table_name,
                    source: err,
                });
            }

            let Some(dependency_earliest_block) = catalog
                .earliest_block()
                .await
                .map_err(DumpTableSpawnError::EarliestBlock)?
            else {
                // If the dependencies have synced nothing, we have nothing to do.
                tracing::warn!("no blocks to dump for {table_name}, dependencies are empty");
                return Ok::<(), DumpTableSpawnError>(());
            };

            // If derived dataset has a start_block, validate it
            if let Some(dataset_start_block) = &manifest_start_block
                && dataset_start_block < &dependency_earliest_block
            {
                return Err(DumpTableSpawnError::StartBlockBeforeDependencies {
                    dataset_start_block: *dataset_start_block,
                    dependency_earliest_block,
                });
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
                    let max_end_blocks = query_ctx
                        .max_end_blocks(&plan.clone().attach_to(&query_ctx)?)
                        .await?;
                    // For now, all materialized tables only support single networks.
                    assert_eq!(max_end_blocks.len(), 1);
                    Ok::<Option<BlockNum>, GetLatestBlockError>(max_end_blocks.into_values().next())
                })
                .await
                .map_err(DumpTableSpawnError::ResolveEndBlock)?;

            let end = match resolved {
                ResolvedEndBlock::Continuous => None,
                ResolvedEndBlock::NoDataAvailable => {
                    tracing::warn!("no blocks to dump for {table_name}, dependencies are empty");
                    return Ok::<(), DumpTableSpawnError>(());
                }
                ResolvedEndBlock::Block(block) => Some(block),
            };

            let latest_range = table
                .canonical_chain()
                .await
                .map_err(DumpTableSpawnError::CanonicalChain)?
                .map(|c| c.last().clone());
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
            .await
            .map_err(DumpTableSpawnError::DumpSqlQuery)?;

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

        return Err(DumpTableError::ParallelTaskFailed(err));
    }

    // Record dump duration on successful completion
    if let Some(ref metrics) = metrics_for_after {
        let duration_millis = dump_start_time.elapsed().as_millis() as f64;
        let job_id = table_name_for_metrics.clone();
        metrics.record_dump_duration(duration_millis, table_name_for_metrics.to_string(), job_id);
    }

    Ok(())
}

/// Errors that occur when dumping a derived dataset table
///
/// This error type is used by `dump_table()`.
#[derive(Debug, thiserror::Error)]
pub enum DumpTableError {
    /// Table definition not found in the dataset manifest
    ///
    /// This occurs when the requested table name does not exist in the
    /// manifest's table definitions.
    #[error("table `{0}` not found in dataset")]
    TableNotFound(String),

    /// Failed to parse the SQL query from the table definition
    ///
    /// This occurs when the SQL string in the table's input definition
    /// cannot be parsed as valid SQL.
    #[error("failed to parse SQL query: {0}")]
    ParseSql(#[source] ParseSqlError),

    /// Failed to resolve a dataset revision reference
    ///
    /// This occurs when looking up a dependency's revision (tag, hash, or version)
    /// in the dataset store fails.
    #[error("failed to resolve revision: {0}")]
    ResolveRevision(#[source] ResolveRevisionError),

    /// A dependency dataset was not found
    ///
    /// This occurs when a dependency alias in the manifest references a dataset
    /// that does not exist or cannot be resolved.
    #[error("failed to resolve dependency '{alias}' ({reference})")]
    DependencyNotFound { alias: String, reference: String },

    /// Failed to resolve table references from the SQL query
    ///
    /// This occurs when extracting and resolving table references from the
    /// parsed SQL query fails.
    #[error("failed to resolve table references: {0}")]
    ResolveTableReferences(#[source] ResolveTableReferencesError<DepAliasError>),

    /// Failed to resolve function references from the SQL query
    ///
    /// This occurs when extracting and resolving function references from the
    /// parsed SQL query fails.
    #[error("failed to resolve function references: {0}")]
    ResolveFunctionReferences(#[source] ResolveFunctionReferencesError<DepAliasOrSelfRefError>),

    /// Failed to create the logical catalog for query execution
    ///
    /// This occurs when building the logical catalog from the resolved
    /// table and function references fails.
    #[error("failed to create catalog: {0}")]
    CreateCatalog(#[source] logical_catalog::CreateCatalogError),

    /// Failed to create the physical catalog for query execution
    ///
    /// This occurs when building the physical catalog from the logical
    /// catalog fails.
    #[error("failed to create physical catalog: {0}")]
    CreatePhysicalCatalog(#[source] physical_catalog::CreateCatalogError),

    /// A parallel dump task failed during execution
    ///
    /// This occurs when one of the parallel tasks spawned for dumping table data
    /// fails or panics. The fail-fast behavior ensures all other tasks are aborted
    /// when this happens.
    #[error("parallel dump task failed: {0}")]
    ParallelTaskFailed(#[source] TryWaitAllError<DumpTableSpawnError>),
}

/// Errors that occur in the spawned async task for table dumping
///
/// This error type is used by the async task spawned in `dump_table()`.
#[derive(Debug, thiserror::Error)]
pub enum DumpTableSpawnError {
    /// Failed to plan the SQL query for execution
    ///
    /// This occurs when DataFusion cannot create an execution plan
    /// from the parsed SQL query.
    #[error("failed to plan SQL query: {0}")]
    PlanSql(#[source] common::query_context::Error),

    /// The query is not incremental and cannot be synced
    ///
    /// This occurs when the SQL query cannot be incrementalized,
    /// which is required for derived dataset syncing.
    #[error("syncing table {table_name} is not supported: {source}")]
    NonIncrementalQuery {
        table_name: String,
        #[source]
        source: common::incrementalizer::NonIncrementalQueryError,
    },

    /// Failed to determine the earliest block from dependencies
    ///
    /// This occurs when querying the catalog for the earliest available
    /// block across all dependency datasets fails.
    #[error("failed to get earliest block: {0}")]
    EarliestBlock(#[source] EarliestBlockError),

    /// Dataset start block is before the earliest dependency block
    ///
    /// This occurs when the manifest specifies a start block that is
    /// earlier than the earliest available data in the dependencies.
    #[error(
        "earliest block is before start block: {dataset_start_block} < {dependency_earliest_block}"
    )]
    StartBlockBeforeDependencies {
        dataset_start_block: BlockNum,
        dependency_earliest_block: BlockNum,
    },

    /// Failed to resolve the end block for dumping
    ///
    /// This occurs when determining the target end block for the
    /// dump operation fails.
    #[error("failed to resolve end block: {0}")]
    ResolveEndBlock(#[source] ResolutionError),

    /// Failed to get the canonical chain from the physical table
    ///
    /// This occurs when querying the table's canonical chain for
    /// resume watermark calculation fails.
    #[error("failed to get canonical chain: {0}")]
    CanonicalChain(#[source] CanonicalChainError),

    /// Failed to execute the SQL query dump operation
    ///
    /// This occurs when the inner `dump_sql_query` function fails.
    #[error("failed to dump SQL query: {0}")]
    DumpSqlQuery(#[source] DumpSqlQueryError),
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
) -> Result<(), DumpSqlQueryError> {
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
        .await
        .map_err(DumpSqlQueryError::StreamingQuerySpawn)?
        .into_stream()
    };

    let mut microbatch_start = start;
    let mut filename = FileName::new_with_random_suffix(microbatch_start);
    let mut buf_writer = ctx
        .data_store
        .create_revision_file_writer(physical_table.revision(), &filename);
    let mut writer = ParquetFileWriter::new(
        ctx.data_store.clone(),
        buf_writer,
        filename,
        physical_table.clone(),
        opts.max_row_group_bytes,
        opts.parquet.clone(),
    )
    .map_err(DumpSqlQueryError::CreateParquetFileWriter)?;

    let table_name = physical_table.table_name();
    let location_id = *physical_table.location_id();

    // Receive data from the query stream, commiting a file on every watermark update received. The
    // `microbatch_max_interval` parameter controls the frequency of these updates.
    while let Some(message) = stream.next().await {
        let message = message.map_err(DumpSqlQueryError::StreamingQuery)?;
        match message {
            QueryMessage::MicrobatchStart {
                range: _,
                is_reorg: _,
            } => (),
            QueryMessage::Data(batch) => {
                writer
                    .write(&batch)
                    .await
                    .map_err(DumpSqlQueryError::WriteBatch)?;

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
                } = writer
                    .close(range, vec![], Generation::default())
                    .await
                    .map_err(DumpSqlQueryError::CloseFile)?;

                commit_metadata(
                    &ctx.metadata_db,
                    parquet_meta,
                    object_meta,
                    physical_table.location_id(),
                    &url,
                    footer,
                )
                .await
                .map_err(DumpSqlQueryError::CommitMetadata)?;

                compactor.try_run().map_err(DumpSqlQueryError::Compactor)?;

                // Open new file for next chunk
                microbatch_start = microbatch_end + 1;
                filename = FileName::new_with_random_suffix(microbatch_start);
                buf_writer = ctx
                    .data_store
                    .create_revision_file_writer(physical_table.revision(), &filename);
                writer = ParquetFileWriter::new(
                    ctx.data_store.clone(),
                    buf_writer,
                    filename,
                    physical_table.clone(),
                    opts.max_row_group_bytes,
                    opts.parquet.clone(),
                )
                .map_err(DumpSqlQueryError::CreateParquetFileWriter)?;

                if let Some(ref metrics) = metrics {
                    metrics.record_file_written(table_name.to_string(), location_id);
                }
            }
        }
    }

    Ok(())
}

/// Errors that occur when executing a SQL query dump operation
///
/// This error type is used by `dump_sql_query()`.
#[derive(Debug, thiserror::Error)]
pub enum DumpSqlQueryError {
    /// Failed to spawn the streaming query execution
    ///
    /// This occurs when initializing the streaming query executor fails.
    #[error("failed to spawn streaming query: {0}")]
    StreamingQuerySpawn(#[source] crate::streaming_query::SpawnError),

    /// Failed to create the parquet file writer
    ///
    /// This occurs when initializing the parquet writer for output files fails.
    #[error("failed to create parquet file writer: {0}")]
    CreateParquetFileWriter(#[source] ParquetError),

    /// Failed to write a record batch to the parquet file
    ///
    /// This occurs when writing query result batches to the parquet file fails.
    #[error("failed to write batch: {0}")]
    WriteBatch(#[source] ParquetError),

    /// Failed to close and finalize the parquet file
    ///
    /// This occurs when closing the parquet writer and finalizing the file fails.
    #[error("failed to close file: {0}")]
    CloseFile(#[source] ParquetFileWriterCloseError),

    /// Failed to commit file metadata to the database
    ///
    /// This occurs when registering the written parquet file's metadata
    /// in the metadata database fails.
    #[error("failed to commit metadata: {0}")]
    CommitMetadata(#[source] CommitMetadataError),

    /// Failed to run the compactor after writing a file
    ///
    /// This occurs when the compactor task fails after a successful file write.
    #[error("failed to run compactor: {0}")]
    Compactor(#[source] AmpCompactorTaskError),

    /// Failed to receive the next message from the streaming query
    ///
    /// This occurs when the streaming query execution encounters an error
    /// while producing result batches.
    #[error("failed to get next message: {0}")]
    StreamingQuery(#[source] MessageStreamError),
}
