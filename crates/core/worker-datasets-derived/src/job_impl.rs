//! # Derived dataset materialization implementation
//!
//! This module implements the core logic for materializing derived datasets to Parquet files.
//! Unlike raw dataset materializations that extract blockchain data directly, derived dataset
//! materializations execute SQL queries against existing datasets to create transformed datasets.
//!
//! ## Overview
//!
//! Derived datasets allow users to define custom transformations and aggregations over blockchain
//! data using SQL queries. Each derived dataset consists of one or more tables, where each table
//! is defined by a SQL query that can reference other datasets as data sources. The materialization
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
//! - **Non-Incremental Tables**: Recompute the entire table on each materialization operation.
//!   These are typically used for queries that require global aggregations or joins.
//!
//! ## Materialization Process
//!
//! The derived dataset materialization process follows these main steps:
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
//! For incremental tables, the materialization process implements sophisticated block range
//! management:
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
//! The derived dataset materialization process includes several reliability features:
//!
//! - **Dependency Validation**: Checks that all required source datasets have data
//!   available before attempting to execute queries, preventing unnecessary work.
//!
//! - **Atomic Batch Processing**: Each batch is processed atomically, ensuring that
//!   partial results are not recorded in case of failures.
//!
//! - **Parallel Task Management**: Uses a specialized join set (`FailFastJoinSet`)
//!   to coordinate concurrent table processing tasks, ensuring that if any table processing
//!   fails, all remaining tasks are immediately terminated to prevent partial materializations.
//!
//! - **Metadata Consistency**: Commits metadata updates only after successful
//!   completion of each batch, maintaining consistency between data files and
//!   processing state.

pub mod query;
pub mod table;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use amp_data_store::retryable::RetryableErrorExt as _;
use amp_worker_core::{
    check::consistency_check, compaction::AmpCompactor, error_detail::ErrorDetailsProvider,
    retryable::RetryableErrorExt, tasks::TryWaitAllError,
};
use common::{physical_table::PhysicalTable, retryable::RetryableErrorExt as _};
use datasets_common::{hash_reference::HashReference, table_name::TableName};
use tracing::Instrument;

use self::table::{MaterializeTableError, materialize_table};
use crate::{job_ctx::Context, job_descriptor::JobDescriptor};

/// Executes materialization of a set of derived dataset tables.
/// All tables must belong to the same dataset.
pub async fn execute(
    ctx: Context,
    desc: JobDescriptor,
    writer: impl Into<Option<metadata_db::jobs::JobId>>,
) -> Result<(), Error> {
    let dataset_ref = HashReference::new(
        desc.dataset_namespace.clone(),
        desc.dataset_name.clone(),
        desc.manifest_hash.clone(),
    );
    let end = desc.end_block;

    let writer = writer.into();

    // Resolve manifest once using the provided hash reference
    let manifest = ctx
        .datasets_cache
        .get_derived_manifest(dataset_ref.hash())
        .await
        .map(Arc::new)
        .map_err(Error::GetDerivedManifest)?;

    let parquet_opts = amp_worker_core::parquet_opts(ctx.config.parquet_writer.clone());

    // Get dataset for table resolution
    let dataset = ctx
        .datasets_cache
        .get_dataset(&dataset_ref)
        .await
        .map_err(Error::GetDataset)?;

    // Initialize physical tables and compactors
    let mut tables: Vec<(Arc<PhysicalTable>, Arc<AmpCompactor>)> = vec![];
    for table_def in dataset.tables() {
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

        // Resolve networks: raw tables have an intrinsic network; derived tables
        // need resolution from the transitive dependency chain.
        let networks = match table_def.network() {
            Some(id) => BTreeSet::from([id.clone()]),
            None => common::datasets_cache::resolve_dataset_networks(
                &ctx.datasets_cache,
                Arc::clone(&dataset),
            )
            .await
            .map_err(Error::ResolveNetworks)?,
        };

        let physical_table = Arc::new(PhysicalTable::from_revision(
            ctx.data_store.clone(),
            dataset.reference().clone(),
            dataset.start_block(),
            table_def.clone(),
            networks,
            revision,
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

    // Collect all sibling tables for inter-table dependency resolution.
    // Each materialize_table call receives this map so self-ref tables
    // (e.g., `self.blocks_base`) can be resolved to sibling PhysicalTables.
    let siblings: BTreeMap<TableName, Arc<PhysicalTable>> = tables
        .iter()
        .map(|(pt, _)| (pt.table_name().clone(), Arc::clone(pt)))
        .collect();

    // Process all tables in parallel using FailFastJoinSet
    let mut join_set =
        amp_worker_core::tasks::FailFastJoinSet::<Result<(), MaterializeTableError>>::new();

    let env = common::exec_env::create(
        ctx.config.max_mem_mb,
        ctx.config.query_max_mem_mb,
        &ctx.config.spill_location,
        ctx.data_store.clone(),
        ctx.datasets_cache.clone(),
        ctx.ethcall_udfs_cache.clone(),
    )
    .map_err(Error::CreateQueryEnv)?;
    for (table, compactor) in &tables {
        let ctx = ctx.clone();
        let env = env.clone();
        let table = Arc::clone(table);
        let compactor = Arc::clone(compactor);
        let manifest = manifest.clone();
        let siblings = siblings.clone();
        let opts = parquet_opts.clone();

        join_set.spawn(
            async move {
                let table_name = table.table_name().to_string();

                materialize_table(ctx, &manifest, env, table, compactor, opts, end, &siblings)
                    .await?;

                tracing::info!("materialization of `{}` completed successfully", table_name);
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

/// Errors that occur during derived dataset materialization operations
///
/// This error type is used by the `execute()` function to report issues encountered
/// when materializing derived datasets to Parquet files.
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
    GetDataset(#[source] common::datasets_cache::GetDatasetError),

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
    /// The materialization operation cannot proceed without a valid query environment.
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
    /// The table must pass consistency checks before materialization can proceed.
    #[error("Consistency check failed for table '{table_name}'")]
    ConsistencyCheck {
        table_name: String,
        #[source]
        source: amp_worker_core::check::ConsistencyError,
    },

    /// Failed to resolve networks from dependency chain
    ///
    /// This occurs when traversing the derived dataset's dependencies fails
    /// to resolve all raw dataset networks. Common causes:
    /// - Dependency dataset not found
    /// - Failed to resolve dependency revision
    /// - Dataset store connectivity issues
    #[error("Failed to resolve networks from dependencies")]
    ResolveNetworks(#[source] common::datasets_cache::DependencyTraversalError),

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
    GetDerivedManifest(#[source] common::datasets_cache::GetDerivedManifestError),

    /// Failed to materialize individual table
    ///
    /// This occurs when the table-level materialization operation fails. This wraps errors
    /// from the `materialize_table()` function which handles SQL execution, query planning,
    /// and Parquet file writing.
    ///
    /// Common causes:
    /// - SQL query execution failures
    /// - Dependency resolution errors
    /// - Parquet file writing errors
    /// - Metadata database update failures
    #[error("Failed to materialize table '{table_name}'")]
    MaterializeTable {
        table_name: String,
        #[source]
        source: MaterializeTableError,
    },

    /// Parallel task execution failed
    ///
    /// This occurs when one or more parallel table materialization tasks fail or panic.
    /// The fail-fast join set ensures that if any task fails, all remaining
    /// tasks are immediately terminated to prevent partial materializations.
    ///
    /// When this error occurs, the entire materialization operation is aborted and no
    /// partial results are committed. The operation is safe to retry from
    /// the beginning.
    #[error("Parallel table materialization tasks failed")]
    ParallelTasksFailed(#[source] TryWaitAllError<MaterializeTableError>),
}

impl RetryableErrorExt for Error {
    fn is_retryable(&self) -> bool {
        match self {
            // Delegate to inner error classification
            Self::GetDataset(err) => err.is_retryable(),

            // Network resolution depends on dataset store — transient
            Self::ResolveNetworks(err) => err.is_retryable(),

            // Transient DB/store lookup failures
            Self::GetActivePhysicalTable(err) => err.is_retryable(),
            Self::RegisterNewPhysicalTable(_) => true,
            Self::LockRevisionsForWriter(err) => err.is_retryable(),

            // Query environment setup failure — fatal (invalid config)
            Self::CreateQueryEnv(_) => false,

            // Data integrity violation — fatal
            Self::ConsistencyCheck { .. } => false,

            // Delegate to inner error classification
            Self::GetDerivedManifest(err) => err.is_retryable(),

            // Delegate to MaterializeTableError classification
            Self::MaterializeTable { source, .. } => source.is_retryable(),

            // Parallel tasks — delegate to TryWaitAllError classification
            Self::ParallelTasksFailed(err) => err.is_retryable(),
        }
    }
}

impl ErrorDetailsProvider for Error {
    fn detail_source(&self) -> Option<&dyn ErrorDetailsProvider> {
        match self {
            Self::MaterializeTable { source, .. } => Some(source),
            Self::ParallelTasksFailed(err) => Some(err),
            _ => None,
        }
    }
}

impl amp_worker_core::retryable::JobErrorExt for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Self::GetDataset(_) => "GET_DATASET",
            Self::ResolveNetworks(_) => "RESOLVE_NETWORKS",
            Self::GetActivePhysicalTable(_) => "GET_ACTIVE_PHYSICAL_TABLE",
            Self::RegisterNewPhysicalTable(_) => "REGISTER_NEW_PHYSICAL_TABLE",
            Self::LockRevisionsForWriter(_) => "LOCK_REVISIONS_FOR_WRITER",
            Self::CreateQueryEnv(_) => "CREATE_QUERY_ENV",
            Self::ConsistencyCheck { .. } => "CONSISTENCY_CHECK",
            Self::GetDerivedManifest(_) => "GET_DERIVED_MANIFEST",
            Self::MaterializeTable { .. } => "MATERIALIZE_TABLE",
            Self::ParallelTasksFailed(_) => "PARALLEL_TASKS_FAILED",
        }
    }
}

#[cfg(test)]
mod tests {
    use amp_worker_core::block_ranges::ResolutionError;
    use common::sql::ParseSqlError;

    use self::table::{MaterializeTableError, MaterializeTableSpawnError};
    use super::*;

    mod materialize_table_error_is_fatal {
        use super::*;

        #[test]
        fn is_fatal_table_not_found_returns_true() {
            //* Given
            let err = MaterializeTableError::TableNotFound("missing".to_string());

            //* When
            let result = err.is_fatal();

            //* Then
            assert!(result, "TableNotFound should be fatal");
        }

        #[test]
        fn is_fatal_parse_sql_returns_true() {
            //* Given
            let err = MaterializeTableError::ParseSql(ParseSqlError::NoStatements);

            //* When
            let result = err.is_fatal();

            //* Then
            assert!(result, "ParseSql should be fatal");
        }

        #[test]
        fn is_fatal_dependency_not_found_returns_true() {
            //* Given
            let err = MaterializeTableError::DependencyNotFound {
                alias: "dep".to_string(),
                reference: "ref".to_string(),
            };

            //* When
            let result = err.is_fatal();

            //* Then
            assert!(result, "DependencyNotFound should be fatal");
        }
    }

    mod materialize_table_spawn_error_is_fatal {
        use super::*;

        #[test]
        fn is_fatal_resolve_end_block_invalid_returns_true() {
            //* Given
            let err =
                MaterializeTableSpawnError::ResolveEndBlock(ResolutionError::InvalidEndBlock {
                    start_block: 100,
                    end_block: 5,
                });

            //* When
            let result = err.is_fatal();

            //* Then
            assert!(result, "InvalidEndBlock should be fatal");
        }

        #[test]
        fn is_fatal_resolve_end_block_fetch_failed_returns_false() {
            //* Given
            let err = MaterializeTableSpawnError::ResolveEndBlock(
                ResolutionError::FetchLatestFailed("timeout".to_string()),
            );

            //* When
            let result = err.is_fatal();

            //* Then
            assert!(!result, "FetchLatestFailed should not be fatal");
        }
    }
}
