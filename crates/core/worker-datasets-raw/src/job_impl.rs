//! # Raw Datasets implementation
//!
//! This module implements the core logic for materializing raw blockchain datasets to Parquet files.
//! The materialize process extracts blockchain data from a specified block range and writes it to
//! partitioned Parquet files in an object store for efficient querying and analysis.
//!
//! ## Overview
//!
//! The raw dataset materialize operates on blockchain data organized into tables, where each table
//! represents a different type of blockchain entity (e.g., blocks, transactions, logs).
//! The process is designed to handle large datasets efficiently through parallelization
//! and partitioning strategies.
//!
//! ## Materialization Process
//!
//! The materialize process follows these main steps:
//!
//! 1. **Block Range Resolution**: Determines the actual block range to process by resolving
//!    relative block numbers (negative values) against the latest block number from the client.
//!
//! 2. **Range Intersection**: Computes the intersection of block ranges across all tables to
//!    ensure only blocks that need to be scanned for all tables are processed. This avoids
//!    redundant work when different tables have different scanning progress.
//!
//! 3. **Gap Detection**: Identifies ranges of blocks that haven't been scanned yet by computing
//!    the complement of already-scanned ranges within the target block range.
//!
//! 4. **Job Partitioning**: Splits the unscanned block ranges across multiple parallel jobs
//!    to balance the workload. Each job processes a subset of block ranges independently.
//!    Jobs are sized with a minimum of 2000 blocks per partition to ensure efficient
//!    processing while maintaining reasonable parallelism.
//!
//! 5. **Parallel Execution**: Spawns multiple materialize partition tasks that run concurrently,
//!    with staggered starts to avoid overwhelming the blockchain client with simultaneous
//!    requests.
//!
//! ## Partition Strategy
//!
//! Each materialize partition operates independently and processes its assigned block ranges sequentially.
//! Within each partition:
//!
//! - **Block Streaming**: Fetches blocks from the blockchain client using an asynchronous
//!   stream to maintain a steady flow of data without overwhelming memory. Uses a buffered
//!   channel (100 blocks) to balance memory usage with streaming performance.
//!
//! - **Table Partitioning**: Writes data to separate Parquet files for each table, with
//!   automatic file rotation based on a configurable partition size limit. This ensures
//!   files remain manageable and optimizes query performance.
//!
//! - **Metadata Tracking**: Updates metadata database with information about which block
//!   ranges have been successfully processed for each table, enabling incremental materializations
//!   and resume capabilities.
//!
//! ## Error Handling and Reliability
//!
//! The materialize process is designed for reliability in distributed environments:
//!
//! - **Early Termination**: If any partition job fails, all other jobs are terminated
//!   to prevent partial or inconsistent materializations.
//!
//! - **Atomic Operations**: Each partition completes its assigned block ranges atomically,
//!   ensuring that partial progress is not recorded in case of failures. Metadata updates
//!   only occur after successful completion of entire ranges.
//!
//! - **Resource Management**: Proper cleanup of resources including file handles and
//!   network connections, even in error scenarios.
//!
//! ## Performance Considerations
//!
//! The implementation includes several optimizations for handling large-scale blockchain data:
//!
//! - **Configurable Parallelism**: The number of concurrent jobs can be tuned based on
//!   available resources and client rate limits.
//!
//! - **Adaptive Partitioning**: File sizes are controlled by monitoring uncompressed data
//!   size, allowing for consistent partition sizes across different data densities.
//!
//! - **Rate Limiting**: Staggered job starts help avoid overwhelming blockchain clients
//!   with simultaneous connection requests. Jobs are started with a 1-second delay
//!   between each spawn to distribute the initial connection load.
use std::{collections::BTreeMap, ops::RangeInclusive, sync::Arc, time::Instant};

use amp_data_store::retryable::RetryableErrorExt as _;
use amp_providers_registry::retryable::RetryableErrorExt as _;
use amp_worker_core::{
    ResolvedEndBlock,
    block_ranges::resolve_end_block,
    check::consistency_check,
    compaction::AmpCompactor,
    error_detail::ErrorDetailsProvider,
    progress::{SyncCompletedInfo, SyncFailedInfo, SyncStartedInfo},
    retryable::RetryableErrorExt,
    tasks::TryWaitAllError,
};
use common::{
    catalog::{logical::LogicalTable, physical::Catalog},
    physical_table::{MissingRangesError, PhysicalTable, segments::merge_ranges},
    retryable::RetryableErrorExt as _,
};
use datasets_common::{
    block_num::BlockNum, dataset::Dataset as _, hash_reference::HashReference,
    table_name::TableName,
};
use datasets_raw::{
    client::{BlockStreamer as _, BlockStreamerExt as _, LatestBlockError},
    dataset::Dataset as RawDataset,
};

use crate::{job_ctx::Context, job_descriptor::JobDescriptor};

mod ranges;
mod writer;

use self::ranges::{RunRangeError, materialize_ranges, spawn_freshness_tracker};

/// Executes a raw dataset job. All tables must belong to the same dataset.
#[tracing::instrument(skip_all, err)]
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
    let max_writers = desc.max_writers;

    let progress_reporter = ctx.progress_reporter.clone();
    let dataset = ctx
        .datasets_cache
        .get_dataset(&dataset_ref)
        .await
        .map_err(Error::GetDataset)?;
    let dataset = dataset
        .downcast_arc::<RawDataset>()
        .map_err(|_| Error::NotARawDataset(dataset_ref.clone()))?;

    let writer = writer.into();
    let dataset_reference = dataset.reference();

    let materialize_start_time = Instant::now();
    let parquet_opts = amp_worker_core::parquet_opts(ctx.config.parquet_writer.clone());

    // Initialize physical tables and compactors
    let mut tables: Vec<(Arc<PhysicalTable>, Arc<AmpCompactor>)> = vec![];
    for table_def in dataset.tables() {
        // Try to get existing active physical table (handles retry case)
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
                .create_new_table_revision(dataset_reference, table_def.name())
                .await
                .map_err(Error::RegisterNewPhysicalTable)?,
        };

        let physical_table = Arc::new(PhysicalTable::from_revision(
            ctx.data_store.clone(),
            dataset.reference().clone(),
            dataset.start_block(),
            table_def.clone(),
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

    let sql_schema_name = dataset.reference().to_string();
    let resolved_tables: Vec<_> = tables
        .iter()
        .map(|(t, _)| {
            LogicalTable::new(
                sql_schema_name.clone(),
                dataset_reference.clone(),
                t.table().clone(),
            )
        })
        .collect();
    let catalog = Catalog::new(
        resolved_tables,
        vec![],
        tables
            .iter()
            .map(|(t, _)| (Arc::clone(t), Arc::from(sql_schema_name.as_str())))
            .collect(),
        Default::default(),
    );

    // Ensure consistency before starting the materialize procedure.
    for (table, _) in &tables {
        consistency_check(table, &ctx.data_store)
            .await
            .map_err(|err| Error::ConsistencyCheck {
                table_name: table.table_name().clone(),
                source: err,
            })?;
    }

    // Spawn freshness tracker if metrics are available
    ctx.metrics.as_ref().map(|metrics| {
        spawn_freshness_tracker(
            catalog.clone(),
            ctx.notification_multiplexer.clone(),
            metrics.clone(),
        )
    });

    let metrics = ctx.metrics.clone();
    let finalized_blocks_only = dataset.finalized_blocks_only();

    let kind = dataset.kind();
    let network = dataset.network();
    let mut client = ctx
        .ethcall_udfs_cache
        .providers_registry()
        .create_block_stream_client(kind, network, metrics.as_ref().map(|m| m.meter()))
        .await
        .map_err(Error::CreateBlockStreamClient)?
        .with_retry();

    let provider_name = client.provider_name().to_string();
    tracing::info!("connected to provider: {provider_name}");

    let start = dataset.start_block().unwrap_or(0);
    let resolved = resolve_end_block(&end, start, client.latest_block(finalized_blocks_only))
        .await
        .map_err(Error::ResolveEndBlock)?;

    let end = match resolved {
        ResolvedEndBlock::NoDataAvailable => {
            tracing::warn!("no blocks available from provider: {provider_name}");
            return Ok(());
        }
        ResolvedEndBlock::Continuous => None,
        ResolvedEndBlock::Block(block) => Some(block),
    };

    let mut timer = tokio::time::interval(ctx.config.poll_interval);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // Emit sync.started event for each table
    if let Some(ref reporter) = progress_reporter {
        for (table, _) in &tables {
            reporter.report_sync_started(SyncStartedInfo {
                table_name: table.table_name().clone(),
                start_block: dataset.start_block(),
                end_block: end,
            });
        }
    }

    // In order to resolve reorgs in the same block as they are detected, we run the
    // `materialize_ranges` procedure in a loop.
    //
    // To reduce RPC polling of `latest_block`, we wait on `timer` when we know the
    // next iteration would have no work to do unless there is a new block.
    //
    // We wrap the loop in a block to handle errors and emit failure events.
    let materialize_result: Result<(), Error> = async {
        loop {
            let Some(latest_block) = client
                .latest_block(finalized_blocks_only)
                .await
                .map_err(Error::LatestBlock)?
            else {
                // No data to materialize, wait for more data
                timer.tick().await;
                continue;
            };
            if latest_block < start {
                // Start not yet reached, wait for more data
                timer.tick().await;
                continue;
            }

            let mut missing_ranges_by_table: BTreeMap<TableName, Vec<RangeInclusive<BlockNum>>> =
                Default::default();

            let mut compactors_by_table: BTreeMap<TableName, Arc<AmpCompactor>> =
                Default::default();

            for (table, compactor) in &tables {
                let end = match end {
                    None => latest_block,
                    Some(end) => BlockNum::min(end, latest_block),
                };
                let missing_ranges = table
                    .missing_ranges(start..=end)
                    .await
                    .map_err(Error::MissingRanges)?;
                let table_name = table.table_name();
                missing_ranges_by_table.insert(table_name.clone(), missing_ranges);
                compactors_by_table.insert(table_name.clone(), Arc::clone(compactor));
            }

            // Use the union of missing table block ranges.
            let missing_dataset_ranges = {
                let ranges: Vec<RangeInclusive<BlockNum>> = missing_ranges_by_table
                    .values()
                    .flatten()
                    .cloned()
                    .collect();
                merge_ranges(ranges)
            };

            // If there are no ranges then there is no more work to do, check materialize end condition.
            if missing_dataset_ranges.is_empty() {
                // If we've reached the configured end block, stop completely and return.
                if let Some(end) = end
                    && end <= latest_block
                {
                    break;
                } else {
                    // Otherwise, wait for more data.
                    timer.tick().await;
                    continue;
                }
            }

            materialize_ranges(
                missing_dataset_ranges,
                max_writers,
                &client,
                &ctx,
                &catalog,
                parquet_opts.clone(),
                missing_ranges_by_table,
                compactors_by_table,
                &tables,
                start,
                end,
                latest_block,
            )
            .await
            .map_err(Error::PartitionTask)?;
        }
        Ok(())
    }
    .await;

    // Handle materialize errors by emitting failure events for each table
    if let Err(ref err) = materialize_result {
        if let Some(ref reporter) = progress_reporter {
            let error_message = err.to_string();
            for (table, _) in &tables {
                reporter.report_sync_failed(SyncFailedInfo {
                    table_name: table.table_name().clone(),
                    error_message: error_message.clone(),
                    error_type: Some("MaterializeError".to_string()),
                });
            }
        }
        return materialize_result;
    }

    // Record materialize duration on successful completion
    let duration_millis = materialize_start_time.elapsed().as_millis() as u64;
    if let Some(ref metrics) = metrics {
        for (table, _compactor) in &tables {
            let table_name = table.table_name().to_string();
            metrics.record_dump_duration(duration_millis as f64, table_name);
        }
    }

    // Emit sync.completed event for each table
    if let Some(ref reporter) = progress_reporter {
        // The final block is the configured end block (we only exit the loop when we reach it)
        let final_block = end.expect("materialize loop only exits when end block is reached");
        for (table, _) in &tables {
            reporter.report_sync_completed(SyncCompletedInfo {
                table_name: table.table_name().clone(),
                final_block,
                duration_millis,
            });
        }
    }

    tracing::info!("materialize completed successfully");

    Ok(())
}

/// Errors that occur during raw dataset materialize operations
///
/// This error type is used by the `execute()` function to report issues encountered
/// when materializing raw datasets to Parquet files.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to load the dataset from the dataset store.
    #[error("Failed to get dataset")]
    GetDataset(#[source] common::datasets_cache::GetDatasetError),

    /// The dataset is not a raw dataset (e.g., it is a derived dataset).
    #[error("Dataset '{0}' is not a raw dataset")]
    NotARawDataset(HashReference),

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

    /// Failed consistency check for table
    ///
    /// This occurs when the consistency check detects issues between metadata database
    /// and object store for a table. Common causes:
    /// - Missing registered files (data corruption)
    /// - Object store connectivity issues
    /// - Metadata database query failures
    ///
    /// The table must pass consistency checks before materialize can proceed.
    #[error("Consistency check failed for table '{table_name}'")]
    ConsistencyCheck {
        table_name: TableName,
        #[source]
        source: amp_worker_core::check::ConsistencyError,
    },

    /// Failed to create block stream client for dataset
    ///
    /// This occurs when the materialize implementation cannot create a blockchain client
    /// for the dataset's provider. Common causes:
    /// - Provider configuration not found
    /// - Invalid provider configuration
    /// - Network connectivity issues to provider
    #[error("Failed to create block stream client for dataset")]
    CreateBlockStreamClient(#[source] amp_providers_registry::CreateClientError),

    /// Failed to resolve end block number
    ///
    /// This occurs when resolving the end block (absolute or relative) against
    /// the blockchain's latest block fails. Common causes:
    /// - Blockchain client connectivity issues (when fetching latest block)
    /// - Invalid end block configuration (end < start)
    /// - RPC provider returning invalid block numbers
    /// - Provider temporarily unavailable
    #[error("Failed to resolve end block")]
    ResolveEndBlock(#[source] amp_worker_core::block_ranges::ResolutionError),

    /// Failed to get latest block number from blockchain client
    ///
    /// This occurs when querying the blockchain provider for the latest block fails.
    /// Common causes:
    /// - Network connectivity issues
    /// - RPC provider rate limiting
    /// - Provider temporarily unavailable
    /// - Invalid provider credentials
    #[error("Failed to get latest block number")]
    LatestBlock(#[source] LatestBlockError),

    /// Failed to get missing block ranges for table
    ///
    /// This occurs when querying the metadata database for unprocessed block ranges
    /// fails. The metadata database tracks which block ranges have been successfully
    /// materialized to prevent redundant work.
    ///
    /// Common causes:
    /// - Metadata database connectivity issues
    /// - Corrupted file metadata
    /// - Database query timeout
    #[error("Failed to get missing block ranges for table")]
    MissingRanges(#[source] MissingRangesError),

    /// A partition task execution failed
    ///
    /// This occurs when one of the parallel partition tasks fails during execution
    /// or panics unexpectedly. When a partition task fails, all other running
    /// partitions are terminated to prevent partial materializations.
    ///
    /// Common causes:
    /// - Block streaming failures from blockchain client
    /// - Parquet file writing errors
    /// - Metadata database update failures
    /// - Object store connectivity issues
    /// - RawDatasetWriter initialization failures
    /// - Partition task panics (assertion failures, unwrap on None/Err, stack overflow)
    #[error("Partition task failed")]
    PartitionTask(#[source] TryWaitAllError<RunRangeError>),
}

impl RetryableErrorExt for Error {
    fn is_retryable(&self) -> bool {
        match self {
            // Delegate to inner error classification
            Self::GetDataset(err) => err.is_retryable(),

            // Wrong dataset type — fatal
            Self::NotARawDataset(_) => false,

            // Transient DB/store lookup failures
            Self::GetActivePhysicalTable(err) => err.is_retryable(),
            Self::RegisterNewPhysicalTable(_) => true,
            Self::LockRevisionsForWriter(err) => err.is_retryable(),

            // Data integrity violation — fatal
            Self::ConsistencyCheck { .. } => false,

            // Delegate to inner error classification
            Self::CreateBlockStreamClient(err) => err.is_retryable(),

            // Block range resolution — inspect the source variant
            Self::ResolveEndBlock(err) => err.is_retryable(),

            // Transient RPC/network failures — recoverable
            Self::LatestBlock(_) => true,

            // Delegate to inner error classification
            Self::MissingRanges(err) => err.is_retryable(),

            // Partition tasks — delegate to TryWaitAllError classification
            Self::PartitionTask(err) => err.is_retryable(),
        }
    }
}

impl ErrorDetailsProvider for Error {
    fn detail_source(&self) -> Option<&dyn ErrorDetailsProvider> {
        match self {
            Self::PartitionTask(err) => Some(err),
            _ => None,
        }
    }
}

impl amp_worker_core::retryable::JobErrorExt for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Self::GetDataset(_) => "GET_DATASET",
            Self::NotARawDataset(_) => "NOT_A_RAW_DATASET",
            Self::GetActivePhysicalTable(_) => "GET_ACTIVE_PHYSICAL_TABLE",
            Self::RegisterNewPhysicalTable(_) => "REGISTER_NEW_PHYSICAL_TABLE",
            Self::LockRevisionsForWriter(_) => "LOCK_REVISIONS_FOR_WRITER",
            Self::ConsistencyCheck { .. } => "CONSISTENCY_CHECK",
            Self::CreateBlockStreamClient(_) => "CREATE_BLOCK_STREAM_CLIENT",
            Self::ResolveEndBlock(_) => "RESOLVE_END_BLOCK",
            Self::LatestBlock(_) => "LATEST_BLOCK",
            Self::MissingRanges(_) => "MISSING_RANGES",
            Self::PartitionTask(_) => "PARTITION_TASK",
        }
    }
}
