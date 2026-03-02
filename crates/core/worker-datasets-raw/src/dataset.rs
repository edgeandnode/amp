//! # Raw Datasets dump implementation
//!
//! This module implements the core logic for dumping raw blockchain datasets to Parquet files.
//! The dump process extracts blockchain data from a specified block range and writes it to
//! partitioned Parquet files in an object store for efficient querying and analysis.
//!
//! ## Overview
//!
//! The raw dataset dump operates on blockchain data organized into tables, where each table
//! represents a different type of blockchain entity (e.g., blocks, transactions, logs).
//! The process is designed to handle large datasets efficiently through parallelization
//! and partitioning strategies.
//!
//! ## Dump Process
//!
//! The dump process follows these main steps:
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
//! 5. **Parallel Execution**: Spawns multiple dump partition tasks that run concurrently,
//!    with staggered starts to avoid overwhelming the blockchain client with simultaneous
//!    requests.
//!
//! ## Partition Strategy
//!
//! Each dump partition operates independently and processes its assigned block ranges sequentially.
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
//!   ranges have been successfully processed for each table, enabling incremental dumps
//!   and resume capabilities.
//!
//! ## Error Handling and Reliability
//!
//! The dump process is designed for reliability in distributed environments:
//!
//! - **Early Termination**: If any partition job fails, all other jobs are terminated
//!   to prevent partial or inconsistent dumps.
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

use std::{
    collections::BTreeMap,
    ops::RangeInclusive,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use amp_data_store::DataStore;
use amp_worker_core::{
    Ctx, EndBlock, ResolvedEndBlock, WriterProperties,
    block_ranges::resolve_end_block,
    check::consistency_check,
    compaction::AmpCompactor,
    metrics,
    progress::{
        ProgressReporter, ProgressUpdate, SyncCompletedInfo, SyncFailedInfo, SyncStartedInfo,
    },
    tasks::{FailFastJoinSet, TryWaitAllError},
};
use common::{
    BlockNum,
    catalog::{
        logical::{LogicalCatalog, LogicalTable},
        physical::{Catalog, CatalogTable},
    },
    parquet::errors::ParquetError,
    physical_table::{MissingRangesError, PhysicalTable, segments::merge_ranges},
};
use datasets_common::{hash_reference::HashReference, table_name::TableName};
use datasets_raw::client::{
    BlockStreamError, BlockStreamer, BlockStreamerExt as _, CleanupError, LatestBlockError,
};
use futures::TryStreamExt as _;
use metadata_db::{MetadataDb, NotificationMultiplexerHandle, physical_table_revision::LocationId};
use monitoring::logging;
use tokio::task::JoinHandle;
use tracing::{Instrument, instrument};

use crate::writer::{RawDatasetWriter, RawDatasetWriterCloseError, RawDatasetWriterError};

/// Dumps a set of raw dataset tables. All tables must belong to the same dataset.
#[instrument(skip_all, err)]
pub async fn dump(
    ctx: Ctx,
    dataset_reference: &HashReference,
    max_writers: u16,
    end: EndBlock,
    writer: impl Into<Option<metadata_db::jobs::JobId>>,
    progress_reporter: Option<Arc<dyn ProgressReporter>>,
) -> Result<(), Error> {
    let writer = writer.into();

    let dump_start_time = Instant::now();
    let parquet_opts = amp_worker_core::parquet_opts(&ctx.config.parquet);

    let dataset = ctx
        .dataset_store
        .get_dataset(dataset_reference)
        .await
        .map_err(Error::GetDataset)?;

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
    let logical = LogicalCatalog::from_tables(resolved_tables.iter());
    let catalog = Catalog::new(
        logical,
        tables
            .iter()
            .map(|(t, _)| CatalogTable::new(Arc::clone(t), sql_schema_name.clone()))
            .collect(),
    );

    // Ensure consistency before starting the dump procedure.
    for (table, _) in &tables {
        consistency_check(table, &ctx.data_store)
            .await
            .map_err(|err| Error::ConsistencyCheck {
                table_name: table.table_name().to_string(),
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
    let network = dataset
        .tables()
        .first()
        .expect("raw dataset must have tables")
        .network();
    let mut client = ctx
        .providers_registry
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
    // `dump_ranges` procedure in a loop.
    //
    // To reduce RPC polling of `latest_block`, we wait on `timer` when we know the
    // next iteration would have no work to do unless there is a new block.
    //
    // We wrap the loop in a block to handle errors and emit failure events.
    let dump_result: Result<(), Error> = async {
        loop {
            let Some(latest_block) = client
                .latest_block(finalized_blocks_only)
                .await
                .map_err(Error::LatestBlock)?
            else {
                // No data to dump, wait for more data
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

            // If there are no ranges then there is no more work to do, check dump end condition.
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

            dump_ranges(
                missing_dataset_ranges,
                max_writers,
                &client,
                &ctx,
                &catalog,
                parquet_opts.clone(),
                missing_ranges_by_table,
                compactors_by_table,
                metrics.as_ref(),
                &tables,
                start,
                end,
                latest_block,
                progress_reporter.clone(),
            )
            .await?;
        }
        Ok(())
    }
    .await;

    // Handle dump errors by emitting failure events for each table
    if let Err(ref err) = dump_result {
        if let Some(ref reporter) = progress_reporter {
            let error_message = err.to_string();
            for (table, _) in &tables {
                reporter.report_sync_failed(SyncFailedInfo {
                    table_name: table.table_name().clone(),
                    error_message: error_message.clone(),
                    error_type: Some("DumpError".to_string()),
                });
            }
        }
        return dump_result;
    }

    // Record dump duration on successful completion
    let duration_millis = dump_start_time.elapsed().as_millis() as u64;
    if let Some(ref metrics) = metrics {
        for (table, _compactor) in &tables {
            let table_name = table.table_name().to_string();
            metrics.record_dump_duration(duration_millis as f64, table_name);
        }
    }

    // Emit sync.completed event for each table
    if let Some(ref reporter) = progress_reporter {
        // The final block is the configured end block (we only exit the loop when we reach it)
        let final_block = end.expect("dump loop only exits when end block is reached");
        for (table, _) in &tables {
            reporter.report_sync_completed(SyncCompletedInfo {
                table_name: table.table_name().clone(),
                final_block,
                duration_millis,
            });
        }
    }

    client.wait_for_cleanup().await.map_err(Error::Cleanup)?;
    tracing::info!("dump completed successfully");

    Ok(())
}

/// Errors that occur during raw dataset dump operations
///
/// This error type is used by the `dump()` function to report issues encountered
/// when dumping raw datasets to Parquet files.
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
    GetDataset(#[source] common::dataset_store::GetDatasetError),

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
    /// The table must pass consistency checks before dump can proceed.
    #[error("Consistency check failed for table '{table_name}'")]
    ConsistencyCheck {
        table_name: String,
        #[source]
        source: amp_worker_core::check::ConsistencyError,
    },

    /// Failed to create block stream client for dataset
    ///
    /// This occurs when the dump implementation cannot create a blockchain client
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
    /// dumped to prevent redundant work.
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
    /// partitions are terminated to prevent partial dumps.
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

    /// Failure during blockchain client cleanup
    ///
    /// At the end of the dump process, the blockchain client may need to perform
    /// cleanup operations, some of which could fail. This error indicates such a failure.
    #[error("Failed to perform blockchain client cleanup: {0}")]
    Cleanup(#[source] CleanupError),
}

impl Error {
    pub fn is_fatal(&self) -> bool {
        // TODO: To keep things semantically the same, [TryWaitAllError::Panic] is not
        // considered a fatal error, even though it indicates a bug. We may want to
        // revisit this in the future.
        matches!(
            self,
            Self::PartitionTask(TryWaitAllError::Error(RunRangeError::ReadStream(
                BlockStreamError::Fatal(_)
            )))
        )
    }
}

/// Dumps block ranges by partitioning them across multiple parallel workers.
#[instrument(skip_all, err)]
#[expect(clippy::too_many_arguments)]
async fn dump_ranges<S: BlockStreamer + Send + Sync>(
    missing_dataset_ranges: Vec<RangeInclusive<BlockNum>>,
    max_writers: u16,
    client: &S,
    ctx: &Ctx,
    catalog: &Catalog,
    parquet_opts: Arc<WriterProperties>,
    missing_ranges_by_table: BTreeMap<TableName, Vec<RangeInclusive<BlockNum>>>,
    compactors_by_table: BTreeMap<TableName, Arc<AmpCompactor>>,
    metrics: Option<&Arc<metrics::MetricsRegistry>>,
    tables: &[(Arc<PhysicalTable>, Arc<AmpCompactor>)],
    // The job's actual start block (from dataset definition), used for progress events
    job_start_block: BlockNum,
    // The job's actual end block (resolved), used for progress events. None for continuous mode.
    job_end_block: Option<BlockNum>,
    // Current chain head block, used for percentage calculation in continuous mode
    chain_head: BlockNum,
    // Optional progress reporter for external event streaming
    progress_reporter: Option<Arc<dyn ProgressReporter>>,
) -> Result<(), Error> {
    tracing::info!(
        "dumping ranges {}",
        missing_dataset_ranges
            .iter()
            .map(|r| format!("[{}-{}]", r.start(), r.end()))
            .collect::<Vec<String>>()
            .join(", ")
    );

    // Split them across the target number of writers as to balance the number of blocks per writer.
    let missing_dataset_ranges =
        split_and_partition(missing_dataset_ranges, max_writers as u64, 2000);

    // Extract table names for progress reporting
    let table_names: Vec<TableName> = tables.iter().map(|(t, _)| t.table_name().clone()).collect();

    // Use the job's actual start/end blocks for progress events (not the missing ranges).
    // This ensures consumers see the overall job progress, not just progress through remaining work.
    // For continuous mode, chain_head is used instead of job_end_block for percentage calculation.
    let progress_tracker = Arc::new(ProgressTracker {
        last_logged_percent: AtomicUsize::new(0),
        last_emission_time: Mutex::new(Instant::now() - ctx.config.progress_interval),
        last_emitted_block: AtomicU64::new(0),
        has_emitted: AtomicBool::new(false),
        interval: ctx.config.progress_interval,
        progress_reporter: progress_reporter.clone(),
        table_names,
        job_start_block,
        job_end_block,
        chain_head,
    });

    let writers = missing_dataset_ranges
        .into_iter()
        .enumerate()
        .map(|(i, ranges)| DumpPartition {
            block_streamer: client.clone(),
            metadata_db: ctx.metadata_db.clone(),
            data_store: ctx.data_store.clone(),
            catalog: catalog.clone(),
            ranges,
            parquet_opts: parquet_opts.clone(),
            missing_ranges_by_table: missing_ranges_by_table.clone(),
            compactors_by_table: compactors_by_table.clone(),
            id: i as u32,
            metrics: metrics.cloned(),
            progress_tracker: progress_tracker.clone(),
        });

    // Spawn the writers, starting them with a 1-second delay between each.
    // Note that tasks spawned in the join set start executing immediately in parallel
    let mut join_set = FailFastJoinSet::<Result<(), RunRangeError>>::new();
    for writer in writers {
        let span = tracing::info_span!("dump_partition", partition_id = writer.id);
        join_set.spawn(writer.run().instrument(span));
    }

    // Wait for all the writers to finish, returning an error if any writer panics or fails.
    if let Err(err) = join_set.try_wait_all().await {
        // Log detailed error information based on error type
        match &err {
            TryWaitAllError::Error(err) => {
                tracing::error!(
                    error=%err,
                    error_source=logging::error_source(&err),
                    "dataset dump failed: partition task error"
                );
            }
            TryWaitAllError::Panic(err) => {
                tracing::error!(error=%err, "dataset dump failed: partition task panicked");
            }
        }

        // Record error metrics
        if let Some(metrics) = metrics {
            for (table, _) in tables {
                let table_name = table.table_name().to_string();
                metrics.record_dump_error(table_name);
            }
        }

        return Err(Error::PartitionTask(err));
    }

    Ok(())
}

/// Internal progress tracker that handles throttling and forwards to the external reporter.
struct ProgressTracker {
    /// Last percentage that was logged (for throttling log output)
    last_logged_percent: AtomicUsize,
    /// Time of last progress event emission (for time-based throttling)
    last_emission_time: Mutex<Instant>,
    /// Last block that triggered a progress event
    last_emitted_block: AtomicU64,
    /// Whether we've emitted at least one progress event
    has_emitted: AtomicBool,
    /// Minimum interval between progress events
    interval: Duration,
    /// Optional progress reporter for external event streaming
    progress_reporter: Option<Arc<dyn ProgressReporter>>,
    /// Table names for progress reporting
    table_names: Vec<TableName>,
    /// The job's actual start block (from dataset definition)
    job_start_block: BlockNum,
    /// The job's actual end block (resolved). None for continuous mode.
    job_end_block: Option<BlockNum>,
    /// Current chain head block, used for percentage calculation in continuous mode
    chain_head: BlockNum,
}

impl ProgressTracker {
    /// Signal to the progress tracker that another block has been covered.
    fn block_covered(&self, current_block: BlockNum) {
        // Calculate current percentage based on chain head position (for logging).
        // For bounded jobs: percentage = (current - start) / (end - start) * 100
        // For continuous jobs: percentage = (current - start) / (chain_head - start) * 100
        let effective_end = self.job_end_block.unwrap_or(self.chain_head);
        let total_range = effective_end.saturating_sub(self.job_start_block) + 1;
        let blocks_done = current_block.saturating_sub(self.job_start_block) + 1;
        let current_percent = if total_range > 0 {
            ((blocks_done as f64 / total_range as f64) * 100.0).min(100.0) as usize
        } else {
            0
        };

        // Log progress when percentage increases (throttled to avoid spam)
        let last_logged = self.last_logged_percent.load(Ordering::SeqCst);
        if current_percent > last_logged {
            // Only log every 5% to avoid excessive logging
            if current_percent / 5 > last_logged / 5 {
                tracing::info!(
                    "overall progress: {blocks_done}/{total_range} blocks ({current_percent}%)",
                );
            }
            self.last_logged_percent
                .store(current_percent, Ordering::SeqCst);
        }

        // Time-based progress event emission
        // Only emit if: interval has elapsed AND we have new progress
        if let Some(ref reporter) = self.progress_reporter {
            let last_block = self.last_emitted_block.load(Ordering::SeqCst);
            let has_emitted = self.has_emitted.load(Ordering::SeqCst);

            // Check if we have new progress: either first emission or current > last
            let progress_made = !has_emitted || current_block > last_block;

            if progress_made {
                let mut last_time = self.last_emission_time.lock().unwrap();
                let now = Instant::now();

                if now.duration_since(*last_time) >= self.interval {
                    // Update state before emitting
                    *last_time = now;
                    drop(last_time); // Release lock before emitting
                    self.last_emitted_block
                        .store(current_block, Ordering::SeqCst);
                    self.has_emitted.store(true, Ordering::SeqCst);

                    // Report for each table
                    for table_name in &self.table_names {
                        reporter.report_progress(ProgressUpdate {
                            table_name: table_name.clone(),
                            start_block: self.job_start_block,
                            current_block,
                            end_block: self.job_end_block,
                            // File and byte counts are tracked at the writer level and
                            // reported via metrics. Progress reports focus on block progress.
                            files_count: 0,
                            total_size_bytes: 0,
                        });
                    }
                }
            }
        }
    }
}

/// Splits block ranges into at most `n` partitions where each partition is as equal in total
/// length as possible. If a range exceeds the target partition size, it is split across
/// partitions. `min_partition_blocks` should be used to prevent partitions from being too small,
/// though the last partition may still be smaller than that.
fn split_and_partition(
    mut ranges: Vec<RangeInclusive<BlockNum>>,
    n: u64,
    min_partition_blocks: u64,
) -> Vec<Vec<RangeInclusive<BlockNum>>> {
    if ranges.is_empty() {
        return vec![];
    }

    let range_blocks = |r: &RangeInclusive<BlockNum>| -> u64 { (*r.end() - *r.start()) + 1 };
    let total_blocks =
        |ranges: &[RangeInclusive<BlockNum>]| -> u64 { ranges.iter().map(range_blocks).sum() };
    let target_partition_blocks = total_blocks(&ranges).div_ceil(n).max(min_partition_blocks);
    let mut partitions: Vec<Vec<RangeInclusive<BlockNum>>> = Default::default();
    let mut current_partition: Vec<RangeInclusive<BlockNum>> = Default::default();
    let mut capacity = target_partition_blocks;
    while !ranges.is_empty() {
        let len = range_blocks(&ranges[0]);
        if len > capacity {
            let (start, end) = ranges[0].clone().into_inner();
            let new_end = (start + capacity) - 1;
            ranges[0] = (new_end + 1)..=end;
            current_partition.push(start..=new_end);
            capacity = 0;
        } else {
            current_partition.push(ranges.remove(0));
            capacity -= len;
        }
        if capacity == 0 {
            capacity = target_partition_blocks;
            partitions.push(current_partition);
            current_partition = Default::default();
        }
    }
    assert!(
        partitions
            .iter()
            .all(|p| total_blocks(p) >= min_partition_blocks)
    );
    if !current_partition.is_empty() {
        partitions.push(current_partition);
    }
    assert!(partitions.len() <= n as usize);
    partitions
}

/// A partition of a raw dataset dump job that processes a subset of block ranges.
///
/// Each partition operates independently and processes its assigned block ranges sequentially.
/// The partition is responsible for:
/// - Streaming blocks from the blockchain client
/// - Writing data to Parquet files with automatic file rotation
/// - Updating metadata to track processing progress
/// - Handling errors and resource cleanup
///
/// Partitions are created by splitting the total work across multiple parallel jobs
/// to balance load and improve throughput while respecting client rate limits.
struct DumpPartition<S: BlockStreamer> {
    /// The block streamer
    block_streamer: S,
    /// The metadata database
    metadata_db: MetadataDb,
    /// The data store for object storage operations
    data_store: DataStore,
    /// The tables to write to
    catalog: Catalog,
    /// The block ranges to scan
    ranges: Vec<RangeInclusive<BlockNum>>,
    /// The Parquet writer properties
    parquet_opts: Arc<WriterProperties>,
    /// The missing block ranges by table
    missing_ranges_by_table: BTreeMap<TableName, Vec<RangeInclusive<BlockNum>>>,
    /// The compactors for each table
    compactors_by_table: BTreeMap<TableName, Arc<AmpCompactor>>,
    /// The partition ID
    id: u32,
    /// Metrics registry
    metrics: Option<Arc<metrics::MetricsRegistry>>,
    /// A progress tracker which logs the overall progress of all partitions.
    progress_tracker: Arc<ProgressTracker>,
}
impl<S: BlockStreamer> DumpPartition<S> {
    /// Consumes the instance returning a future that runs the partition, processing all assigned block ranges sequentially.
    async fn run(self) -> Result<(), RunRangeError> {
        tracing::info!(
            "ranges to scan: {}",
            self.ranges
                .iter()
                .map(|r| format!("[{}-{}]", r.start(), r.end()))
                .collect::<Vec<String>>()
                .join(", "),
        );

        // The ranges are run sequentially by design, as parallelism is controlled by the number of jobs.
        for range in &self.ranges {
            tracing::info!(
                "starting scan for range [{}-{}]",
                range.start(),
                range.end(),
            );
            let start_time = Instant::now();

            self.run_range(range.clone()).await?;

            tracing::info!(
                "finished scan for range [{}-{}] in {} minutes",
                range.start(),
                range.end(),
                start_time.elapsed().as_secs() / 60
            );
        }
        Ok(())
    }

    async fn run_range(&self, range: RangeInclusive<BlockNum>) -> Result<(), RunRangeError> {
        let stream = {
            let block_streamer = self.block_streamer.clone();
            block_streamer
                .block_stream(*range.start(), *range.end())
                .await
        };

        // limit the missing table ranges to the partition range
        let mut missing_ranges_by_table: BTreeMap<TableName, Vec<RangeInclusive<BlockNum>>> =
            Default::default();
        for (table, ranges) in &self.missing_ranges_by_table {
            let entry = missing_ranges_by_table.entry(table.clone()).or_default();
            for missing in ranges {
                let start = BlockNum::max(*missing.start(), *range.start());
                let end = BlockNum::min(*missing.end(), *range.end());
                if start <= end {
                    entry.push(start..=end);
                }
            }
        }

        let mut writer = RawDatasetWriter::new(
            self.catalog.clone(),
            self.metadata_db.clone(),
            self.data_store.clone(),
            self.parquet_opts.clone(),
            missing_ranges_by_table,
            self.compactors_by_table.clone(),
            self.metrics.clone(),
        )
        .map_err(RunRangeError::CreateWriter)?;

        let mut stream = std::pin::pin!(stream);
        let mut prev_block_num = None;
        while let Some(dataset_rows) = stream.try_next().await.map_err(RunRangeError::ReadStream)? {
            let cur_block_num = dataset_rows.block_num();
            if let Some(prev) = prev_block_num
                && cur_block_num <= prev
            {
                return Err(RunRangeError::NonIncreasingBlockNum {
                    previous: prev,
                    current: cur_block_num,
                });
            }
            prev_block_num = Some(cur_block_num);

            for table_rows in dataset_rows {
                if let Some(ref metrics) = self.metrics {
                    let num_rows: u64 = table_rows.rows.num_rows().try_into().unwrap();
                    let table_name = table_rows.table.name();
                    let physical_table = self
                        .catalog
                        .physical_tables()
                        .find(|t| t.table_name() == table_name)
                        .expect("table should exist");
                    let location_id = *physical_table.location_id();
                    // Record rows only (bytes tracked separately in writer)
                    metrics.record_ingestion_rows(num_rows, table_name.to_string(), location_id);
                    // Update latest block gauge
                    metrics.set_latest_block(cur_block_num, table_name.to_string(), location_id);
                }

                writer
                    .write(table_rows)
                    .await
                    .map_err(RunRangeError::Write)?;
            }

            self.progress_tracker.block_covered(cur_block_num);
        }

        // Close the last part file for each table, checking for any errors.
        writer.close().await.map_err(RunRangeError::Close)?;

        Ok(())
    }
}

/// Errors that occur when running a block range dump operation
///
/// This error type is used by `DumpPartition::run_range()`.
#[derive(Debug, thiserror::Error)]
pub enum RunRangeError {
    /// Failed to create the raw dataset writer
    ///
    /// This occurs when initializing the parquet writer for one or more tables fails.
    ///
    /// Possible causes:
    /// - Invalid writer properties configuration
    /// - Schema initialization failure
    /// - Memory allocation failure
    #[error("Failed to create raw dataset writer")]
    CreateWriter(#[source] ParquetError),

    /// Failed to read blocks from the blockchain stream
    ///
    /// This occurs when the block streamer encounters an error while fetching
    /// or streaming blockchain data.
    ///
    /// Possible causes:
    /// - Network connectivity issues with the blockchain provider
    /// - Provider rate limiting or temporary unavailability
    /// - Invalid block range requested
    /// - Provider returned malformed data
    #[error("Failed to read stream")]
    ReadStream(#[source] BlockStreamError),

    /// Block numbers between two consecutive stream items were not in strictly increasing order.
    ///
    /// This indicates a bug in the block streamer implementation, as it should guarantee that blocks
    /// are returned in strictly increasing order.
    ///
    /// NOTE: The two block numbers do not need to be consecutive (e.g. Solana skipped slots), but
    /// they must be strictly increasing.
    #[error(
        "Non-increasing block numbers detected in stream: previous block was {previous}, current block is {current}"
    )]
    NonIncreasingBlockNum {
        previous: BlockNum,
        current: BlockNum,
    },

    /// Failed to write block data to parquet files
    ///
    /// This occurs when writing table rows to the underlying parquet writer fails.
    ///
    /// Possible causes:
    /// - I/O error during write operation
    /// - Insufficient disk space
    /// - Schema mismatch between data and writer
    #[error("Failed to write")]
    Write(#[source] RawDatasetWriterError),

    /// Failed to close and finalize the writer
    ///
    /// This occurs when closing the parquet writer or committing metadata fails.
    ///
    /// Possible causes:
    /// - I/O error during file finalization
    /// - Database error when registering file metadata
    /// - Compaction task failure
    #[error("Failed to close")]
    Close(#[source] RawDatasetWriterCloseError),
}

/// Spawns a background task that tracks freshness metrics for all tables in the catalog.
///
/// The task subscribes to table change notifications and, when a table changes, takes a
/// snapshot to get the `synced_range` timestamp and reports it as the table freshness metric.
fn spawn_freshness_tracker(
    catalog: Catalog,
    multiplexer: Arc<NotificationMultiplexerHandle>,
    metrics: Arc<metrics::MetricsRegistry>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        // Subscribe to all table locations
        let mut subscriptions: BTreeMap<
            LocationId,
            (Arc<PhysicalTable>, tokio::sync::watch::Receiver<()>),
        > = BTreeMap::new();
        for table in catalog.physical_tables() {
            let location_id = table.location_id();
            let receiver = multiplexer.subscribe(location_id).await;
            subscriptions.insert(location_id, (table.clone(), receiver));
        }

        loop {
            // `select_all` panics on an empty iterator. All subscriptions being closed
            // (or an empty initial catalog) is a clean exit condition.
            if subscriptions.is_empty() {
                tracing::debug!("all freshness subscriptions closed; exiting freshness tracker");
                break;
            }

            // Wait for any table to receive a change notification
            let futures = subscriptions.iter_mut().map(|(loc, (table, rx))| {
                let table = table.clone();
                let loc = *loc;
                Box::pin(async move {
                    match rx.changed().await {
                        Ok(()) => Ok((loc, table)),
                        Err(_) => Err(loc),
                    }
                })
            });

            let result = futures::future::select_all(futures).await.0;

            match result {
                Ok((location_id, table)) => {
                    // Take snapshot and get synced range
                    match table.snapshot(false).await {
                        Ok(snapshot) => match snapshot.synced_range() {
                            Ok(Some(range)) => {
                                if let Some(ts) = range.timestamp {
                                    metrics.record_table_freshness(
                                        table.table_name().to_string(),
                                        *location_id,
                                        ts,
                                    );
                                }
                            }
                            Ok(None) => {}
                            Err(err) => {
                                tracing::warn!(
                                    %location_id,
                                    error = %err,
                                    error_source = logging::error_source(&err),
                                    "skipping freshness tracking: table has multi-network segments"
                                );
                            }
                        },
                        Err(err) => {
                            tracing::warn!(
                                %location_id,
                                error = %err,
                                error_source = logging::error_source(&err),
                                "failed to take snapshot for freshness tracking"
                            );
                        }
                    }
                }
                Err(location_id) => {
                    // The watch sender was dropped. Remove this subscription so the next
                    // iteration of select_all does not immediately re-fire on the same
                    // closed receiver, which would create a tight busy-loop.
                    tracing::warn!(%location_id, "freshness subscription ended; removing from tracker");
                    subscriptions.remove(&location_id);
                }
            }
        }
    })
}

#[cfg(test)]
mod test {
    use std::{
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicU64, AtomicUsize},
        },
        time::{Duration, Instant},
    };

    use amp_worker_core::progress::{
        ProgressReporter, ProgressUpdate, SyncCompletedInfo, SyncFailedInfo, SyncStartedInfo,
    };
    use datasets_common::table_name::TableName;

    use super::*;

    /// A mock progress reporter that records all progress updates.
    struct MockProgressReporter {
        updates: Mutex<Vec<ProgressUpdate>>,
    }

    impl MockProgressReporter {
        fn new() -> Self {
            Self {
                updates: Mutex::new(Vec::new()),
            }
        }

        fn get_updates(&self) -> Vec<ProgressUpdate> {
            self.updates.lock().unwrap().clone()
        }
    }

    impl ProgressReporter for MockProgressReporter {
        fn report_progress(&self, update: ProgressUpdate) {
            tracing::info!(
                table = %update.table_name,
                start_block = update.start_block,
                current_block = update.current_block,
                "[PROGRESS] block_covered"
            );
            self.updates.lock().unwrap().push(update);
        }

        fn report_sync_started(&self, _info: SyncStartedInfo) {}
        fn report_sync_completed(&self, _info: SyncCompletedInfo) {}
        fn report_sync_failed(&self, _info: SyncFailedInfo) {}
    }

    #[test]
    fn progress_tracker_emits_on_time_interval() {
        // Initialize logging so we can see the events
        let _ = tracing_subscriber::fmt()
            .with_env_filter("info")
            .with_test_writer()
            .try_init();

        // Given: A ProgressTracker with zero interval (emits on every update)
        let reporter = Arc::new(MockProgressReporter::new());
        let table_name: TableName = "blocks".parse().unwrap();

        let tracker = ProgressTracker {
            last_logged_percent: AtomicUsize::new(0),
            last_emission_time: Mutex::new(Instant::now() - Duration::from_secs(10)),
            last_emitted_block: AtomicU64::new(0),
            has_emitted: AtomicBool::new(false),
            interval: Duration::ZERO, // Zero interval for testing
            progress_reporter: Some(reporter.clone()),
            table_names: vec![table_name.clone()],
            job_start_block: 0,
            job_end_block: Some(99),
            chain_head: 99,
        };

        // When: Process all 100 blocks (block numbers 0-99)
        for block in 0..100u64 {
            tracker.block_covered(block);
        }

        // Then: With zero interval, should emit on every unique block
        let updates = reporter.get_updates();

        // We expect 100 updates (one per block with new progress)
        assert_eq!(
            updates.len(),
            100,
            "Expected 100 progress updates, got {}",
            updates.len()
        );

        // Verify updates have correct metadata
        for update in &updates {
            assert_eq!(update.table_name, table_name);
            assert_eq!(update.start_block, 0);
            assert_eq!(update.end_block, Some(99));
        }

        // Verify first and last blocks
        let first_block = updates.first().unwrap().current_block;
        let last_block = updates.last().unwrap().current_block;
        assert_eq!(first_block, 0, "First update should be at block 0");
        assert_eq!(last_block, 99, "Last update should be at block 99");
    }

    #[test]
    fn progress_tracker_emits_for_multiple_tables() {
        // Given: A ProgressTracker with multiple tables
        let reporter = Arc::new(MockProgressReporter::new());
        let tables: Vec<TableName> = vec![
            "blocks".parse().unwrap(),
            "transactions".parse().unwrap(),
            "logs".parse().unwrap(),
        ];

        let tracker = ProgressTracker {
            last_logged_percent: AtomicUsize::new(0),
            last_emission_time: Mutex::new(Instant::now() - Duration::from_secs(10)),
            last_emitted_block: AtomicU64::new(0),
            has_emitted: AtomicBool::new(false),
            interval: Duration::ZERO, // Zero interval for testing
            progress_reporter: Some(reporter.clone()),
            table_names: tables.clone(),
            job_start_block: 0,
            job_end_block: Some(99),
            chain_head: 99,
        };

        // When: Process 10 blocks
        for block in 0..10 {
            tracker.block_covered(block);
        }

        // Then: Should have 10 updates per table * 3 tables = 30 updates
        let updates = reporter.get_updates();
        assert_eq!(
            updates.len(),
            30, // 10 blocks * 3 tables
            "Expected 30 progress updates (10 blocks * 3 tables), got {}",
            updates.len()
        );

        // Verify each block has updates for all 3 tables
        for block in 0..10u64 {
            let block_updates: Vec<_> = updates
                .iter()
                .filter(|u| u.current_block == block)
                .collect();
            assert_eq!(
                block_updates.len(),
                3,
                "Expected 3 table updates at block {}, got {}",
                block,
                block_updates.len()
            );
        }
    }

    #[test]
    fn progress_tracker_no_reporter_no_updates() {
        // Given: A ProgressTracker with no reporter
        let tracker = ProgressTracker {
            last_logged_percent: AtomicUsize::new(0),
            last_emission_time: Mutex::new(Instant::now() - Duration::from_secs(10)),
            last_emitted_block: AtomicU64::new(0),
            has_emitted: AtomicBool::new(false),
            interval: Duration::ZERO,
            progress_reporter: None,
            table_names: vec!["blocks".parse().unwrap()],
            job_start_block: 0,
            job_end_block: Some(99),
            chain_head: 99,
        };

        // When: Process all 100 blocks
        for block in 0..100 {
            tracker.block_covered(block);
        }

        // Then: No crash, no updates (reporter is None)
        // This just verifies the code handles None gracefully
    }

    #[test]
    fn progress_tracker_large_block_range() {
        // Given: A ProgressTracker with 10,000 blocks and zero interval
        let reporter = Arc::new(MockProgressReporter::new());
        let table_name: TableName = "blocks".parse().unwrap();

        let tracker = ProgressTracker {
            last_logged_percent: AtomicUsize::new(0),
            last_emission_time: Mutex::new(Instant::now() - Duration::from_secs(10)),
            last_emitted_block: AtomicU64::new(0),
            has_emitted: AtomicBool::new(false),
            interval: Duration::ZERO, // Zero interval for testing
            progress_reporter: Some(reporter.clone()),
            table_names: vec![table_name],
            job_start_block: 1000, // Non-zero start block
            job_end_block: Some(10999),
            chain_head: 10999,
        };

        // When: Process all 10,000 blocks
        for block in 1000..11000 {
            tracker.block_covered(block);
        }

        // Then: Should have 10,000 progress updates (one per block with zero interval)
        let updates = reporter.get_updates();
        assert_eq!(
            updates.len(),
            10000,
            "Expected 10000 progress updates (one per block), got {}",
            updates.len()
        );

        // Verify job_start_block and job_end_block are preserved in updates
        for update in &updates {
            assert_eq!(update.start_block, 1000);
            assert_eq!(update.end_block, Some(10999));
        }
    }

    #[test]
    fn progress_tracker_throttles_with_non_zero_interval() {
        // Given: A ProgressTracker with a 1 hour interval (won't elapse during test)
        let reporter = Arc::new(MockProgressReporter::new());
        let table_name: TableName = "blocks".parse().unwrap();

        let tracker = ProgressTracker {
            last_logged_percent: AtomicUsize::new(0),
            last_emission_time: Mutex::new(Instant::now()), // Just now
            last_emitted_block: AtomicU64::new(0),
            has_emitted: AtomicBool::new(false),
            interval: Duration::from_secs(3600), // 1 hour - won't elapse
            progress_reporter: Some(reporter.clone()),
            table_names: vec![table_name],
            job_start_block: 0,
            job_end_block: Some(99),
            chain_head: 99,
        };

        // When: Process all 100 blocks
        for block in 0..100 {
            tracker.block_covered(block);
        }

        // Then: Should have NO progress updates because interval hasn't elapsed
        let updates = reporter.get_updates();
        assert_eq!(
            updates.len(),
            0,
            "Expected 0 progress updates (interval not elapsed), got {}",
            updates.len()
        );
    }

    #[test]
    fn split_and_partition() {
        assert_eq!(
            super::split_and_partition(vec![0..=10], 2, 4),
            vec![vec![0..=5], vec![6..=10]],
        );
        assert_eq!(
            super::split_and_partition(vec![1..=5, 10..=15, 20..=25], 1, 5),
            vec![vec![1..=5, 10..=15, 20..=25]],
        );
        assert_eq!(
            super::split_and_partition(vec![1..=10, 11..=20, 21..=30], 3, 5),
            vec![vec![1..=10], vec![11..=20], vec![21..=30]],
        );
        assert_eq!(
            super::split_and_partition(vec![1..=30], 3, 5),
            vec![vec![1..=10], vec![11..=20], vec![21..=30]],
        );
        assert_eq!(
            super::split_and_partition(vec![0..=9, 20..=29], 4, 5),
            vec![vec![0..=4], vec![5..=9], vec![20..=24], vec![25..=29]],
        );
        assert_eq!(
            super::split_and_partition(vec![1..=5, 6..=10, 11..=15, 16..=20], 4, 10),
            vec![vec![1..=5, 6..=10], vec![11..=15, 16..=20]],
        );
        assert_eq!(
            super::split_and_partition(vec![1..=100], 2, 10),
            vec![vec![1..=50], vec![51..=100]],
        );
        assert_eq!(
            super::split_and_partition(vec![1..=5, 11..=20], 5, 10),
            vec![vec![1..=5, 11..=15], vec![16..=20]],
        );
        assert_eq!(
            super::split_and_partition(vec![1..=100], 4, 10),
            vec![[1..=25], [26..=50], [51..=75], [76..=100]],
        );
    }

    /// Regression: closed watch subscriptions must be removed from the subscription map so
    /// `select_all` does not re-fire immediately on the same closed receiver, creating a
    /// CPU-spinning busy-loop.
    ///
    /// The fix in `spawn_freshness_tracker` removes the closed subscription and exits when the
    /// map is empty. This test exercises the identical `select_all` + BTreeMap removal pattern
    /// used in production, verifying that the loop terminates instead of spinning.
    #[tokio::test]
    async fn freshness_tracker_loop_exits_when_all_subscriptions_close() {
        use std::collections::BTreeMap;

        use tokio::sync::watch;

        // Given: two watch channels whose senders are dropped immediately (simulates
        // the notification multiplexer shutting down or being dropped).
        let (tx1, rx1) = watch::channel::<()>(());
        let (tx2, rx2) = watch::channel::<()>(());
        drop(tx1);
        drop(tx2);

        let mut subscriptions: BTreeMap<u64, watch::Receiver<()>> = BTreeMap::new();
        subscriptions.insert(1, rx1);
        subscriptions.insert(2, rx2);

        // When: running the same select_all + removal pattern as spawn_freshness_tracker.
        // With the fix both closed receivers are removed one-by-one, then the empty-map
        // guard exits the loop. Without the fix, the loop would spin indefinitely.
        let completed = tokio::time::timeout(std::time::Duration::from_millis(500), async move {
            loop {
                // Empty-map guard: avoids select_all panic and exits when all channels close.
                if subscriptions.is_empty() {
                    break;
                }

                let futures = subscriptions.iter_mut().map(|(&key, rx)| {
                    Box::pin(async move {
                        match rx.changed().await {
                            Ok(()) => Ok(key),
                            Err(_) => Err(key),
                        }
                    })
                });

                // Bind to a `let` first so the remaining futures (which hold mutable
                // references into `subscriptions`) are dropped before the `match` arm
                // calls `subscriptions.remove()`. Same pattern as production code.
                let result = futures::future::select_all(futures).await.0;
                match result {
                    Ok(_) => {}
                    Err(key) => {
                        subscriptions.remove(&key);
                    }
                }
            }
        })
        .await;

        // Then: the loop exits within the timeout — no busy-loop
        assert!(
            completed.is_ok(),
            "freshness tracker loop must exit when all subscriptions close"
        );
    }
}
