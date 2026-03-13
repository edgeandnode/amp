//! Internal helpers for the raw dataset materialize: partition scheduling, block streaming,
//! progress tracking, and freshness monitoring.

use std::{
    collections::BTreeMap,
    num::NonZeroU64,
    ops::RangeInclusive,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use amp_data_store::DataStore;
use amp_worker_core::{
    WriterProperties,
    compaction::AmpCompactor,
    error_detail::ErrorDetailsProvider,
    metrics,
    progress::{ProgressReporter, ProgressUpdate},
    retryable::RetryableErrorExt,
    tasks::{FailFastJoinSet, TryWaitAllError},
};
use common::{
    BlockNum,
    catalog::physical::Catalog,
    parquet::errors::ParquetError,
    physical_table::{PhysicalTable, segments::merge_ranges},
};
use datasets_common::{dataset::Table as _, table_name::TableName};
use datasets_raw::{
    client::{BlockStreamError, BlockStreamer},
    rows::Rows,
};
use futures::TryStreamExt as _;
use metadata_db::{MetadataDb, NotificationMultiplexerHandle, physical_table_revision::LocationId};
use monitoring::logging;
use tokio::task::JoinHandle;
use tracing::Instrument as _;

use super::writer::{RawDatasetWriter, RawDatasetWriterCloseError, RawDatasetWriterError};
use crate::job_ctx::Context;

/// Materializes block ranges by partitioning them across multiple parallel workers.
#[tracing::instrument(skip_all, err)]
#[expect(clippy::too_many_arguments)]
pub(super) async fn materialize_ranges<S: BlockStreamer + Send + Sync>(
    missing_dataset_ranges: Vec<RangeInclusive<BlockNum>>,
    max_writers: u16,
    client: &S,
    ctx: &Context,
    catalog: &Catalog,
    parquet_opts: Arc<WriterProperties>,
    missing_ranges_by_table: BTreeMap<TableName, Vec<RangeInclusive<BlockNum>>>,
    compactors_by_table: BTreeMap<TableName, Arc<AmpCompactor>>,
    tables: &[(Arc<PhysicalTable>, Arc<AmpCompactor>)],
    // The job's actual start block (from dataset definition), used for progress events
    job_start_block: BlockNum,
    // The job's actual end block (resolved), used for progress events. None for continuous mode.
    job_end_block: Option<BlockNum>,
    // Current chain head block, used for percentage calculation in continuous mode
    chain_head: BlockNum,
    // Enable cryptographic verification of EVM block data
    verify: bool,
) -> Result<(), TryWaitAllError<RunRangeError>> {
    tracing::info!(
        "materializing ranges {}",
        missing_dataset_ranges
            .iter()
            .map(|r| format!("[{}-{}]", r.start(), r.end()))
            .collect::<Vec<String>>()
            .join(", ")
    );

    // Split them across the target number of writers as to balance the number of blocks per writer.
    let missing_dataset_ranges = if let Some(bucket_size) = client.bucket_size() {
        split_and_partition_bucketed(
            missing_dataset_ranges,
            max_writers as u64,
            2000,
            bucket_size,
        )
    } else {
        split_and_partition(missing_dataset_ranges, max_writers as u64, 2000)
    };

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
        progress_reporter: ctx.progress_reporter.clone(),
        table_names,
        job_start_block,
        job_end_block,
        chain_head,
    });

    let writers = missing_dataset_ranges
        .into_iter()
        .enumerate()
        .map(|(i, ranges)| MaterializePartition {
            block_streamer: client.clone(),
            metadata_db: ctx.metadata_db.clone(),
            data_store: ctx.data_store.clone(),
            catalog: catalog.clone(),
            ranges,
            parquet_opts: parquet_opts.clone(),
            missing_ranges_by_table: missing_ranges_by_table.clone(),
            compactors_by_table: compactors_by_table.clone(),
            id: i as u32,
            metrics: ctx.metrics.clone(),
            progress_tracker: progress_tracker.clone(),
            verify,
        });

    // Spawn the writers, starting them with a 1-second delay between each.
    // Note that tasks spawned in the join set start executing immediately in parallel
    let mut join_set = FailFastJoinSet::<Result<(), RunRangeError>>::new();
    for writer in writers {
        let span = tracing::info_span!("materialize_partition", partition_id = writer.id);
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
                    "dataset materialize failed: partition task error"
                );
            }
            TryWaitAllError::Panic(err) => {
                tracing::error!(error=%err, "dataset materialize failed: partition task panicked");
            }
        }

        // Record error metrics
        if let Some(ref metrics) = ctx.metrics {
            for (table, _) in tables {
                let table_name = table.table_name().to_string();
                metrics.record_dump_error(table_name);
            }
        }

        return Err(err);
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
            .all(|p| total_blocks(p) >= min_partition_blocks),
        "Partition contains fewer blocks than min_partition_blocks"
    );
    if !current_partition.is_empty() {
        partitions.push(current_partition);
    }
    assert!(
        partitions.len() <= n as usize,
        "Number of partitions exceeds requested maximum"
    );
    partitions
}

/// Partition ranges like [split_and_partition], while making sure not to split `bucket_size`
/// buckets across partitions. This is important for some clients (e.g. Solana) which require that
/// all blocks in a bucket are fetched by the same client instance.
///
/// Block ranges are first split at bucket boundaries (buckets are `[0..N-1]`, `[N..2N-1]`,
/// `[2N..3N-1]`, etc.) and the partitioning ensures that no bucket is split across multiple
/// partitions. Multiple buckets may share a partition to stay within the `n` partition limit.
fn split_and_partition_bucketed(
    ranges: Vec<RangeInclusive<BlockNum>>,
    n: u64,
    min_partition_blocks: u64,
    bucket_size: NonZeroU64,
) -> Vec<Vec<RangeInclusive<BlockNum>>> {
    if ranges.is_empty() {
        return vec![];
    }

    let bucket_size = bucket_size.get();
    let target_partition_blocks = total_blocks(&ranges).div_ceil(n).max(min_partition_blocks);

    // Phase 1: Split ranges at bucket boundaries.
    let mut split_ranges = Vec::new();
    for range in ranges {
        let (mut start, end) = range.into_inner();
        while start <= end {
            debug_assert!(
                start < u64::MAX - bucket_size,
                "overflow in bucketed partition, start: {start}, bucket_size: {bucket_size}"
            );
            let this_bucket_end = ((start / bucket_size) + 1) * bucket_size - 1;
            let this_end = this_bucket_end.min(end);
            split_ranges.push(start..=this_end);
            start = this_end + 1;
        }
    }

    // Phase 2: Partition buckets into at most n partitions, keeping buckets together.
    let mut partitions: Vec<Vec<RangeInclusive<BlockNum>>> = Vec::new();
    let mut current_partition: Vec<RangeInclusive<BlockNum>> = Vec::new();
    let mut current_partition_blocks = 0u64;

    let mut ranges_iter = split_ranges.into_iter().peekable();
    while let Some(range) = ranges_iter.next() {
        let range_end = *range.end();
        let len = range_blocks(&range);
        current_partition.push(range);
        current_partition_blocks += len;

        if current_partition_blocks >= target_partition_blocks {
            let next_range_is_in_same_bucket = ranges_iter.peek().is_some_and(|next_range| {
                let next_range_bucket = *next_range.start() / bucket_size;
                let current_range_bucket = range_end / bucket_size;
                next_range_bucket == current_range_bucket
            });

            if !next_range_is_in_same_bucket {
                partitions.push(std::mem::take(&mut current_partition));
                current_partition_blocks = 0;
            }
        }
    }

    assert!(
        partitions
            .iter()
            .all(|p| total_blocks(p) >= min_partition_blocks),
        "Partition contains fewer blocks than min_partition_blocks"
    );

    if !current_partition.is_empty() {
        partitions.push(current_partition);
    }

    assert!(
        partitions.len() <= n as usize,
        "Number of partitions exceeds requested maximum"
    );

    // Phase 3: Merge adjacent ranges within each partition (since splitting at bucket
    // boundaries may have created adjacent ranges).
    partitions.into_iter().map(merge_ranges).collect()
}

fn range_blocks(r: &RangeInclusive<BlockNum>) -> u64 {
    (*r.end() - *r.start()) + 1
}
fn total_blocks(ranges: &[RangeInclusive<BlockNum>]) -> u64 {
    ranges.iter().map(range_blocks).sum()
}

/// A partition of a raw dataset materialize job that processes a subset of block ranges.
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
struct MaterializePartition<S: BlockStreamer> {
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
    /// Enable cryptographic verification of EVM block data
    verify: bool,
}
impl<S: BlockStreamer> MaterializePartition<S> {
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

            self.run_range(range.clone())
                .await
                .map_err(|err| err.with_block_range(*range.start(), *range.end()))?;

            tracing::info!(
                "finished scan for range [{}-{}] in {} minutes",
                range.start(),
                range.end(),
                start_time.elapsed().as_secs() / 60
            );
        }
        Ok(())
    }

    #[tracing::instrument(
        skip_all,
        err,
        fields(
            start_block = %range.start(),
            end_block = %range.end(),
        )
    )]
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
        .map_err(RunRangeError::create_writer)?;

        let mut stream = std::pin::pin!(stream);
        let mut prev_block_num = None;
        while let Some(dataset_rows) = async {
            stream
                .try_next()
                .await
                .map_err(RunRangeError::read_stream)
        }
        .instrument(tracing::info_span!("fetch_block"))
        .await?
        {
            let cur_block_num = dataset_rows.block_num();
            if let Some(prev) = prev_block_num
                && cur_block_num <= prev
            {
                return Err(RunRangeError::non_increasing_block_num(prev, cur_block_num));
            }
            prev_block_num = Some(cur_block_num);

            self.process_block(cur_block_num, dataset_rows, &mut writer)
                .await?;
        }

        // Close the last part file for each table, checking for any errors.
        writer.close().await.map_err(RunRangeError::close)?;

        Ok(())
    }

    #[tracing::instrument(skip_all, err, fields(block_num = %block_num))]
    async fn process_block(
        &self,
        block_num: BlockNum,
        dataset_rows: Rows,
        writer: &mut RawDatasetWriter,
    ) -> Result<(), RunRangeError> {
        // Verify block data if verification is enabled
        if self.verify {
            self.verify_block_data(&dataset_rows)?;
        }

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
                metrics.set_latest_block(block_num, table_name.to_string(), location_id);
            }

            writer
                .write(table_rows)
                .await
                .map_err(RunRangeError::write)?;
        }

        self.progress_tracker.block_covered(block_num);
        Ok(())
    }

    /// Verify block data using cryptographic verification.
    ///
    /// Expects the dataset to contain blocks, transactions, and logs tables.
    /// Converts the Arrow RecordBatches to verification types and performs:
    /// - Block hash verification
    /// - Transaction root verification
    /// - Receipt root verification
    #[expect(clippy::result_large_err)]
    fn verify_block_data(&self, dataset_rows: &Rows) -> Result<(), RunRangeError> {
        // Collect the RecordBatches for blocks, transactions, and logs
        let mut block_batch = None;
        let mut transactions_batch = None;
        let mut logs_batch = None;

        for table_rows in dataset_rows {
            let table_name = table_rows.table.name().to_string();
            match table_name.as_str() {
                "blocks" => block_batch = Some(&table_rows.rows),
                "transactions" => transactions_batch = Some(&table_rows.rows),
                "logs" => logs_batch = Some(&table_rows.rows),
                _ => {} // Ignore other tables
            }
        }

        // Ensure all required tables are present
        let block_batch = block_batch.ok_or_else(|| {
            RunRangeError::verification_failed(verification::VerificationError::MissingTable(
                "blocks",
            ))
        })?;
        let transactions_batch = transactions_batch.ok_or_else(|| {
            RunRangeError::verification_failed(verification::VerificationError::MissingTable(
                "transactions",
            ))
        })?;
        let logs_batch = logs_batch.ok_or_else(|| {
            RunRangeError::verification_failed(verification::VerificationError::MissingTable(
                "logs",
            ))
        })?;

        // Convert RecordBatches to verification Block
        let block = verification::client::block_from_record_batches(
            block_batch,
            transactions_batch,
            logs_batch,
        )
        .map_err(|err| RunRangeError::verification_failed(err.into()))?;

        // Run verification
        verification::verify_block(&block).map_err(RunRangeError::verification_failed)?;

        Ok(())
    }
}

/// Errors that occur when running a block range materialize operation.
///
/// This is a wrapper struct around [`RunRangeErrorKind`] that optionally carries
/// the block range context of the failed segment. The block range is attached
/// via [`with_block_range`](Self::with_block_range) and surfaces as structured
/// JSON fields through [`ErrorDetailsProvider`].
#[derive(Debug)]
pub struct RunRangeError {
    kind: RunRangeErrorKind,
    block_range: Option<(BlockNum, BlockNum)>,
}

impl RunRangeError {
    /// Attach block range context to this error.
    fn with_block_range(mut self, start: BlockNum, end: BlockNum) -> Self {
        self.block_range = Some((start, end));
        self
    }

    pub(super) fn create_writer(err: ParquetError) -> Self {
        Self {
            kind: RunRangeErrorKind::CreateWriter(err),
            block_range: None,
        }
    }

    pub(super) fn read_stream(err: BlockStreamError) -> Self {
        Self {
            kind: RunRangeErrorKind::ReadStream(err),
            block_range: None,
        }
    }

    pub(super) fn non_increasing_block_num(previous: BlockNum, current: BlockNum) -> Self {
        Self {
            kind: RunRangeErrorKind::NonIncreasingBlockNum { previous, current },
            block_range: None,
        }
    }

    pub(super) fn write(err: RawDatasetWriterError) -> Self {
        Self {
            kind: RunRangeErrorKind::Write(err),
            block_range: None,
        }
    }

    pub(super) fn close(err: RawDatasetWriterCloseError) -> Self {
        Self {
            kind: RunRangeErrorKind::Close(err),
            block_range: None,
        }
    }

    pub(super) fn verification_failed(err: verification::VerificationError) -> Self {
        Self {
            kind: RunRangeErrorKind::VerificationFailed(err),
            block_range: None,
        }
    }
}

impl std::fmt::Display for RunRangeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.kind.fmt(f)
    }
}

impl std::error::Error for RunRangeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.kind.source()
    }
}

impl RetryableErrorExt for RunRangeError {
    fn is_retryable(&self) -> bool {
        self.kind.is_retryable()
    }
}

impl ErrorDetailsProvider for RunRangeError {
    fn error_details(&self) -> serde_json::Map<String, serde_json::Value> {
        let (start, end) = match self.block_range {
            Some((s, e)) => (Some(s), Some(e)),
            None => (None, None),
        };
        amp_worker_core::error_detail::block_range_details(start, end)
    }
}

/// The specific error variants for [`RunRangeError`].
#[derive(Debug, thiserror::Error)]
enum RunRangeErrorKind {
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

    /// Block verification failed
    ///
    /// This occurs when cryptographic verification of block data fails during extraction.
    /// The block hash, transaction root, or receipt root does not match the computed value
    /// from the extracted data.
    ///
    /// Possible causes:
    /// - Data corruption during extraction
    /// - Provider returned invalid data
    /// - Missing required tables (blocks, transactions, logs)
    /// - Schema mismatch between extraction and verification expectations
    ///
    /// This error is retryable as the issue may be transient.
    #[error("Verification failed")]
    VerificationFailed(#[source] verification::VerificationError),
}

impl RetryableErrorExt for RunRangeErrorKind {
    fn is_retryable(&self) -> bool {
        match self {
            Self::CreateWriter(_) => true,
            Self::ReadStream(BlockStreamError::Fatal(_)) => false,
            Self::ReadStream(BlockStreamError::Recoverable(_)) => true,
            Self::NonIncreasingBlockNum { .. } => false,
            Self::Write(err) => err.is_retryable(),
            Self::Close(err) => err.is_retryable(),
            Self::VerificationFailed(_) => true,
        }
    }
}

/// Spawns a background task that tracks freshness metrics for all tables in the catalog.
///
/// The task subscribes to table change notifications and, when a table changes, takes a
/// snapshot to get the `synced_range` timestamp and reports it as the table freshness metric.
pub(super) fn spawn_freshness_tracker(
    catalog: Catalog,
    multiplexer: Arc<NotificationMultiplexerHandle>,
    metrics: Arc<metrics::MetricsRegistry>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        // Subscribe to all table locations
        let mut subscriptions: BTreeMap<
            LocationId,
            (Arc<PhysicalTable>, tokio::sync::watch::Receiver<()>),
        > = Default::default();
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
    use rstest::rstest;

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

    #[rstest]
    #[case::empty_input(
        vec![], // input
        4, // n
        1, // min_partition_blocks
        NonZeroU64::new(10).unwrap(), // bucket_size
        vec![] // expected
    )]
    #[case::one_range_one_bucket(
        vec![0..=5], // input       
        4, // n
        1, // min_partition_blocks
        NonZeroU64::new(10).unwrap(), // bucket_size
        vec![vec![0..=5]] // expected
    )]
    #[case::two_ranges_one_bucket(
        vec![1..=4, 6..=9], // input
        4, // n
        1, // min_partition_blocks
        NonZeroU64::new(10).unwrap(), // bucket_size
        vec![vec![1..=4, 6..=9]] // expected
    )]
    #[case::one_range_three_buckets(
        vec![0..=39], // input
        2, // n
        1, // min_partition_blocks
        NonZeroU64::new(10).unwrap(), // bucket_size
        vec![vec![0..=19], vec![20..=39]] // expected
    )]
    #[case::range_splits_bucket_is_not_one_partition(
        vec![8..=12], // input
        4, // n
        1, // min_partition_blocks
        NonZeroU64::new(10).unwrap(), // bucket_size
        vec![vec![8..=9], vec![10..=12]] // expected
    )]
    #[case::two_ranges_already_aligned(
        vec![0..=9, 10..=19], // input
        4, // n
        1, // min_partition_blocks
        NonZeroU64::new(10).unwrap(), // bucket_size
        vec![vec![0..=9], vec![10..=19]] // expected
    )]
    #[case::min_partition_blocks_greater_than_bucket_size(
        vec![0..=5], // input
        4, // n
        10, // min_partition_blocks
        NonZeroU64::new(3).unwrap(), // bucket_size
        vec![vec![0..=5]] // expected
    )]
    #[case::single_partition_all_buckets_merged(
        vec![0..=9, 10..=19, 20..=29], // input
        1, // n
        1, // min_partition_blocks
        NonZeroU64::new(10).unwrap(), // bucket_size
        vec![vec![0..=29]] // expected
    )]
    #[case::non_contiguous_ranges_with_gaps(
        vec![5..=15, 25..=35], // input
        2, // n
        1, // min_partition_blocks
        NonZeroU64::new(10).unwrap(), // bucket_size
        vec![vec![5..=15], vec![25..=35]] // expected
    )]
    #[case::n_greater_than_buckets(
        vec![0..=9, 10..=19], // input
        10, // n
        1, // min_partition_blocks
        NonZeroU64::new(10).unwrap(), // bucket_size
        vec![vec![0..=9], vec![10..=19]] // expected
    )]
    #[case::adjacent_ranges_should_merge(
        vec![0..=9, 10..=19], // input
        2, // n
        1, // min_partition_blocks
        NonZeroU64::new(20).unwrap(), // bucket_size
        vec![vec![0..=19]] // expected
    )]
    fn split_and_partition_bucketed(
        #[case] input: Vec<std::ops::RangeInclusive<u64>>,
        #[case] n: u64,
        #[case] min_partition_blocks: u64,
        #[case] bucket_size: NonZeroU64,
        #[case] expected: Vec<Vec<std::ops::RangeInclusive<u64>>>,
    ) {
        assert_eq!(
            super::split_and_partition_bucketed(input, n, min_partition_blocks, bucket_size),
            expected
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

    mod run_range_error {
        use amp_worker_core::error_detail::ErrorDetailsProvider;
        use common::parquet::errors::ParquetError;

        use super::super::RunRangeError;

        #[test]
        fn error_details_with_block_range_returns_start_and_end() {
            //* Given
            let err = RunRangeError::create_writer(ParquetError::General("test".into()))
                .with_block_range(100, 200);

            //* When
            let details = err.error_details();

            //* Then
            assert_eq!(details.len(), 2, "should contain exactly two entries");
            assert_eq!(
                details["block_range_start"],
                serde_json::json!(100),
                "block_range_start should match"
            );
            assert_eq!(
                details["block_range_end"],
                serde_json::json!(200),
                "block_range_end should match"
            );
        }

        #[test]
        fn error_details_without_block_range_returns_empty() {
            //* Given
            let err = RunRangeError::create_writer(ParquetError::General("test".into()));

            //* When
            let details = err.error_details();

            //* Then
            assert!(
                details.is_empty(),
                "details should be empty without block range"
            );
        }

        #[test]
        fn display_without_block_range_delegates_to_kind() {
            //* Given
            let err = RunRangeError::create_writer(ParquetError::General("test".into()));

            //* When
            let msg = err.to_string();

            //* Then
            assert_eq!(
                msg, "Failed to create raw dataset writer",
                "should delegate to kind Display"
            );
        }

        #[test]
        fn display_with_block_range_delegates_to_kind() {
            //* Given
            let err = RunRangeError::create_writer(ParquetError::General("test".into()))
                .with_block_range(100, 200);

            //* When
            let msg = err.to_string();

            //* Then
            assert_eq!(
                msg, "Failed to create raw dataset writer",
                "block range should not affect Display output"
            );
        }

        #[test]
        fn source_with_inner_error_returns_inner() {
            use std::error::Error;

            //* Given
            let err = RunRangeError::create_writer(ParquetError::General("inner".into()));

            //* When
            let source = err.source();

            //* Then
            let source = source.expect("should have a source error");
            assert_eq!(
                source.to_string(),
                "Parquet error: inner",
                "source should be the inner error"
            );
        }

        #[test]
        fn is_retryable_with_retryable_kind_returns_true() {
            use amp_worker_core::retryable::RetryableErrorExt;

            //* Given
            let err = RunRangeError::create_writer(ParquetError::General("test".into()));

            //* When
            let retryable = err.is_retryable();

            //* Then
            assert!(retryable, "CreateWriter errors should be retryable");
        }

        #[test]
        fn is_retryable_with_fatal_kind_returns_false() {
            use amp_worker_core::retryable::RetryableErrorExt;

            //* Given
            let err = RunRangeError::non_increasing_block_num(10, 5);

            //* When
            let retryable = err.is_retryable();

            //* Then
            assert!(
                !retryable,
                "NonIncreasingBlockNum errors should not be retryable"
            );
        }
    }
}
