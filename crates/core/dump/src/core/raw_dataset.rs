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
        Arc, RwLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use common::{
    BlockNum, BlockStreamer, BoxError,
    catalog::physical::{Catalog, PhysicalTable},
    metadata::segments::merge_ranges,
};
use datasets_common::table_name::TableName;
use futures::TryStreamExt as _;
use metadata_db::MetadataDb;
use tracing::{Instrument, instrument};

use super::{Ctx, EndBlock, ResolvedEndBlock, tasks::FailFastJoinSet};
use crate::{WriterProperties, metrics, raw_dataset_writer::RawDatasetWriter};

/// Dumps a raw dataset by extracting blockchain data from specified block ranges
/// and writing it to partitioned Parquet files.
///
/// Returns `Ok(())` on successful completion or an error if any partition fails.
/// On failure, all running partitions are terminated to prevent partial dumps.
#[instrument(skip_all, err)]
#[expect(clippy::too_many_arguments)]
pub async fn dump(
    ctx: Ctx,
    max_writers: u16,
    catalog: Catalog,
    tables: &[Arc<PhysicalTable>],
    parquet_opts: Arc<WriterProperties>,
    end: EndBlock,
    metrics: Option<Arc<metrics::MetricsRegistry>>,
    finalized_blocks_only: bool,
) -> Result<(), BoxError> {
    let dump_start_time = Instant::now();
    let dataset = tables[0].dataset();

    let mut client = ctx
        .dataset_store
        .get_client(dataset.manifest_hash(), metrics.as_ref().map(|m| m.meter()))
        .await?;

    let provider_name = client.provider_name().to_string();
    tracing::info!("connected to provider: {provider_name}");

    let start = dataset.start_block.unwrap_or(0);
    let resolved = end
        .resolve(start, client.latest_block(finalized_blocks_only))
        .await?;

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
    loop {
        timer.tick().await;

        let Some(latest_block) = client.latest_block(finalized_blocks_only).await? else {
            // No data to dump
            continue;
        };
        if latest_block < start {
            continue;
        }

        let mut missing_ranges_by_table: BTreeMap<TableName, Vec<RangeInclusive<BlockNum>>> =
            Default::default();
        for table in tables {
            let end = match end {
                None => latest_block,
                Some(end) => BlockNum::min(end, latest_block),
            };
            let missing_ranges = table.missing_ranges(start..=end).await?;
            missing_ranges_by_table.insert(table.table().name().clone(), missing_ranges);
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
        if missing_dataset_ranges.is_empty() {
            if let Some(end) = end
                && end <= latest_block
            {
                tracing::info!("no blocks to dump");
                return Ok(());
            }
            continue;
        }
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

        let total_blocks_to_cover = missing_dataset_ranges
            .iter()
            .flatten()
            .map(|r| r.clone().count())
            .sum::<usize>();

        let progress_reporter = Arc::new(ProgressReporter {
            overall_blocks_covered: AtomicUsize::new(0),
            total_blocks_to_cover,
            last_log_time: RwLock::new(Instant::now()),
        });

        let writers = missing_dataset_ranges
            .into_iter()
            .enumerate()
            .map(|(i, ranges)| DumpPartition {
                block_streamer: client.clone(),
                metadata_db: ctx.metadata_db.clone(),
                catalog: catalog.clone(),
                ranges,
                parquet_opts: parquet_opts.clone(),
                missing_ranges_by_table: missing_ranges_by_table.clone(),
                id: i as u32,
                metrics: metrics.clone(),
                progress_reporter: progress_reporter.clone(),
            });

        // Spawn the writers, starting them with a 1 second delay between each.
        // Note that tasks spawned in the join set start executing immediately in parallel
        let mut join_set = FailFastJoinSet::<Result<(), BoxError>>::new();
        for writer in writers {
            let span = tracing::info_span!("dump_partition", partition_id = writer.id);
            join_set.spawn(writer.run().instrument(span));
        }

        // Wait for all the writers to finish, returning an error if any writer panics or fails.
        if let Err(err) = join_set.try_wait_all().await {
            tracing::error!(error=%err, "dataset dump failed");

            // Record error metrics
            if let Some(ref metrics) = metrics {
                for table in tables {
                    let table_name = table.table_name().to_string();
                    metrics.record_dump_error(table_name);
                }
            }

            return Err(err.into_box_error());
        }

        if let Some(end) = end
            && latest_block >= end
        {
            // Record dump duration on successful completion
            if let Some(ref metrics) = metrics {
                let duration_millis = dump_start_time.elapsed().as_millis() as f64;
                for table in tables {
                    let table_name = table.table_name().to_string();
                    let job_id = table_name.clone();
                    metrics.record_dump_duration(duration_millis, table_name, job_id);
                }
            }
            return Ok(());
        }
    }
}

struct ProgressReporter {
    overall_blocks_covered: AtomicUsize,
    total_blocks_to_cover: usize,
    last_log_time: RwLock<Instant>,
}

impl ProgressReporter {
    /// Signal to the progress reporter that another block has been covered.
    fn block_covered(&self) {
        let overall_blocks_covered = self.overall_blocks_covered.fetch_add(1, Ordering::SeqCst) + 1;
        let now = Instant::now();
        let last_log_time = *self.last_log_time.read().unwrap();
        if now.duration_since(last_log_time) >= Duration::from_secs(15) {
            let percent_covered =
                (overall_blocks_covered as f64 / self.total_blocks_to_cover as f64) * 100.0;
            tracing::info!(
                "overall progress: {overall_blocks_covered}/{} blocks ({percent_covered:.2}%)",
                self.total_blocks_to_cover
            );
            *self.last_log_time.write().unwrap() = now;
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
    /// The tables to write to
    catalog: Catalog,
    /// The block ranges to scan
    ranges: Vec<RangeInclusive<BlockNum>>,
    /// The Parquet writer properties
    parquet_opts: Arc<WriterProperties>,
    /// The missing block ranges by table
    missing_ranges_by_table: BTreeMap<TableName, Vec<RangeInclusive<BlockNum>>>,
    /// The partition ID
    id: u32,
    /// Metrics registry
    metrics: Option<Arc<metrics::MetricsRegistry>>,
    /// A progress reporter which logs the overall progress of all partitions.
    progress_reporter: Arc<ProgressReporter>,
}
impl<S: BlockStreamer> DumpPartition<S> {
    /// Consumes the instance returning a future that runs the partition, processing all assigned block ranges sequentially.
    async fn run(self) -> Result<(), BoxError> {
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

    async fn run_range(&self, range: RangeInclusive<BlockNum>) -> Result<(), BoxError> {
        // Get dataset name for instrumentation
        let dataset = self.catalog.tables()[0]
            .job_labels()
            .dataset_name
            .to_string();

        // Track records extracted
        let records_extracted = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

        // Create task type for instrumentation
        let task_type = monitoring::TaskType::Dump {
            dataset: dataset.clone(),
            block_range: (*range.start(), *range.end()),
            records_extracted: None, // Will be set later
        };

        let instrumentation = monitoring::InstrumentedTaskExecution::new(task_type);
        let records_counter = records_extracted.clone();

        let dump_fut = async move {
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
                self.parquet_opts.clone(),
                missing_ranges_by_table,
                self.metrics.clone(),
            )?;

            let mut stream = std::pin::pin!(stream);
            while let Some(dataset_rows) = stream.try_next().await? {
                for table_rows in dataset_rows {
                    let num_rows: u64 = table_rows.rows.num_rows().try_into().unwrap();
                    records_counter
                        .fetch_add(num_rows as usize, std::sync::atomic::Ordering::Relaxed);

                    if let Some(ref metrics) = self.metrics {
                        let table_name = table_rows.table.name();
                        let block_num = table_rows.block_num();
                        let physical_table = self
                            .catalog
                            .tables()
                            .iter()
                            .find(|t| t.table_name() == table_name)
                            .expect("table should exist");
                        let location_id = *physical_table.location_id();
                        // Record rows only (bytes tracked separately in writer)
                        metrics.record_ingestion_rows(
                            num_rows,
                            table_name.to_string(),
                            location_id,
                        );
                        // Update latest block gauge
                        metrics.set_latest_block(block_num, table_name.to_string(), location_id);
                    }

                    writer.write(table_rows).await?;
                }

                self.progress_reporter.block_covered();
            }

            // Close the last part file for each table, checking for any errors.
            writer.close().await?;

            Ok(())
        };

        // Execute with instrumentation
        let (result, _metrics) = instrumentation.execute(dump_fut).await;
        result
    }
}

#[cfg(test)]
mod test {
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
}
