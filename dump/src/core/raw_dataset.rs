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
    sync::Arc,
    time::{Duration, Instant},
};

use common::{
    multirange::MultiRange, query_context::QueryContext, BlockNum, BlockStreamer, BoxError,
};
use futures::TryStreamExt as _;
use metadata_db::MetadataDb;
use tokio::sync::Mutex;
use tracing::instrument;

use super::{block_ranges, tasks::FailFastJoinSet, Ctx};
use crate::parquet_writer::{ParquetWriterProperties, RawDatasetWriter};

/// Dumps a raw dataset by extracting blockchain data from specified block ranges
/// and writing it to partitioned Parquet files.
///
/// Returns `Ok(())` on successful completion or an error if any partition fails.
/// On failure, all running partitions are terminated to prevent partial dumps.
#[instrument(skip_all, fields(dataset = %dataset_name), err)]
pub async fn dump(
    ctx: Ctx,
    n_jobs: u16,
    query_ctx: Arc<QueryContext>,
    dataset_name: &str,
    block_ranges_by_table: BTreeMap<String, MultiRange>,
    partition_size: u64,
    parquet_opts: &ParquetWriterProperties,
    (start, end): (i64, Option<i64>),
) -> Result<(), BoxError> {
    let mut client = ctx.dataset_store.load_client(dataset_name).await?;

    let (start, end) = match (start, end) {
        (start, Some(end)) if start >= 0 && end >= 0 => (start as BlockNum, end as BlockNum),
        _ => {
            let latest_block = client.latest_block(true).await?;
            block_ranges::resolve_relative(start, end, latest_block)?
        }
    };

    // Use the intersection of the block ranges for all tables, only considering ranges scanned for
    // all tables.
    let block_ranges = {
        let mut block_ranges = block_ranges_by_table.clone().into_values();
        let first = block_ranges.next().ok_or("no tables")?;

        block_ranges.fold(first, |acc, r| acc.intersection(&r))
    };

    // Find the ranges of blocks that have not been scanned yet for at least one table.
    let ranges = block_ranges.complement(start, end);
    tracing::info!("dumping dataset {dataset_name} for ranges {ranges}");

    if ranges.total_len() == 0 {
        tracing::info!("no blocks to dump for {dataset_name}");
        return Ok(());
    }

    // Split them across the target number of jobs as to balance the number of blocks per job.
    let multiranges = ranges.split_and_partition(n_jobs as u64, 2000);
    let (tx, rx) = tokio::sync::mpsc::channel::<DumpPartition<_>>(n_jobs as usize * 2);

    // Enqueue all jobs
    for (i, multirange) in multiranges.into_iter().enumerate() {
        let job = DumpPartition {
            query_ctx: query_ctx.clone(),
            metadata_db: ctx.metadata_db.clone(),
            block_streamer: client.clone(),
            multirange,
            id: i as u32,
            partition_size,
            parquet_opts: parquet_opts.clone(),
            block_ranges_by_table: block_ranges_by_table.clone(),
        };
        tx.send(job).await.expect("queue send failed");
    }
    drop(tx); // Close the sender so workers exit when done

    let rx = Arc::new(Mutex::new(rx));
    let mut handles = Vec::new();
    let num_workers = 8;
    for worker in 0..num_workers {
        let rx = Arc::clone(&rx);
        handles.push(tokio::spawn(async move {
            loop {
                let job = {
                    let mut guard = rx.lock().await;
                    guard.recv().await
                };

                match job {
                    Some(job) => {
                        tracing::info!(
                            "worker {} took job {} (block ranges {:?}) off the queue",
                            worker,
                            job.id,
                            job.multirange,
                        );

                        if let Err(e) = job.run().await {
                            tracing::error!(error = %e, "partition job failed");
                        }
                    }
                    None => break,
                }
            }
        }));
    }

    // Wait for all workers to finish
    for handle in handles {
        handle.await.expect("worker panicked");
    }

    Ok(())
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
    metadata_db: Arc<MetadataDb>,
    /// The query context
    query_ctx: Arc<QueryContext>,
    /// The block ranges to scan
    multirange: MultiRange,
    /// The Parquet writer properties
    parquet_opts: ParquetWriterProperties,
    /// The target size of each table partition file in bytes.
    ///
    /// This is measured as the estimated uncompressed size of the partition.
    /// Once the size is reached, a new part file is created.
    /// Note that different tables may have a different number of partitions for a same block range.
    /// Lighter tables will have less parts than heavier tables.
    partition_size: u64,
    /// The block ranges by table
    block_ranges_by_table: BTreeMap<String, MultiRange>,
    /// The partition ID
    id: u32,
}
impl<S: BlockStreamer> DumpPartition<S> {
    /// Consumes the instance returning a future that runs the partition, processing all assigned block ranges sequentially.
    async fn run(self) -> Result<(), BoxError> {
        tracing::info!(
            "job partition #{} ranges to scan: {}",
            self.id,
            self.multirange
        );

        // The ranges are run sequentially by design, as parallelism is controlled by the number of jobs.
        for (range_start, range_end) in &self.multirange.ranges {
            tracing::info!(
                "job partition #{} starting scan for range [{}, {}]",
                self.id,
                range_start,
                range_end
            );
            let start_time = Instant::now();

            self.run_range(*range_start, *range_end).await?;

            let run_time = start_time.elapsed();
            tracing::info!(
                "job partition #{} finished scan for range [{}, {}] in {}",
                self.id,
                range_start,
                range_end,
                humantime::format_duration(run_time),
            );
        }
        Ok(())
    }

    async fn run_range(&self, start: u64, end: u64) -> Result<(), BoxError> {
        let stream = {
            let block_streamer = self.block_streamer.clone();
            block_streamer.block_stream(start, end).await
        };

        let mut writer = RawDatasetWriter::new(
            self.query_ctx.clone(),
            self.metadata_db.clone(),
            self.parquet_opts.clone(),
            start,
            end,
            self.partition_size,
            self.block_ranges_by_table.clone(),
        )?;

        let mut stream = std::pin::pin!(stream);
        while let Some(dataset_rows) = stream.try_next().await? {
            for table_rows in dataset_rows {
                writer.write(table_rows).await?;
            }
        }

        // Close the last part file for each table, checking for any errors.
        writer.close().await?;

        Ok(())
    }
}
