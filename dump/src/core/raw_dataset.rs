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
//!   stream to maintain a steady flow of data without overwhelming memory.
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
//!   ensuring that partial progress is not recorded in case of failures.
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
//!   with simultaneous connection requests.

use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use common::{
    multirange::MultiRange, parquet::file::properties::WriterProperties as ParquetWriterProperties,
    query_context::QueryContext, BlockNum, BlockStreamer, BoxError,
};
use dataset_store::DatasetStore;
use futures::{future::try_join_all, TryFutureExt as _};
use metadata_db::MetadataDb;
use tracing::instrument;

use super::block_ranges;
use crate::parquet_writer::RawDatasetWriter;

/// Dumps a raw dataset
#[instrument(skip_all, fields(dataset = %dataset_name), err)]
pub async fn dump(
    n_jobs: u16,
    ctx: Arc<QueryContext>,
    dataset_name: &str,
    dataset_store: &DatasetStore,
    block_ranges_by_table: BTreeMap<String, MultiRange>,
    partition_size: u64,
    parquet_opts: &ParquetWriterProperties,
    start: i64,
    end: Option<i64>,
) -> Result<(), BoxError> {
    let mut client = dataset_store.load_client(dataset_name).await?;

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

    let jobs = multiranges.into_iter().enumerate().map(|(i, multirange)| {
        Arc::new(DumpPartition {
            dataset_ctx: ctx.clone(),
            metadata_db: dataset_store.metadata_db.clone(),
            block_streamer: client.clone(),
            multirange,
            id: i as u32,
            partition_size,
            parquet_opts: parquet_opts.clone(),
            block_ranges_by_table: block_ranges_by_table.clone(),
        })
    });

    // Spawn the jobs so they run in parallel, terminating early if any job fails.
    let mut join_handles = vec![];
    for job in jobs {
        let handle = tokio::spawn(job.run());

        // Stagger the start of each job by 1 second in an attempt to avoid client rate limits.
        tokio::time::sleep(Duration::from_secs(1)).await;

        join_handles.push(async { handle.err_into().await.and_then(|x| x) });
    }

    try_join_all(join_handles).await?;

    Ok(())
}

/// A partition of a raw dataset dump job
struct DumpPartition<T: BlockStreamer> {
    dataset_ctx: Arc<QueryContext>,
    block_streamer: T,
    multirange: MultiRange,
    id: u32,
    parquet_opts: ParquetWriterProperties,

    // The target size of each table partition file in bytes. This is measured as the estimated
    // uncompressed size of the partition. Once the size is reached, a new part file is created. Note
    // that different tables may have a different number of partitions for a same block range.
    // Lighter tables will have less parts than heavier tables.
    partition_size: u64,

    block_ranges_by_table: BTreeMap<String, MultiRange>,

    metadata_db: Arc<MetadataDb>,
}

impl<S: BlockStreamer> DumpPartition<S> {
    // - Spawns a task to fetch blocks from the `client`.
    // - Returns a future that will read that block stream and write a parquet file to the object store.
    async fn run(self: Arc<Self>) -> Result<(), BoxError> {
        tracing::info!(
            "job partition #{} ranges to scan: {}",
            self.id,
            self.multirange
        );

        // The ranges are run sequentially by design, as parallelism is controlled by the number of jobs.
        for range in &self.multirange.ranges {
            tracing::info!(
                "job partition #{} starting scan for range [{}, {}]",
                self.id,
                range.0,
                range.1
            );
            let start_time = Instant::now();

            self.run_range(range.0, range.1).await?;

            tracing::info!(
                "job partition #{} finished scan for range [{}, {}] in {} minutes",
                self.id,
                range.0,
                range.1,
                start_time.elapsed().as_secs() / 60
            );
        }
        Ok(())
    }

    async fn run_range(&self, start: u64, end: u64) -> Result<(), BoxError> {
        let (mut extractor, extractor_join_handle) = {
            let block_streamer = self.block_streamer.clone();
            let (tx, rx) = tokio::sync::mpsc::channel(100);
            let extractor_task = block_streamer.block_stream(start, end, tx);
            (rx, tokio::spawn(extractor_task))
        };

        let mut writer = RawDatasetWriter::new(
            self.dataset_ctx.clone(),
            self.metadata_db.clone(),
            self.parquet_opts.clone(),
            start,
            end,
            self.partition_size,
            self.block_ranges_by_table.clone(),
        )?;

        while let Some(dataset_rows) = extractor.recv().await {
            for table_rows in dataset_rows {
                writer.write(table_rows).await?;
            }
        }

        // The extraction task stopped sending blocks, so it must have terminated. Here we wait for it to
        // finish and check for any errors and panics.
        tracing::debug!("Waiting for job partition #{} to finish", self.id);
        extractor_join_handle.await??;

        // Close the last part file for each table, checking for any errors.
        writer.close().await?;

        Ok(())
    }
}
