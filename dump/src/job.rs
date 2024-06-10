use common::multirange::MultiRange;
use common::parquet::file::properties::WriterProperties as ParquetWriterProperties;
use common::{BlockStreamer, BoxError, QueryContext};
use log::info;
use std::collections::BTreeMap;
use std::{sync::Arc, time::Instant};

use crate::parquet_writer::DatasetWriter;

pub struct Job<T: BlockStreamer> {
    pub dataset_ctx: Arc<QueryContext>,
    pub block_streamer: T,
    pub multirange: MultiRange,
    pub job_id: u32,
    pub parquet_opts: ParquetWriterProperties,

    // The target size of each table partition file in bytes. This is measured as the estimated
    // uncompressed size of the partition. Once the size is reached, a new part file is created. Note
    // that different tables may have a different number of partitions for a same block range.
    // Lighter tables will have less parts than heavier tables.
    pub partition_size: u64,

    // Block ranges that are already written to the object store, per table. This is used to resume a
    // job that was interrupted. These blocks should simply be skipped.
    pub existing_blocks: BTreeMap<String, MultiRange>,
}

// Spawning a job:
// - Spawns a task to fetch blocks from the `client`.
// - Returns a future that will read that block stream and write a parquet file to the object store.
pub async fn run(job: Arc<Job<impl BlockStreamer>>) -> Result<(), BoxError> {
    info!("job #{} ranges to scan: {}", job.job_id, job.multirange);

    // The ranges are run sequentially by design, as parallelism is controlled by the number of jobs.
    for range in &job.multirange.ranges {
        info!(
            "job #{} starting scan for range [{}, {}]",
            job.job_id, range.0, range.1
        );
        let start_time = Instant::now();

        run_job_range(job.clone(), range.0, range.1).await?;

        info!(
            "job #{} finished scan for range [{}, {}] in {} minutes",
            job.job_id,
            range.0,
            range.1,
            start_time.elapsed().as_secs() / 60
        );
    }
    Ok(())
}

async fn run_job_range(
    job: Arc<Job<impl BlockStreamer>>,
    start: u64,
    end: u64,
) -> Result<(), BoxError> {
    let (mut firehose, firehose_join_handle) = {
        let block_streamer = job.block_streamer.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let firehose_task = block_streamer.block_stream(start, end, tx);
        (rx, tokio::spawn(firehose_task))
    };

    let mut writer = DatasetWriter::new(
        job.dataset_ctx.clone(),
        job.parquet_opts.clone(),
        start,
        job.partition_size,
    )
    .await?;

    while let Some(dataset_rows) = firehose.recv().await {
        if dataset_rows.is_empty() {
            continue;
        }

        let block_num = dataset_rows.block_num()?;

        for table_rows in dataset_rows {
            if table_rows.is_empty() {
                continue;
            }

            // Skip blocks that are already present.
            if job
                .existing_blocks
                .get(table_rows.table.name.as_str())
                .map_or(false, |range| range.contains(block_num))
            {
                continue;
            }

            writer.write(table_rows).await?;
        }
    }

    // The Firehose task stopped sending blocks, so it must have terminated. Here we wait for it to
    // finish and check for any errors and panics.
    log::debug!("Waiting for firehose job #{} to finish", job.job_id);
    firehose_join_handle.await??;
    // Close the last part file for each table, checking for any errors.
    writer.close(end).await?;

    Ok(())
}
