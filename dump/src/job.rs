use common::multirange::MultiRange;
use common::parquet::file::properties::WriterProperties as ParquetWriterProperties;
use common::{BlockStreamer, DataSet};
use futures::FutureExt as _;
use object_store::path::Path;
use object_store::ObjectStore;
use std::collections::BTreeMap;
use std::{sync::Arc, time::Instant};

use crate::parquet_writer::ParquetWriter;

pub struct Job<T: BlockStreamer> {
    pub dataset: DataSet,
    pub block_streamer: T,
    pub start: u64,
    pub end: u64,
    pub job_id: u8,
    pub store: Arc<dyn ObjectStore>,
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

fn path_for_part(table_name: &str, start_block: u64) -> String {
    // Pad `start` to 9 digits.
    let padded_start = format!("{:09}", start_block);

    format!("{}/{}.parquet", table_name, padded_start)
}

// Spawning a job:
// - Spawns a task to fetch blocks from the `client`.
// - Returns a future that will read that block stream and write a parquet file to the object store.
pub async fn run_job(job: Job<impl BlockStreamer>) -> Result<(), anyhow::Error> {
    let start_time = Instant::now();

    let (mut firehose, firehose_join_handle) = {
        let start_block = job.start;
        let end_block = job.end;
        let block_streamer = job.block_streamer.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let firehose_task = block_streamer.block_stream(start_block, end_block, tx);
        (rx, tokio::spawn(firehose_task))
    };

    let mut writers: BTreeMap<String, ParquetWriter> = {
        let mut writers = BTreeMap::new();
        let tables = job.dataset.tables();
        for table in tables {
            let path = Path::parse(&path_for_part(&table.name, job.start))?;
            let writer =
                ParquetWriter::new(&job.store, path, &table.schema, job.parquet_opts.clone())
                    .await?;
            writers.insert(table.name.clone(), writer);
        }
        writers
    };

    while let Some(dataset_rows) = firehose.recv().await {
        if dataset_rows.is_empty() {
            continue;
        }

        let block_num = dataset_rows.block_num()?;

        if block_num % 100000 == 0 {
            println!(
                "Reached block {}, at minute {}",
                block_num,
                start_time.elapsed().as_secs() / 60
            );
        }

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

            let record_batch = table_rows.rows;
            let table = table_rows.table;

            let bytes_written = writers.get(table.name.as_str()).unwrap().bytes_written();

            // Check if we need to create a new part file for the table.
            if bytes_written >= job.partition_size {
                let path = Path::parse(&path_for_part(&table.name, block_num))?;
                let new_writer =
                    ParquetWriter::new(&job.store, path, &table.schema, job.parquet_opts.clone())
                        .await?;
                let old_writer = writers.insert(table.name.clone(), new_writer).unwrap();
                old_writer.close().await?;
            }

            let writer = writers.get_mut(table.name.as_str()).unwrap();
            writer.write(&record_batch).await?;
        }
    }

    // The Firehose task stopped sending blocks, so it must have terminated. Here we check if it
    // terminated with any errors or panics.
    firehose_join_handle.now_or_never().unwrap()??;

    // Close the last part file for each table, checking for any errors.
    for (_, writer) in writers {
        writer.close().await?;
    }

    Ok(())
}
