use common::arrow_helpers::rows_to_record_batch;
use datafusion::parquet::file::properties::WriterProperties as ParquetWriterProperties;
use firehose_datasets::client::Error as FirehoseError;
use firehose_datasets::evm::{self, pbethereum};
use firehose_datasets::{client::Client, evm::protobufs_to_rows};
use futures::{FutureExt, StreamExt as _};
use object_store::path::Path;
use object_store::ObjectStore;
use std::collections::BTreeMap;
use std::{sync::Arc, time::Instant};
use tokio::sync::mpsc;

use crate::parquet_writer::ParquetWriter;

pub struct Job {
    pub client: Client,
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
}

fn path_for_part(table_name: &str, start_block: u64) -> String {
    // Pad `start` to 9 digits.
    let padded_start = format!("{:09}", start_block);

    format!("{}/{}.parquet", table_name, padded_start)
}

// Spawning a job:
// - Spawns a task to fetch blocks from the `client`.
// - Returns a future that will read that block stream and write a parquet file to the object store.
pub async fn run_job(job: Job) -> Result<(), anyhow::Error> {
    let start_time = Instant::now();

    let (mut firehose, firehose_join_handle) = {
        let start_block = job.start;
        let end_block = job.end;
        let client = job.client.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let firehose_task = stream_blocks_to_channel(client, start_block, end_block, tx);
        (rx, tokio::spawn(firehose_task))
    };

    let mut writers: BTreeMap<String, ParquetWriter> = {
        let mut writers = BTreeMap::new();
        let tables = evm::tables::all();
        for table in tables {
            let path = Path::parse(&path_for_part(&table.name, job.start))?;
            let writer =
                ParquetWriter::new(&job.store, path, &table.schema, job.parquet_opts.clone())
                    .await?;
            writers.insert(table.name.clone(), writer);
        }
        writers
    };

    while let Some(block) = firehose.recv().await {
        if block.number % 100000 == 0 {
            println!(
                "Reached block {}, at minute {}",
                block.number,
                start_time.elapsed().as_secs() / 60
            );
        }

        let block_number = block.number;
        let all_table_rows = protobufs_to_rows(block)?;

        for table_rows in all_table_rows {
            let record_batch = rows_to_record_batch(&table_rows)?;
            let table = table_rows.table;

            let bytes_written = writers.get(table.name.as_str()).unwrap().bytes_written();

            // Check if we need to create a new part file for the table.
            if bytes_written >= job.partition_size {
                let path = Path::parse(&path_for_part(&table.name, block_number))?;
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

/// Fetches blocks from the Firehose and sends them through the channel.
///
/// Terminates with `Ok` when the Firehose stream reaches `end_block`, or the channel receiver goes
/// away.
///
/// Terminates with `Err` when there is an error estabilishing the stream.
///
/// Errors from the Firehose stream are logged and retried.
async fn stream_blocks_to_channel(
    mut client: Client,
    start_block: u64,
    end_block: u64,
    tx: mpsc::Sender<pbethereum::Block>,
) -> Result<(), FirehoseError> {
    // Explicitly track the next block in case we need to restart the Firehose stream.
    let mut next_block = start_block;

    // A retry loop for consuming the Firehose.
    'retry: loop {
        let mut stream = match client.blocks(next_block, end_block).await {
            Ok(stream) => Box::pin(stream),

            // If there is an error at the initial connection, we don't retry here as that's
            // unexpected.
            Err(err) => break Err(err),
        };
        while let Some(block) = stream.next().await {
            match block {
                Ok(block) => {
                    let block_num = block.number;

                    // Send the block and check if the receiver has gone away.
                    if tx.send(block).await.is_err() {
                        break;
                    }

                    next_block = block_num + 1;
                }
                Err(err) => {
                    // Log and retry.
                    println!("Error reading firehose stream: {}", err);
                    continue 'retry;
                }
            }
        }

        // The stream has ended, or the receiver has gone away, either way we hit a natural
        // termination condition.
        break Ok(());
    }
}
