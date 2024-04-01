use common::arrow_helpers::rows_to_record_batch;
use datafusion::parquet::file::properties::WriterProperties as ParquetWriterProperties;
use firehose_datasources::evm;
use firehose_datasources::{client::Client, evm::protobufs_to_rows};
use futures::StreamExt as _;
use object_store::path::Path;
use object_store::ObjectStore;
use std::collections::BTreeMap;
use std::{sync::Arc, time::Instant};

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
pub async fn run_job(mut job: Job) -> Result<(), anyhow::Error> {
    let mut stream = Box::pin(job.client.blocks(job.start, job.end).await?);

    // Polls the stream concurrently to the write task
    let start = Instant::now();
    let (tx, mut block_stream) = tokio::sync::mpsc::channel(100);
    tokio::spawn(async move {
        while let Some(block) = stream.next().await {
            let _ = tx.send(block).await;
        }
    });

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

    while let Some(block) = block_stream.recv().await {
        let block = block?;
        if block.number % 100000 == 0 {
            println!(
                "Reached block {}, at minute {}",
                block.number,
                start.elapsed().as_secs() / 60
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

    // Close the last part file for each table.
    for (_, writer) in writers {
        writer.close().await?;
    }

    Ok(())
}
