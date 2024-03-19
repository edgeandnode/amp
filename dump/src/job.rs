use common::arrow_helpers::rows_to_record_batch;
use common::Table;
use datafusion::parquet;
use firehose_datasources::evm;
use firehose_datasources::{client::Client, evm::protobufs_to_rows};
use futures::StreamExt as _;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::AsyncArrowWriter;
use parquet::file::properties::WriterProperties as ParquetWriterProperties;
use std::collections::BTreeMap;
use std::{sync::Arc, time::Instant};

type ParquetWriter = AsyncArrowWriter<BufWriter>;

pub struct Job {
    pub client: Client,
    pub start: u64,
    pub end: u64,
    pub job_id: u8,
    pub store: Arc<dyn ObjectStore>,
}

impl Job {
    fn path_for_table(&self, table_name: &str) -> String {
        // Pad `start` and `end` to 9 digits.
        let padded_start = format!("{:09}", self.start);
        let padded_end = format!("{:09}", self.end);

        format!("{}/{}-{}.parquet", table_name, padded_start, padded_end)
    }

    async fn writer_for_table(&self, table: &Table) -> Result<ParquetWriter, anyhow::Error> {
        let path = Path::parse(&self.path_for_table(table.name.as_str()))?;
        let object_writer = BufWriter::new(self.store.clone(), path);

        let schema = table.schema.clone();
        let opts = Some(parquet_options());

        // Watch https://github.com/apache/arrow-datafusion/issues/9493 for a higher level, parallel
        // API for parquet writing.
        let writer = ParquetWriter::try_new(object_writer, schema, opts)?;
        Ok(writer)
    }
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
            let writer = job.writer_for_table(&table).await?;
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

        let all_table_rows = protobufs_to_rows(block)?;

        for table_rows in all_table_rows {
            let record_batch = rows_to_record_batch(&table_rows)?;
            let writer = writers.get_mut(table_rows.table.name.as_str()).unwrap();
            writer.write(&record_batch).await?;
        }
    }

    for (_, writer) in writers {
        writer.close().await?;
    }

    Ok(())
}

fn parquet_options() -> ParquetWriterProperties {
    use parquet::basic::{Compression, ZstdLevel};

    // For DataFusion defaults, see `ParquetOptions` here:
    // https://github.com/apache/arrow-datafusion/blob/main/datafusion/common/src/config.rs
    let compression = Compression::ZSTD(ZstdLevel::try_new(1).unwrap());

    // Note: We could set `sorting_columns` for columns like `block_num` and `ordinal`. However,
    // Datafusion doesn't actually read that metadata info anywhere and just reiles on the
    // `file_sort_order` set on the reader configuration.
    ParquetWriterProperties::builder()
        .set_compression(compression)
        .build()
}
