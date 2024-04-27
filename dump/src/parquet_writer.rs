use std::collections::BTreeMap;
use std::sync::Arc;

use common::arrow::array::RecordBatch;
use common::meta_tables::scanned_ranges::ScannedRange;
use common::parquet::errors::ParquetError;
use common::{parquet, BlockNum, DataSet, Table, TableRows};
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::AsyncArrowWriter;
use parquet::file::properties::WriterProperties as ParquetWriterProperties;

fn path_for_part(table_name: &str, start_block: u64) -> String {
    // Pad `start` to 9 digits.
    let padded_start = format!("{:09}", start_block);

    format!("{}/{}.parquet", table_name, padded_start)
}

pub struct DatasetWriter {
    writers: BTreeMap<String, ParquetWriter>,

    opts: ParquetWriterProperties,
    store: Arc<dyn ObjectStore>,
    partition_size: u64,
}

impl DatasetWriter {
    pub async fn new(
        dataset: &DataSet,
        store: Arc<dyn ObjectStore>,
        opts: ParquetWriterProperties,
        start: BlockNum,
        partition_size: u64,
    ) -> Result<Self, anyhow::Error> {
        let mut writers = BTreeMap::new();
        for table in dataset.tables() {
            let path = Path::parse(&path_for_part(&table.name, start))?;
            let writer = ParquetWriter::new(&store, path, &table, opts.clone(), start).await?;
            writers.insert(table.name.clone(), writer);
        }
        Ok(DatasetWriter {
            writers,
            opts,
            store,
            partition_size,
        })
    }

    pub async fn write(&mut self, table_rows: TableRows) -> Result<(), anyhow::Error> {
        let table = &table_rows.table;
        let block_num = table_rows.block_num()?;

        let bytes_written = self
            .writers
            .get(table.name.as_str())
            .unwrap()
            .bytes_written();

        // Check if we need to create a new part file for the table.
        if bytes_written >= self.partition_size {
            let path = Path::parse(&path_for_part(&table.name, block_num))?;
            let new_writer =
                ParquetWriter::new(&self.store, path, table, self.opts.clone(), block_num).await?;
            let old_writer = self.writers.insert(table.name.clone(), new_writer).unwrap();

            // The `__scanned_ranges` optimization works best if ranges are adjacent, even if the
            // tables themselves are sparse and don't have data for all block numbers. So we start
            // the new range at `block_num` and close the previous one at `block_num - 1`.
            old_writer.close(block_num - 1).await?;
        }

        let writer = self.writers.get_mut(table.name.as_str()).unwrap();
        let _scanned_range = writer.write(&table_rows.rows).await?;

        Ok(())
    }

    /// Flush and close all pending writes.
    pub async fn close(self) -> Result<(), ParquetError> {
        for (_, writer) in self.writers {
            let end = writer.bytes_written();
            let _scanned_range = writer.close(end).await?;
        }
        Ok(())
    }
}

pub struct ParquetWriter {
    writer: AsyncArrowWriter<BufWriter>,

    table: Table,

    // The first block number in the range that this writer is responsible for.
    start: BlockNum,

    // Sum of `get_slice_memory_size` for all data written. Does not correspond to the actual size of
    // the written file, particularly because this is uncompressed.
    bytes_written: u64,
}

impl ParquetWriter {
    pub async fn new(
        store: &Arc<dyn ObjectStore>,
        path: Path,
        table: &Table,
        opts: ParquetWriterProperties,
        start: BlockNum,
    ) -> Result<ParquetWriter, ParquetError> {
        let object_writer = BufWriter::new(store.clone(), path);

        // Watch https://github.com/apache/arrow-datafusion/issues/9493 for a higher level, parallel
        // API for parquet writing.
        let writer = AsyncArrowWriter::try_new(object_writer, table.schema.clone(), Some(opts))?;
        Ok(ParquetWriter {
            writer,
            start,
            table: table.clone(),
            bytes_written: 0,
        })
    }

    pub async fn write(&mut self, batch: &RecordBatch) -> Result<(), ParquetError> {
        // Calculate the size of the batch in bytes. `get_slice_memory_size` is the most precise way.
        self.bytes_written += batch
            .columns()
            .iter()
            .map(|c| c.to_data().get_slice_memory_size().unwrap())
            .sum::<usize>() as u64;

        self.writer.write(batch).await
    }

    pub async fn close(self, end: BlockNum) -> Result<ScannedRange, ParquetError> {
        self.writer.close().await?;
        let scanned_range = ScannedRange {
            table: self.table.name.clone(),
            range_start: self.start,
            range_end: end,
        };
        Ok(scanned_range)
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}
