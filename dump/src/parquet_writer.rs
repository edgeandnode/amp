use std::sync::Arc;

use common::arrow::array::RecordBatch;
use common::arrow::datatypes::SchemaRef;
use common::parquet;
use common::parquet::errors::ParquetError;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::AsyncArrowWriter;
use parquet::file::properties::WriterProperties as ParquetWriterProperties;

pub struct ParquetWriter {
    writer: AsyncArrowWriter<BufWriter>,

    // Sum of `get_slice_memory_size` for all data written. Does not correspond to the actual size of
    // the written file, particularly because this is uncompressed.
    bytes_written: u64,
}

impl ParquetWriter {
    pub async fn new(
        store: &Arc<dyn ObjectStore>,
        path: Path,
        schema: &SchemaRef,
        opts: ParquetWriterProperties,
    ) -> Result<ParquetWriter, ParquetError> {
        let object_writer = BufWriter::new(store.clone(), path);

        // Watch https://github.com/apache/arrow-datafusion/issues/9493 for a higher level, parallel
        // API for parquet writing.
        let writer = AsyncArrowWriter::try_new(object_writer, schema.clone(), Some(opts))?;
        Ok(ParquetWriter {
            writer,
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

    pub async fn close(self) -> Result<(), ParquetError> {
        self.writer.close().await?;
        Ok(())
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}
