use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use parquet::{
    arrow::{arrow_writer::ArrowWriterOptions, async_writer::AsyncFileWriter},
    errors::Result,
    file::{
        metadata::{KeyValue, ParquetMetaData, RowGroupMetaDataPtr},
        properties::WriterProperties,
    },
};

use crate::writer::Pipeline;

pub struct AsyncArrowWriter<W: AsyncFileWriter + Send> {
    pipeline: Pipeline<Vec<u8>, W>,
    _marker: std::marker::PhantomData<W>,
}

impl<W: AsyncFileWriter + Send> AsyncArrowWriter<W> {
    pub fn try_new(
        writer: W,
        arrow_schema: SchemaRef,
        props: Option<WriterProperties>,
    ) -> Result<Self> {
        Ok(Self {
            pipeline: Pipeline::<Vec<u8>, W>::builder()
                .build_properties(&arrow_schema, props)?
                .build_writer(Vec::new())?
                .with_progress()
                .into_factory()
                .spawn_encoder()
                .spawn_writer(writer)
                .build(),
            _marker: std::marker::PhantomData,
        })
    }

    /// Try to create a new Async Arrow Writer with [`ArrowWriterOptions`]
    ///
    /// This is unsafe because the options are not used to configure the writer in any way.
    /// This function exists for API compatibility with parquet::arrow::AsyncArrowWriter,
    /// but the options are ignored and will not have any effect on the writer's behavior.
    ///
    /// Use [`try_new`](Self::try_new) or [`try_new_with_shared_backend`](Self::try_new_with_shared_backend) instead,
    /// which will properly pass the provided configuration options to the writer as well as
    /// map the Arrow schema into the writer properties metadata and ensure the writer is configured
    /// to coerce types as needed.
    ///
    /// # Safety
    /// The provided options are ignored and will not have any effect on the writer's behavior.
    pub unsafe fn try_new_with_options(
        writer: W,
        arrow_schema: SchemaRef,
        _options: ArrowWriterOptions,
    ) -> Result<Self> {
        Self::try_new(writer, arrow_schema, None)
    }

    /// Write a RecordBatch to the writer.
    pub fn write(&mut self, batch: &RecordBatch) -> impl Future<Output = Result<()>> {
        self.pipeline.add_batch_async(batch.to_owned())
    }

    /// Flush buffered data to a new row group.
    ///
    /// Forces any buffered batches to be encoded and written,
    /// even if the row group size limit hasn't been reached.
    pub async fn flush(&mut self) -> Result<()> {
        self.pipeline.flush_async().await
    }

    /// Close the writer and return file metadata.
    pub async fn close(self) -> Result<ParquetMetaData> {
        self.pipeline.close_async().await
    }

    pub fn append_key_value_metadata(&mut self, kv: KeyValue) {
        self.pipeline.append_key_value_metadata(kv);
    }

    /// Estimated memory usage, in bytes, of this `ArrowWriter`
    pub fn memory_size(&self) -> usize {
        self.pipeline.memory_size()
    }

    /// Anticipated encoded size of the in progress row group.
    pub fn in_progress_size(&self) -> usize {
        self.pipeline.in_progress_size()
    }

    /// Returns the number of rows buffered in the in progress row group
    pub fn in_progress_rows(&self) -> usize {
        self.pipeline.in_progress_rows()
    }

    /// Returns the number of bytes written by this instance
    pub fn bytes_written(&self) -> usize {
        self.pipeline.bytes_written()
    }

    /// Returns metadata for all flushed row groups.
    pub fn flushed_row_groups(&self) -> &[RowGroupMetaDataPtr] {
        self.pipeline.flushed_row_groups()
    }
}
