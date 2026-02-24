mod backend;
mod encoder;
mod factory;
mod job;
mod progress;
mod properties;
mod writer;

use std::io::Write;

use arrow_array::RecordBatch;
use bytes::Bytes;
use parquet::{
    arrow::async_writer::AsyncFileWriter,
    errors::Result,
    file::metadata::{KeyValue, ParquetMetaData, RowGroupMetaDataPtr},
};

pub use self::properties::PipelineProperties;
use self::{
    encoder::{EncodedRowGroup, EncoderExecutor, RowGroupEncoder},
    factory::PipelineFactoryBuilder,
    job::{EncoderOutbox, WriteJob, WriterInbox, WriterOutbox},
    progress::Progress,
    writer::WriterExecutor,
};

pub struct Pipeline<Buf: Default + Write + Send, AsyncWriter: AsyncFileWriter + Send>
where
    Bytes: From<Buf>,
{
    pub(self) progress: Option<Progress>,
    pub(self) encoder_executor: EncoderExecutor,
    pub(self) writer_executor: WriterExecutor<AsyncWriter, Buf>,
}

impl<Buf: Default + Write + Send, AsyncWriter: AsyncFileWriter + Send> Pipeline<Buf, AsyncWriter>
where
    Bytes: From<Buf>,
{
    pub fn builder() -> PipelineFactoryBuilder<Buf> {
        PipelineFactoryBuilder::default()
    }

    pub async fn add_batch_async(&mut self, batch: RecordBatch) -> Result<()> {
        tokio::try_join!(
            self.writer_executor.drain_ready(),
            self.encoder_executor.handle_batch_async(batch)
        )?;
        Ok(())
    }

    pub async fn flush_async(&mut self) -> Result<()> {
        tokio::try_join!(
            self.writer_executor.drain_ready(),
            self.encoder_executor.flush_async(false)
        )?;
        Ok(())
    }

    pub fn append_key_value_metadata(&mut self, kv: KeyValue) {
        self.writer_executor.append_key_value_metadata(kv);
    }

    pub async fn close_async(mut self) -> Result<ParquetMetaData> {
        self.encoder_executor.flush(true)?;
        let metadata = self.writer_executor.close_async().await?;
        Ok(metadata)
    }

    pub fn shutdown(self) {
        self.encoder_executor.shutdown();
    }

    /// Get the number of rows that are buffered or being encoded but not yet written.
    pub fn in_progress_rows(&self) -> usize {
        self.progress
            .as_ref()
            .map(|p| p.in_progress_rows())
            .unwrap_or_default()
    }

    /// Get the estimated size of data that is buffered or being encoded but not yet written.
    pub fn in_progress_size(&self) -> usize {
        self.progress
            .as_ref()
            .map(|p| p.in_progress_size())
            .unwrap_or_default()
    }

    /// Get the estimated memory usage of buffered data.
    pub fn memory_size(&self) -> usize {
        self.progress
            .as_ref()
            .map(|p| p.memory_size())
            .unwrap_or_default()
    }

    /// Get the total bytes written to the output so far.
    pub fn bytes_written(&self) -> usize {
        self.progress
            .as_ref()
            .map(|p| p.bytes_written())
            .unwrap_or_default()
    }

    pub fn flushed_row_groups(&self) -> &[RowGroupMetaDataPtr] {
        self.writer_executor.flushed_row_groups()
    }
}
