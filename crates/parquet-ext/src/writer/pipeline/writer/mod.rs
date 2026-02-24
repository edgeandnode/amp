mod row_group;

use std::io::Write;

use bytes::Bytes;
use parquet::{
    arrow::async_writer::AsyncFileWriter,
    errors::{ParquetError, Result},
    file::{
        metadata::{KeyValue, ParquetMetaData, RowGroupMetaDataPtr},
        writer::SerializedFileWriter,
    },
};

use self::row_group::PendingRowGroups;
use super::{Progress, WriterInbox};
use crate::writer::pipeline::{job::WriteJob, writer::row_group::PendingRowGroup};

pub struct WriterExecutor<Writer: AsyncFileWriter + Send, Buf: Write + Send>
where
    Bytes: From<Buf>,
{
    inbox: WriterInbox,
    writer: Writer,
    file_writer: SerializedFileWriter<Buf>,
    pending: PendingRowGroups,
    progress: Option<Progress>,
    flushed_row_groups: Vec<RowGroupMetaDataPtr>,
    key_value_metadata: Vec<KeyValue>,
}

impl<Writer: AsyncFileWriter + Send, Buf: Default + Write + Send> WriterExecutor<Writer, Buf>
where
    Bytes: From<Buf>,
{
    pub fn new(
        inbox: WriterInbox,
        writer: Writer,
        file_writer: SerializedFileWriter<Buf>,
        progress: Option<&Progress>,
    ) -> Self {
        Self {
            inbox,
            writer,
            file_writer,
            pending: PendingRowGroups::default(),
            progress: progress.cloned(),
            flushed_row_groups: Vec::new(),
            key_value_metadata: Vec::new(),
        }
    }

    /// Non-blocking drain: process any ready WriteJobs via try_recv.
    pub async fn drain_ready(&mut self) -> Result<()> {
        while let Ok(job) = self.inbox.try_recv() {
            match job {
                WriteJob::Encoded { row_group } => {
                    self.pending.insert(row_group);
                }
                WriteJob::Finalize { row_group } => {
                    self.pending.insert(row_group);
                }
                WriteJob::Error { id, error } => {
                    eprintln!("Encoding error for row group {id}: {error}");
                    return Err(error);
                }
            }
        }

        self.flush_ready().await
    }

    /// Blocking close: receive all remaining WriteJobs until Finalize, then finalize.
    pub async fn close_async(mut self) -> Result<ParquetMetaData> {
        loop {
            match self
                .inbox
                .recv_async()
                .await
                .map_err(|err| ParquetError::External(err.into()))?
            {
                WriteJob::Encoded { row_group } => {
                    self.pending.insert(row_group);
                    self.flush_ready().await?;
                }
                WriteJob::Finalize { row_group } => {
                    self.pending.insert(row_group);
                    self.flush_ready().await?;
                    return self.finalize().await;
                }
                WriteJob::Error { id, error } => {
                    eprintln!("Encoding error for row group {id}: {error}");
                    return Err(error);
                }
            }
        }
    }

    /// Get flushed row group metadata.
    pub fn flushed_row_groups(&self) -> &[RowGroupMetaDataPtr] {
        &self.flushed_row_groups
    }

    pub fn append_key_value_metadata(&mut self, kv: KeyValue) {
        self.key_value_metadata.push(kv);
    }

    async fn write_row_group(&mut self, pending: PendingRowGroup) -> Result<()> {
        let PendingRowGroup {
            chunks,
            rows,
            bytes,
        } = pending;

        if let Some(progress) = &mut self.progress {
            progress.finish_encoding(rows, bytes);
        }

        if rows == 0 {
            return Ok(());
        }

        let mut row_group_writer = self.file_writer.next_row_group()?;

        for chunk in chunks {
            chunk.append_to_row_group(&mut row_group_writer)?;
        }

        let metadata = row_group_writer.close()?;
        self.flushed_row_groups.push(metadata);

        let bs = Bytes::from(std::mem::take(self.file_writer.inner_mut()));
        let bytes_len = bs.len();
        self.writer.write(bs).await?;
        if let Some(progress) = &mut self.progress {
            progress.record_write(rows, bytes_len);
        }

        Ok(())
    }

    /// Write any row groups that are ready (in order).
    async fn flush_ready(&mut self) -> Result<()> {
        while let Some(pending) = self.pending.next_ready() {
            self.write_row_group(pending).await?;
        }
        Ok(())
    }

    async fn flush_pending_encoders(&mut self) -> Result<()> {
        while self.pending.has_pending() {
            match self
                .inbox
                .recv_async()
                .await
                .map_err(|err| ParquetError::General(err.to_string()))
            {
                Ok(WriteJob::Encoded { row_group }) => {
                    self.pending.insert(row_group);
                }
                Ok(WriteJob::Error { id, error }) => {
                    eprintln!("Encoding error for row group {id}: {error}");
                    return Err(error);
                }
                Err(err) => {
                    return Err(err);
                }
                Ok(WriteJob::Finalize { .. }) => {
                    unreachable!("Unexpected Finalize message during drain")
                }
            }

            self.flush_ready().await?;
        }

        Ok(())
    }

    async fn finalize(&mut self) -> Result<ParquetMetaData> {
        self.flush_pending_encoders().await?;
        self.flush_key_value_metadata();

        if !self.pending.is_empty() {
            return Err(ParquetError::General(format!(
                "Attempted to close parquet file with {} pending row groups (missing IDs: {:?})",
                self.pending.len(),
                self.pending.keys().collect::<Vec<_>>()
            )));
        }

        let metadata = self.file_writer.finish()?;

        let buf = std::mem::take(self.file_writer.inner_mut());

        self.writer.write(Bytes::from(buf)).await?;
        self.writer.complete().await?;

        Ok(metadata)
    }

    fn flush_key_value_metadata(&mut self) {
        for kv in self.key_value_metadata.drain(..) {
            self.file_writer.append_key_value_metadata(kv);
        }
    }
}
