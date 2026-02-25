use std::mem::ManuallyDrop;

use arrow_array::RecordBatch;
use futures::future::BoxFuture;
use parquet::errors::{ParquetError, Result};

use super::{Batches, factory::EncoderFactory};
use crate::{
    backend::PipelineBackend,
    writer::pipeline::{EncoderOutbox, Progress, WriterOutbox, job::EncodeJob},
};

pub struct EncoderExecutor {
    backend: Option<PipelineBackend>,
    batches: Batches,
    factory: EncoderFactory,
    progress: Option<Progress>,
    writer_outbox: ManuallyDrop<WriterOutbox>,
    encoder_outbox: ManuallyDrop<EncoderOutbox>,
}

impl Drop for EncoderExecutor {
    fn drop(&mut self) {
        // SAFETY: Channel senders must be dropped before joining the encoder
        // thread. The encoder thread blocks on `inbox.recv()`, which only
        // returns `Err` once all senders are dropped. Joining first would
        // deadlock. These fields are not accessed after this point.
        unsafe {
            ManuallyDrop::drop(&mut self.writer_outbox);
            ManuallyDrop::drop(&mut self.encoder_outbox);
        }
        if let Some(mut backend) = self.backend.take() {
            backend.shutdown();
        }
    }
}

impl EncoderExecutor {
    pub fn new(
        backend: Option<PipelineBackend>,
        factory: EncoderFactory,
        max_rows: usize,
        progress: Option<Progress>,
        writer_outbox: WriterOutbox,
        encoder_outbox: EncoderOutbox,
    ) -> Self {
        EncoderExecutor {
            backend,
            batches: Batches::new(max_rows),
            factory,
            progress,
            writer_outbox: ManuallyDrop::new(writer_outbox),
            encoder_outbox: ManuallyDrop::new(encoder_outbox),
        }
    }

    fn rows(&self) -> usize {
        self.batches.rows
    }

    fn max_rows(&self) -> usize {
        self.batches.max_rows
    }

    fn headroom(&self) -> usize {
        self.max_rows() - self.rows()
    }

    fn take_batches(&mut self) -> (Vec<RecordBatch>, usize) {
        self.batches.take(self.progress.as_ref())
    }

    pub fn handle_batch_async(
        &mut self,
        batch: RecordBatch,
    ) -> BoxFuture<'_, Result<(), ParquetError>> {
        let batch_rows = batch.num_rows();
        if batch_rows == 0 {
            return Box::pin(async { Ok(()) });
        }

        Box::pin(async move {
            if self.rows() + batch_rows > self.max_rows() {
                let headroom = self.headroom();
                if headroom > 0 {
                    let batch_a = batch.slice(0, headroom);
                    let batch_b = batch.slice(headroom, batch_rows - headroom);
                    self.handle_batch_async(batch_a).await?;
                    self.handle_batch_async(batch_b).await?;
                } else {
                    self.flush_async(false).await?;
                    self.handle_batch_async(batch).await?;
                }
                return Ok(());
            }

            self.batches.add_batch(batch, self.progress.as_ref());

            if self.rows() >= self.max_rows() {
                self.flush_async(false).await?;
            }

            Ok(())
        })
    }

    pub async fn flush_async(&mut self, finalize: bool) -> Result<()> {
        if self.batches.is_empty() && !finalize {
            return Ok(());
        }

        let encoder = self.factory.try_next_encoder()?;

        let (batches, rows) = self.take_batches();
        let reply_tx = ManuallyDrop::into_inner(self.encoder_outbox.clone());

        let job = EncodeJob {
            encoder,
            batches,
            rows,
            finalize,
            reply_tx,
            progress: self.progress.clone(),
        };

        self.writer_outbox
            .send_async(job)
            .await
            .map_err(|e| ParquetError::General(e.to_string()))?;

        Ok(())
    }

    #[allow(unused)]
    pub fn flush(&mut self, finalize: bool) -> Result<(), ParquetError> {
        if self.batches.is_empty() && !finalize {
            return Ok(());
        }

        let encoder = self.factory.try_next_encoder()?;

        let (batches, rows) = self.take_batches();
        let reply_tx = ManuallyDrop::into_inner(self.encoder_outbox.clone());

        let job = EncodeJob {
            encoder,
            batches,
            rows,
            finalize,
            reply_tx,
            progress: self.progress.clone(),
        };

        self.writer_outbox
            .send(job)
            .map_err(|e| ParquetError::General(e.to_string()))?;

        Ok(())
    }

    pub fn shutdown(self) {
        drop(self);
    }
}
