use std::{
    sync::atomic::{AtomicUsize, Ordering},
    thread::scope,
};

use arrow_array::RecordBatch;
use parking_lot::Mutex;
use parquet::{
    arrow::arrow_writer::{
        ArrowColumnChunk, ArrowColumnWriter, ArrowLeafColumn, ArrowRowGroupWriterFactory,
        compute_leaves,
    },
    errors::ParquetError,
};
use tokio::sync::oneshot::Receiver;

mod batches;
mod executor;
mod factory;
mod row_group;

pub use self::{
    batches::Batches,
    executor::EncoderExecutor,
    factory::EncoderFactory,
    row_group::{EncodedRowGroup, PartialEncodedRowGroup},
};
use crate::writer::pipeline::{
    job::{EncoderInbox, WriteJob},
    progress::Progress,
};

pub struct Encoder {
    inbox: EncoderInbox,
    num_workers: usize,
    shutdown_receiver: Receiver<()>,
}

impl Encoder {
    pub fn new(inbox: EncoderInbox, num_workers: usize, shutdown_receiver: Receiver<()>) -> Self {
        Encoder {
            inbox,
            num_workers,
            shutdown_receiver,
        }
    }

    pub fn run(self) {
        while let Ok(job) = self.inbox.recv()
            && self.shutdown_receiver.is_empty()
        {
            let id = job.encoder.id();
            let rows = job.rows;

            let encode_result =
                job.encoder
                    .encode(&job.batches, job.progress.as_ref(), self.num_workers);

            let result = match encode_result {
                Ok(encoded) => encoded.into_write_job(rows, job.finalize),
                Err(error) => WriteJob::Error { id, error },
            };

            // Use let _ to silently handle disconnected writers.
            // In shared mode, one writer dropping must not kill the encoder.
            let _ = job.reply_tx.send(result);
        }
    }
}

pub struct RowGroupEncoder {
    row_group_index: usize,
    row_group_writers: Vec<ArrowColumnWriter>,
}

impl RowGroupEncoder {
    fn try_new(
        row_group_index: usize,
        row_group_writer_factory: &ArrowRowGroupWriterFactory,
    ) -> Result<Self, ParquetError> {
        let writers = row_group_writer_factory.create_column_writers(row_group_index)?;

        Ok(RowGroupEncoder {
            row_group_index,
            row_group_writers: writers,
        })
    }

    pub fn id(&self) -> usize {
        self.row_group_index
    }

    pub fn encode(
        self,
        batches: &[RecordBatch],
        progress: Option<&Progress>,
        num_workers: usize,
    ) -> Result<PartialEncodedRowGroup, ParquetError> {
        encode_column_chunks_parallel(
            self.row_group_index,
            batches,
            self.row_group_writers,
            progress,
            num_workers,
        )
    }
}

type ThreadOutput = Vec<(usize, ArrowColumnChunk, isize)>;
type ThreadResult = Result<ThreadOutput, ParquetError>;

pub fn encode_column_chunks_parallel(
    index: usize,
    batches: &[RecordBatch],
    writers: Vec<ArrowColumnWriter>,
    progress: Option<&Progress>,
    num_workers: usize,
) -> Result<PartialEncodedRowGroup, ParquetError> {
    let writers_len = writers.len();

    if batches.is_empty() {
        return Ok(PartialEncodedRowGroup::empty(index));
    }

    let leaf_columns = collect_leaf_columns(batches, writers_len, num_workers)?;

    let num_workers = num_workers.min(writers_len);

    let work_items: Mutex<Vec<(usize, ArrowColumnWriter, Vec<ArrowLeafColumn>)>> = Mutex::new(
        writers
            .into_iter()
            .zip(leaf_columns)
            .enumerate()
            .map(|(i, (w, l))| (i, w, l))
            .collect(),
    );

    let thread_results: Vec<ThreadResult> = scope(|s| {
        let handles: Vec<_> = (0..num_workers)
            .map(|_| {
                let work_items = &work_items;
                s.spawn(move || {
                    let mut local_results = Vec::new();
                    loop {
                        let item = work_items.lock().pop();
                        let Some((idx, writer, leaves)) = item else {
                            break;
                        };
                        let result = encode_column(writer, leaves, progress)?;
                        local_results.push((idx, result.0, result.1));
                    }
                    Ok(local_results)
                })
            })
            .collect();

        handles
            .into_iter()
            .map(|h| match h.join() {
                Ok(result) => result,
                Err(_) => Err(ParquetError::General(
                    "Column encoding thread panicked".to_string(),
                )),
            })
            .collect()
    });

    let mut encoded_cols: Vec<Option<ArrowColumnChunk>> = (0..writers_len).map(|_| None).collect();
    let mut encoded_bytes = 0isize;

    for thread_result in thread_results {
        for (idx, chunk, bytes) in thread_result? {
            encoded_cols[idx] = Some(chunk);
            encoded_bytes += bytes;
        }
    }

    let encoded_cols: Vec<ArrowColumnChunk> =
        encoded_cols.into_iter().map(|slot| slot.unwrap()).collect();

    Ok(PartialEncodedRowGroup {
        id: index,
        encoded_cols,
        encoded_bytes,
    })
}

/// Encodes a single column's leaf data.
fn encode_column(
    mut writer: ArrowColumnWriter,
    leaves: Vec<ArrowLeafColumn>,
    progress: Option<&Progress>,
) -> Result<(ArrowColumnChunk, isize), ParquetError> {
    let mut prev_bytes = writer.get_estimated_total_bytes() as isize;
    let mut total_bytes = prev_bytes;

    if let Some(progress) = progress {
        progress.update_encoded_bytes(prev_bytes);
    }

    for leaf in leaves {
        writer.write(&leaf)?;
        let current_bytes = writer.get_estimated_total_bytes() as isize;
        let delta = current_bytes - prev_bytes;
        if let Some(progress) = progress {
            progress.update_encoded_bytes(delta);
        }
        total_bytes += delta;
        prev_bytes = current_bytes;
    }

    let chunk = writer.close()?;
    Ok((chunk, total_bytes))
}

fn collect_leaf_columns(
    batches: &[RecordBatch],
    writers_len: usize,
    num_workers: usize,
) -> Result<Vec<Vec<ArrowLeafColumn>>, ParquetError> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }

    if batches.len() == 1 || num_workers == 1 {
        return compute_batch_leaves(&batches[0], writers_len).map(|mut result| {
            for batch in &batches[1..] {
                let leaves = compute_batch_leaves(batch, writers_len).unwrap();
                merge_batch_leaves(&mut result, leaves).unwrap();
            }
            result
        });
    }

    let num_workers = num_workers.min(batches.len());
    let next_idx = AtomicUsize::new(0);

    let thread_results: Vec<Result<Vec<Vec<ArrowLeafColumn>>, ParquetError>> =
        std::thread::scope(|s| {
            let handles: Vec<_> = (0..num_workers)
                .map(|_| {
                    let next_idx = &next_idx;
                    s.spawn(move || {
                        let mut local: Option<Vec<Vec<ArrowLeafColumn>>> = None;
                        loop {
                            let idx = next_idx.fetch_add(1, Ordering::Relaxed);
                            if idx >= batches.len() {
                                break;
                            }
                            let leaves = compute_batch_leaves(&batches[idx], writers_len)?;
                            match &mut local {
                                Some(acc) => merge_batch_leaves(acc, leaves)?,
                                None => local = Some(leaves),
                            }
                        }
                        Ok(local.unwrap_or_default())
                    })
                })
                .collect();

            handles
                .into_iter()
                .map(|h| match h.join() {
                    Ok(result) => result,
                    Err(_) => Err(ParquetError::General(
                        "Leaf column collection thread panicked".to_string(),
                    )),
                })
                .collect()
        });

    let mut combined: Option<Vec<Vec<ArrowLeafColumn>>> = None;
    for thread_result in thread_results {
        let leaves = thread_result?;
        if leaves.is_empty() {
            continue;
        }
        match &mut combined {
            Some(acc) => merge_batch_leaves(acc, leaves)?,
            None => combined = Some(leaves),
        }
    }

    Ok(combined.unwrap_or_default())
}

fn compute_batch_leaves(
    batch: &RecordBatch,
    writers_len: usize,
) -> Result<Vec<Vec<ArrowLeafColumn>>, ParquetError> {
    let mut cols: Vec<Vec<ArrowLeafColumn>> = Vec::with_capacity(writers_len);
    let schema = batch.schema();

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let array = batch.column(col_idx);
        let leaves = compute_leaves(field, array)?;

        for leaf in leaves {
            cols.push(vec![leaf]);
        }
    }

    if cols.len() != writers_len {
        return Err(ParquetError::General(format!(
            "Mismatched leaf column counts: {} (expected) vs {} (actual)",
            writers_len,
            cols.len(),
        )));
    }

    Ok(cols)
}

fn merge_batch_leaves(
    result: &mut [Vec<ArrowLeafColumn>],
    leaf_batch: Vec<Vec<ArrowLeafColumn>>,
) -> Result<(), ParquetError> {
    if result.len() != leaf_batch.len() {
        return Err(ParquetError::General(format!(
            "Mismatched leaf column counts: {} (expected) vs {} (actual)",
            result.len(),
            leaf_batch.len()
        )));
    }

    for (i, leaf) in leaf_batch.into_iter().enumerate() {
        result[i].extend(leaf);
    }

    Ok(())
}
