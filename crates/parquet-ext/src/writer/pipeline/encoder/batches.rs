use arrow_array::RecordBatch;

use crate::writer::pipeline::Progress;

pub struct Batches {
    batches: Vec<RecordBatch>,
    pub rows: usize,
    pub bytes: usize,
    pub max_rows: usize,
}

impl Batches {
    pub fn new(max_rows: usize) -> Self {
        Batches {
            batches: Vec::new(),
            rows: 0,
            bytes: 0,
            max_rows,
        }
    }

    pub fn add_batch(&mut self, batch: RecordBatch, progress: Option<&Progress>) {
        let batch_bytes = batch.get_array_memory_size();
        let batch_rows = batch.num_rows();
        self.batches.push(batch);
        self.rows += batch_rows;
        self.bytes += batch_bytes;
        if let Some(progress) = progress {
            progress.add_buffered(batch_rows, batch_bytes);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    pub fn take(&mut self, progress: Option<&Progress>) -> (Vec<RecordBatch>, usize) {
        if let Some(progress) = progress {
            progress.move_to_encoding(self.rows, self.bytes);
        }
        let rows = self.rows;
        self.rows = 0;
        self.bytes = 0;
        (std::mem::take(&mut self.batches), rows)
    }
}
