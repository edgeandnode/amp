use std::cmp::Ordering;

use parquet::arrow::arrow_writer::ArrowColumnChunk;

use crate::writer::pipeline::WriteJob;

pub struct EncodedRowGroup {
    pub id: usize,
    pub encoded_cols: Vec<ArrowColumnChunk>,
    pub encoded_bytes: isize,
    pub encoded_rows: usize,
}

pub struct PartialEncodedRowGroup {
    pub id: usize,
    pub encoded_cols: Vec<ArrowColumnChunk>,
    pub encoded_bytes: isize,
}

impl PartialEncodedRowGroup {
    pub fn empty(id: usize) -> Self {
        Self {
            id,
            encoded_cols: Vec::new(),
            encoded_bytes: 0,
        }
    }

    fn into_encoded_row_group(self, rows: usize) -> EncodedRowGroup {
        EncodedRowGroup {
            id: self.id,
            encoded_cols: self.encoded_cols,
            encoded_bytes: self.encoded_bytes,
            encoded_rows: rows,
        }
    }

    pub fn into_write_job(self, rows: usize, finalize: bool) -> WriteJob {
        let row_group = self.into_encoded_row_group(rows);
        if finalize {
            WriteJob::Finalize { row_group }
        } else {
            WriteJob::Encoded { row_group }
        }
    }
}

impl Eq for PartialEncodedRowGroup {}

impl Ord for PartialEncodedRowGroup {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl PartialEq for PartialEncodedRowGroup {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl PartialOrd for PartialEncodedRowGroup {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
