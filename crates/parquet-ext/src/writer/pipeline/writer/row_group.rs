use std::collections::{BTreeMap, BTreeSet};

use parquet::arrow::arrow_writer::ArrowColumnChunk;

use crate::writer::pipeline::EncodedRowGroup;

#[derive(Default)]
pub struct PendingRowGroups {
    inner: BTreeMap<usize, PendingRowGroup>,
    next: usize,
    outstanding: BTreeSet<usize>,
}

pub struct PendingRowGroup {
    pub chunks: Vec<ArrowColumnChunk>,
    pub rows: usize,
    pub bytes: isize,
}

impl EncodedRowGroup {
    pub fn into_pending(self) -> (usize, PendingRowGroup) {
        (
            self.id,
            PendingRowGroup {
                bytes: self.encoded_bytes,
                rows: self.encoded_rows,
                chunks: self.encoded_cols,
            },
        )
    }
}

impl PendingRowGroups {
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn keys(&self) -> impl Iterator<Item = &usize> {
        self.inner.keys()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn has_pending(&self) -> bool {
        !self.outstanding.is_empty()
    }

    pub fn insert(&mut self, row_group: EncodedRowGroup) {
        let (id, pending) = row_group.into_pending();

        self.inner.insert(id, pending);

        let min_id = self.outstanding.first().cloned().unwrap_or(id).min(id);
        let max_id = self.outstanding.last().cloned().unwrap_or(id).max(id);
        self.outstanding.extend(min_id..=max_id);
    }

    pub fn next_ready(&mut self) -> Option<PendingRowGroup> {
        let id = self.next;
        if let Some(ready_group) = self.inner.remove(&id) {
            self.outstanding.remove(&id);
            self.next += 1;
            Some(ready_group)
        } else {
            None
        }
    }
}
