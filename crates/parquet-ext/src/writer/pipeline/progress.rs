use std::sync::{
    Arc,
    atomic::{AtomicIsize, AtomicUsize, Ordering},
};

#[derive(Default)]
pub struct Progress {
    inner: Arc<ProgressInner>,
}

#[derive(Default)]
struct ProgressInner {
    /// Rows buffered in RowGroupBuilder (not yet sent to encoder).
    buffered_rows: AtomicUsize,
    /// Estimated memory size of buffered batches.
    buffered_bytes: AtomicUsize,
    /// Rows currently being encoded.
    encoding_rows: AtomicUsize,
    /// Estimated size of data being encoded.
    /// Signed because initial estimates shrink as compression occurs.
    encoding_bytes: AtomicIsize,
    /// Bytes written to output.
    bytes_written: AtomicUsize,
    /// Rows written to output.
    rows_written: AtomicUsize,
}

impl Progress {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_buffered(&self, rows: usize, bytes: usize) {
        self.inner.buffered_rows.fetch_add(rows, Ordering::Relaxed);
        self.inner
            .buffered_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn move_to_encoding(&self, rows: usize, bytes: usize) {
        self.inner.buffered_rows.fetch_sub(rows, Ordering::Relaxed);
        self.inner
            .buffered_bytes
            .fetch_sub(bytes, Ordering::Relaxed);
        self.inner.encoding_rows.fetch_add(rows, Ordering::Relaxed);
    }

    pub fn update_encoded_bytes(&self, delta: isize) {
        self.inner
            .encoding_bytes
            .fetch_add(delta, Ordering::Relaxed);
    }

    pub fn finish_encoding(&self, rows: usize, bytes: isize) {
        self.inner.encoding_rows.fetch_sub(rows, Ordering::Relaxed);
        self.inner
            .encoding_bytes
            .fetch_sub(bytes, Ordering::Relaxed);
    }

    pub fn record_write(&self, rows: usize, bytes: usize) {
        self.inner.rows_written.fetch_add(rows, Ordering::Relaxed);
        self.inner.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn buffered_bytes(&self) -> usize {
        self.inner.buffered_bytes.load(Ordering::Relaxed)
    }

    pub fn buffered_rows(&self) -> usize {
        self.inner.buffered_rows.load(Ordering::Relaxed)
    }

    pub fn encoding_bytes(&self) -> usize {
        self.inner.encoding_bytes.load(Ordering::Relaxed).max(0) as usize
    }

    pub fn encoding_rows(&self) -> usize {
        self.inner.encoding_rows.load(Ordering::Relaxed)
    }

    pub fn in_progress_rows(&self) -> usize {
        self.buffered_rows() + self.encoding_rows()
    }

    pub fn in_progress_size(&self) -> usize {
        self.buffered_bytes() + self.encoding_bytes()
    }

    pub fn memory_size(&self) -> usize {
        self.buffered_bytes() + self.encoding_bytes()
    }

    pub fn bytes_written(&self) -> usize {
        self.inner.bytes_written.load(Ordering::Relaxed)
    }

    pub fn rows_written(&self) -> usize {
        self.inner.rows_written.load(Ordering::Relaxed)
    }
}

impl Clone for Progress {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}
