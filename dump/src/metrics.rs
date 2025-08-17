#![allow(unused)]

use std::sync::Arc;

use monitoring::telemetry::metrics;

pub struct ParquetWriterMetrics {
    pub files_written: metrics::Counter,
    pub bytes_written: metrics::Counter,
}

impl ParquetWriterMetrics {
    pub fn new() -> Arc<Self> {
        let files_written =
            metrics::Counter::new("files_written", "Count of parquet files written");
        let bytes_written = metrics::Counter::new(
            "bytes_written",
            "Bytes written to the parquet writer, possibly unflushed. Counts towards the partition size limit, but does not exactly correspond to the size of the parquet files written.",
        );

        Arc::new(ParquetWriterMetrics {
            files_written,
            bytes_written,
        })
    }

    pub fn inc_files_written(&self) {
        self.files_written.inc();
    }

    pub fn add_bytes_written(&self, bytes: u64) {
        self.bytes_written.inc_by(bytes);
    }
}
