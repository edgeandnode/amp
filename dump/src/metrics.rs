#![allow(unused)]

use std::sync::Arc;

use prometheus::{IntCounterVec, Opts, register};

pub struct ParquetWriterMetrics {
    pub files_written: IntCounterVec,
    pub bytes_written: IntCounterVec,
}

impl ParquetWriterMetrics {
    pub fn new() -> Arc<Self> {
        let files_written_opts = Opts::new("files_written", "Count of parquet files written.");

        let bytes_written_opts = Opts::new(
            "bytes_written",
            "Bytes written to the parquet writer, possibly unflushed.
            Counts towards the partition size limit, \
            but does not exactly correspond to the size of the parquet files written.",
        );

        let labels = &["dataset", "network", "location", "table"];
        let files_written = IntCounterVec::new(files_written_opts, labels).unwrap();
        let bytes_written = IntCounterVec::new(bytes_written_opts, labels).unwrap();

        register(Box::new(files_written.clone())).unwrap();
        register(Box::new(bytes_written.clone())).unwrap();

        Arc::new(ParquetWriterMetrics {
            files_written,
            bytes_written,
        })
    }

    pub fn inc_files_written(&self, dataset: &str, location: &str, table: &str) {
        self.files_written
            .with_label_values(&[dataset, location, table])
            .inc();
    }

    pub fn add_bytes_written(&self, dataset: &str, location: &str, table: &str, bytes: u64) {
        self.bytes_written
            .with_label_values(&[dataset, location, table])
            .inc_by(bytes);
    }
}
