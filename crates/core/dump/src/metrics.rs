use std::time::Duration;

use common::BlockNum;
use monitoring::telemetry;

/// The recommended interval at which to export metrics when running the `dump` command.
/// If the export interval is set to a higher value (less frequent), there is a risk
/// some observation points will not be exported.
pub const RECOMMENDED_METRICS_EXPORT_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub struct MetricsRegistry {
    /// Total rows ingested across all dataset types
    pub rows_ingested: telemetry::metrics::Counter,

    /// Total files written across all dataset types
    pub files_written: telemetry::metrics::Counter,

    /// Total bytes written across all dataset types
    pub bytes_written: telemetry::metrics::Counter,

    /// Latest block number successfully dumped for a dataset/table
    pub latest_block: telemetry::metrics::Gauge<u64>,

    /// Duration of dump operations from start to completion in milliseconds
    pub dump_duration: telemetry::metrics::Histogram<f64>,

    /// Count of dump errors
    pub dump_errors: telemetry::metrics::Counter,

    /// Number of files read (input) during compaction
    pub compaction_files_read: telemetry::metrics::Counter,

    /// Total bytes read before compaction
    pub compaction_bytes_read: telemetry::metrics::Counter,

    /// Total bytes written after compaction
    pub compaction_bytes_written: telemetry::metrics::Counter,

    /// Duration of compaction operations in milliseconds
    pub compaction_duration: telemetry::metrics::Histogram<f64>,

    // Compaction metrics.
    pub successful_compactions: telemetry::metrics::Counter,
    pub failed_compactions: telemetry::metrics::Counter,

    // Garbage Collector metrics.
    pub files_deleted: telemetry::metrics::Counter,
    pub files_not_found: telemetry::metrics::Counter,
    pub files_failed_to_delete: telemetry::metrics::Counter,
}

impl MetricsRegistry {
    pub fn new(meter: &telemetry::metrics::Meter) -> Self {
        Self {
            rows_ingested: telemetry::metrics::Counter::new(
                meter,
                "rows_ingested_total",
                "Total number of rows ingested into datasets",
            ),
            files_written: telemetry::metrics::Counter::new(
                meter,
                "files_written_total",
                "Total number of files written for datasets",
            ),
            bytes_written: telemetry::metrics::Counter::new(
                meter,
                "bytes_written_total",
                "Total bytes written for datasets (uncompressed)",
            ),
            latest_block: telemetry::metrics::Gauge::new_u64(
                meter,
                "latest_block_number",
                "Latest block number successfully dumped for a dataset/table",
                "block_number",
            ),
            dump_duration: telemetry::metrics::Histogram::new_f64(
                meter,
                "dump_duration_milliseconds",
                "Time from dump job start to completion",
                "milliseconds",
            ),
            dump_errors: telemetry::metrics::Counter::new(
                meter,
                "dump_errors_total",
                "Total number of dump errors encountered",
            ),
            compaction_files_read: telemetry::metrics::Counter::new(
                meter,
                "compaction_files_read_total",
                "Number of files read (input) during compaction",
            ),
            compaction_bytes_read: telemetry::metrics::Counter::new(
                meter,
                "compaction_bytes_read_total",
                "Total bytes read before compaction",
            ),
            compaction_bytes_written: telemetry::metrics::Counter::new(
                meter,
                "compaction_bytes_written_total",
                "Total bytes written after compaction",
            ),
            compaction_duration: telemetry::metrics::Histogram::new_f64(
                meter,
                "compaction_duration_milliseconds",
                "Duration of compaction operations",
                "milliseconds",
            ),
            successful_compactions: telemetry::metrics::Counter::new(
                meter,
                "successful_compactions",
                "Counter for successful compaction operations",
            ),
            failed_compactions: telemetry::metrics::Counter::new(
                meter,
                "failed_compactions",
                "Counter for failed compaction operations",
            ),
            files_deleted: telemetry::metrics::Counter::new(
                meter,
                "files_deleted",
                "Counter for files successfully deleted by the garbage collector",
            ),
            files_not_found: telemetry::metrics::Counter::new(
                meter,
                "files_not_found",
                "Counter for files not found during garbage collection",
            ),
            files_failed_to_delete: telemetry::metrics::Counter::new(
                meter,
                "files_failed_to_delete",
                "Counter for files that failed to delete during garbage collection",
            ),
        }
    }

    /// Record rows ingested
    pub(crate) fn record_ingestion_rows(
        &self,
        rows: u64,
        dataset: String,
        dataset_version: String,
        table: String,
        location_id: i64,
    ) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("dataset", dataset),
            telemetry::metrics::KeyValue::new("version", dataset_version),
            telemetry::metrics::KeyValue::new("table", table),
            telemetry::metrics::KeyValue::new("location_id", location_id),
        ];
        self.rows_ingested.inc_by_with_kvs(rows, &kv_pairs);
    }

    /// Record bytes written
    pub(crate) fn record_ingestion_bytes(
        &self,
        bytes: u64,
        dataset: String,
        dataset_version: String,
        table: String,
        location_id: i64,
    ) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("dataset", dataset),
            telemetry::metrics::KeyValue::new("version", dataset_version),
            telemetry::metrics::KeyValue::new("table", table),
            telemetry::metrics::KeyValue::new("location_id", location_id),
        ];
        self.bytes_written.inc_by_with_kvs(bytes, &kv_pairs);
    }

    /// Record a file being written
    pub(crate) fn record_file_written(
        &self,
        dataset: String,
        dataset_version: String,
        table: String,
        location_id: i64,
    ) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("dataset", dataset),
            telemetry::metrics::KeyValue::new("version", dataset_version),
            telemetry::metrics::KeyValue::new("table", table),
            telemetry::metrics::KeyValue::new("location_id", location_id),
        ];

        self.files_written.inc_with_kvs(&kv_pairs);
    }

    /// Update the latest block number for a dataset/table
    pub(crate) fn set_latest_block(
        &self,
        block_number: u64,
        dataset: String,
        dataset_version: String,
        table: String,
        location_id: i64,
    ) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("dataset", dataset),
            telemetry::metrics::KeyValue::new("version", dataset_version),
            telemetry::metrics::KeyValue::new("table", table),
            telemetry::metrics::KeyValue::new("location_id", location_id),
        ];
        self.latest_block.record_with_kvs(block_number, &kv_pairs);
    }

    /// Record duration of a dump operation
    pub(crate) fn record_dump_duration(
        &self,
        duration_millis: f64,
        dataset: String,
        dataset_version: String,
        table: String,
        job_id: String,
    ) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("dataset", dataset),
            telemetry::metrics::KeyValue::new("version", dataset_version),
            telemetry::metrics::KeyValue::new("table", table),
            telemetry::metrics::KeyValue::new("job_id", job_id),
        ];
        self.dump_duration
            .record_with_kvs(duration_millis, &kv_pairs);
    }

    /// Record a dump error
    pub(crate) fn record_dump_error(
        &self,
        dataset: String,
        dataset_version: String,
        table: String,
    ) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("dataset", dataset),
            telemetry::metrics::KeyValue::new("version", dataset_version),
            telemetry::metrics::KeyValue::new("table", table),
        ];
        self.dump_errors.inc_with_kvs(&kv_pairs);
    }

    /// Record compaction operation
    ///
    /// Note: Compaction ratio can be calculated in Prometheus as:
    /// `compaction_bytes_written_total / compaction_bytes_read_total`
    pub(crate) fn record_compaction(
        &self,
        dataset: String,
        table: String,
        location_id: i64,
        input_file_count: u64,
        input_bytes: u64,
        output_bytes: u64,
        duration_millis: f64,
    ) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("dataset", dataset),
            telemetry::metrics::KeyValue::new("table", table),
            telemetry::metrics::KeyValue::new("location_id", location_id),
        ];

        self.compaction_files_read
            .inc_by_with_kvs(input_file_count, &kv_pairs);
        self.compaction_bytes_read
            .inc_by_with_kvs(input_bytes, &kv_pairs);
        self.compaction_bytes_written
            .inc_by_with_kvs(output_bytes, &kv_pairs);
        self.compaction_duration
            .record_with_kvs(duration_millis, &kv_pairs);
    }

    pub(crate) fn inc_successful_compactions(
        &self,
        dataset: String,
        table: String,
        range_start: BlockNum,
    ) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("dataset", dataset),
            telemetry::metrics::KeyValue::new("table", table),
            telemetry::metrics::KeyValue::new("range_start", range_start as i64),
        ];
        self.successful_compactions.inc_with_kvs(&kv_pairs);
    }

    pub(crate) fn inc_failed_compactions(&self, dataset: String, table: String) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("dataset", dataset),
            telemetry::metrics::KeyValue::new("table", table),
        ];
        self.failed_compactions.inc_with_kvs(&kv_pairs);
    }

    pub(crate) fn inc_files_deleted(&self, amount: usize, dataset: String, table: String) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("dataset", dataset),
            telemetry::metrics::KeyValue::new("table", table),
        ];
        self.files_deleted.inc_by_with_kvs(amount as u64, &kv_pairs);
    }

    pub(crate) fn inc_files_not_found(&self, amount: usize, dataset: String, table: String) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("dataset", dataset),
            telemetry::metrics::KeyValue::new("table", table),
        ];
        self.files_not_found
            .inc_by_with_kvs(amount as u64, &kv_pairs);
    }

    pub(crate) fn inc_files_failed_to_delete(&self, amount: usize, dataset: String, table: String) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("dataset", dataset),
            telemetry::metrics::KeyValue::new("table", table),
        ];
        self.files_failed_to_delete
            .inc_by_with_kvs(amount as u64, &kv_pairs);
    }
}
