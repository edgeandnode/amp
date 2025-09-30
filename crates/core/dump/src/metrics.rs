use std::time::Duration;

use common::BlockNum;
use monitoring::telemetry;

/// The recommended interval at which to export metrics when running the `dump` command.
/// If the export interval is set to a higher value (less frequent), there is a risk
/// some observation points will not be exported.
pub const RECOMMENDED_METRICS_EXPORT_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub struct MetricsRegistry {
    // Row metrics, expressed in total rows dumped. Most metrics backends have a rate of change
    // function which can be used to turn these metrics into rows per second.
    pub raw_dataset_rows: telemetry::metrics::Counter,
    pub sql_dataset_rows: telemetry::metrics::Counter,

    // File metrics.
    pub raw_dataset_files_written: telemetry::metrics::Counter,
    pub sql_dataset_files_written: telemetry::metrics::Counter,

    // Byte metrics.
    pub raw_dataset_bytes_written: telemetry::metrics::Counter,
    pub sql_dataset_bytes_written: telemetry::metrics::Counter,

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
            raw_dataset_rows: telemetry::metrics::Counter::new(
                meter,
                "raw_dataset_rows_dumped",
                "Counter for raw dataset rows processed",
            ),
            sql_dataset_rows: telemetry::metrics::Counter::new(
                meter,
                "sql_dataset_rows_dumped",
                "Counter for SQL dataset rows processed",
            ),
            raw_dataset_files_written: telemetry::metrics::Counter::new(
                meter,
                "raw_dataset_files_written",
                "Counter for raw dataset files written",
            ),
            sql_dataset_files_written: telemetry::metrics::Counter::new(
                meter,
                "sql_dataset_files_written",
                "Counter for SQL dataset files written",
            ),
            raw_dataset_bytes_written: telemetry::metrics::Counter::new(
                meter,
                "raw_dataset_bytes_written",
                "Counter for raw dataset bytes written",
            ),
            sql_dataset_bytes_written: telemetry::metrics::Counter::new(
                meter,
                "sql_dataset_bytes_written",
                "Counter for SQL dataset bytes written",
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

    pub(crate) fn inc_sql_dataset_rows_by(
        &self,
        amount: u64,
        dataset: String,
        table: String,
        location_id: i64,
    ) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("dataset", dataset),
            telemetry::metrics::KeyValue::new("table", table),
            telemetry::metrics::KeyValue::new("location_id", location_id),
        ];
        self.sql_dataset_rows.inc_by_with_kvs(amount, &kv_pairs);
    }

    pub(crate) fn inc_sql_dataset_files_written(&self, dataset: String) {
        let kv_pairs = [telemetry::metrics::KeyValue::new("dataset", dataset)];
        self.sql_dataset_files_written.inc_with_kvs(&kv_pairs);
    }

    pub(crate) fn inc_sql_dataset_bytes_written_by(
        &self,
        amount: u64,
        dataset: String,
        table: String,
        location_id: i64,
    ) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("dataset", dataset),
            telemetry::metrics::KeyValue::new("table", table),
            telemetry::metrics::KeyValue::new("location_id", location_id),
        ];
        self.sql_dataset_bytes_written
            .inc_by_with_kvs(amount, &kv_pairs);
    }

    pub(crate) fn inc_raw_dataset_rows_by(
        &self,
        amount: u64,
        dataset: String,
        table: String,
        location_id: i64,
    ) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("dataset", dataset),
            telemetry::metrics::KeyValue::new("table", table.clone()),
            telemetry::metrics::KeyValue::new("location_id", location_id),
        ];
        self.raw_dataset_rows.inc_by_with_kvs(amount, &kv_pairs);
    }

    pub(crate) fn inc_raw_dataset_files_written(&self, dataset: String) {
        let kv_pairs = [telemetry::metrics::KeyValue::new("dataset", dataset)];
        self.raw_dataset_files_written.inc_with_kvs(&kv_pairs);
    }

    pub(crate) fn inc_raw_dataset_bytes_written_by(
        &self,
        amount: u64,
        dataset: String,
        table: String,
        location_id: i64,
    ) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("dataset", dataset),
            telemetry::metrics::KeyValue::new("table", table),
            telemetry::metrics::KeyValue::new("location_id", location_id),
        ];
        self.raw_dataset_bytes_written
            .inc_by_with_kvs(amount, &kv_pairs);
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
