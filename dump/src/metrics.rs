use std::time::Duration;

use monitoring::telemetry;

/// The recommended interval at which to export metrics when running the `dump` command.
/// If the export interval is set to a higher value (less frequent), there is a risk
/// some observation points will not be exported.
pub const RECOMMENDED_METRICS_EXPORT_INTERVAL: Duration = Duration::from_secs(1);

pub struct MetricsRegistry {
    // Row metrics, expressed in total rows dumped. Most metrics backends have a rate of change
    // function which can be used to turn these metrics into rows per second.
    raw_dataset_rows: telemetry::metrics::Counter,
    sql_dataset_rows: telemetry::metrics::Counter,

    // File metrics.
    raw_dataset_files_written: telemetry::metrics::Counter,
    sql_dataset_files_written: telemetry::metrics::Counter,

    // Byte metrics.
    raw_dataset_bytes_written: telemetry::metrics::Counter,
    sql_dataset_bytes_written: telemetry::metrics::Counter,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self {
            raw_dataset_rows: telemetry::metrics::Counter::new(
                "raw_dataset_rows_dumped",
                "Counter for raw dataset rows processed",
            ),
            sql_dataset_rows: telemetry::metrics::Counter::new(
                "sql_dataset_rows_dumped",
                "Counter for SQL dataset rows processed",
            ),
            raw_dataset_files_written: telemetry::metrics::Counter::new(
                "raw_dataset_files_written",
                "Counter for raw dataset files written",
            ),
            sql_dataset_files_written: telemetry::metrics::Counter::new(
                "sql_dataset_files_written",
                "Counter for SQL dataset files written",
            ),
            raw_dataset_bytes_written: telemetry::metrics::Counter::new(
                "raw_dataset_bytes_written",
                "Counter for raw dataset bytes written",
            ),
            sql_dataset_bytes_written: telemetry::metrics::Counter::new(
                "sql_dataset_bytes_written",
                "Counter for SQL dataset bytes written",
            ),
        }
    }

    /// Raw dataset dump metrics.
    pub fn inc_raw_rows(&self, rows: u64, dataset: String) {
        let kv_pairs = [telemetry::metrics::KeyValue::new("dataset", dataset)];
        self.raw_dataset_rows.inc_by_with_kvs(rows, &kv_pairs);
    }

    pub fn inc_raw_files(&self, dataset: String) {
        let kv_pairs = [telemetry::metrics::KeyValue::new("dataset", dataset)];
        self.raw_dataset_files_written.inc_with_kvs(&kv_pairs);
    }

    pub fn inc_raw_bytes(&self, bytes: u64, dataset: String) {
        let kv_pairs = [telemetry::metrics::KeyValue::new("dataset", dataset)];
        self.raw_dataset_bytes_written
            .inc_by_with_kvs(bytes, &kv_pairs);
    }

    /// SQL dataset dump metrics.
    pub fn inc_sql_rows(&self, rows: u64, dataset: String) {
        let kv_pairs = [telemetry::metrics::KeyValue::new("dataset", dataset)];
        self.sql_dataset_rows.inc_by_with_kvs(rows, &kv_pairs);
    }

    pub fn inc_sql_files(&self, dataset: String) {
        let kv_pairs = [telemetry::metrics::KeyValue::new("dataset", dataset)];
        self.sql_dataset_files_written.inc_with_kvs(&kv_pairs);
    }

    pub fn inc_sql_bytes(&self, bytes: u64, dataset: String) {
        let kv_pairs = [telemetry::metrics::KeyValue::new("dataset", dataset)];
        self.sql_dataset_bytes_written
            .inc_by_with_kvs(bytes, &kv_pairs);
    }
}
