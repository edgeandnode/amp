use std::{sync::LazyLock, time::Duration};

use monitoring::telemetry;

/// The recommended interval at which to export metrics when running the `dump` command.
/// If the export interval is set to a higher value (less frequent), there is a risk
/// some observation points will not be exported.
pub const RECOMMENDED_METRICS_EXPORT_INTERVAL: Duration = Duration::from_secs(1);

static METRICS: LazyLock<MetricsRegistry> = LazyLock::new(|| MetricsRegistry::new());

/// Dataset dump throughput metrics. Expressed as total number of rows dumped. Metrics backends
/// such as Prometheus should allow querying for the rate of change, turning the metrics into rows
/// per second.
pub(crate) mod throughput {
    use super::*;

    pub(crate) fn raw(rows: u64, dataset: String) {
        let kv_pairs = [telemetry::metrics::KeyValue::new("dataset", dataset)];
        METRICS.raw_dataset_rows.inc_by_with_kvs(rows, &kv_pairs);
    }

    pub(crate) fn sql(rows: u64, dataset: String) {
        let kv_pairs = [telemetry::metrics::KeyValue::new("dataset", dataset)];
        METRICS.sql_dataset_rows.inc_by_with_kvs(rows, &kv_pairs);
    }
}

struct MetricsRegistry {
    raw_dataset_rows: telemetry::metrics::Counter,
    sql_dataset_rows: telemetry::metrics::Counter,
}

impl MetricsRegistry {
    fn new() -> Self {
        Self {
            raw_dataset_rows: telemetry::metrics::Counter::new(
                "raw_dataset_rows_dumped",
                "Counter for raw dataset rows processed",
            ),
            sql_dataset_rows: telemetry::metrics::Counter::new(
                "sql_dataset_rows_dumped",
                "Counter for SQL dataset rows processed",
            ),
        }
    }
}
