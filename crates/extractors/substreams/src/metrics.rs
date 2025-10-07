use monitoring::telemetry;

#[derive(Debug, Clone)]
pub struct MetricsRegistry {
    /// Total number of blocks processed from Substreams
    pub blocks_processed: telemetry::metrics::Counter,

    /// Duration of Substreams streaming sessions in milliseconds
    pub stream_duration: telemetry::metrics::Histogram<f64>,

    /// Total number of Substreams stream errors
    pub stream_errors: telemetry::metrics::Counter,
}

impl MetricsRegistry {
    pub fn new(meter: &telemetry::metrics::Meter) -> Self {
        Self {
            blocks_processed: telemetry::metrics::Counter::new(
                meter,
                "substreams_blocks_processed_total",
                "Total number of blocks processed from Substreams",
            ),
            stream_duration: telemetry::metrics::Histogram::new_f64(
                meter,
                "substreams_stream_duration_milliseconds",
                "Duration of Substreams streaming sessions",
                "milliseconds",
            ),
            stream_errors: telemetry::metrics::Counter::new(
                meter,
                "substreams_stream_errors_total",
                "Total number of Substreams stream errors",
            ),
        }
    }

    /// Record a block processed from Substreams
    pub(crate) fn record_block_processed(&self, provider: &str, network: &str, module: &str) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("provider", provider.to_string()),
            telemetry::metrics::KeyValue::new("network", network.to_string()),
            telemetry::metrics::KeyValue::new("module", module.to_string()),
        ];
        self.blocks_processed.inc_with_kvs(&kv_pairs);
    }

    /// Record a stream error
    pub(crate) fn record_stream_error(
        &self,
        provider: &str,
        network: &str,
        module: &str,
        error_type: &str,
    ) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("provider", provider.to_string()),
            telemetry::metrics::KeyValue::new("network", network.to_string()),
            telemetry::metrics::KeyValue::new("module", module.to_string()),
            telemetry::metrics::KeyValue::new("error_type", error_type.to_string()),
        ];
        self.stream_errors.inc_with_kvs(&kv_pairs);
    }

    /// Record stream session duration
    pub(crate) fn record_stream_duration(
        &self,
        duration_millis: f64,
        provider: &str,
        network: &str,
        module: &str,
    ) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("provider", provider.to_string()),
            telemetry::metrics::KeyValue::new("network", network.to_string()),
            telemetry::metrics::KeyValue::new("module", module.to_string()),
        ];
        self.stream_duration
            .record_with_kvs(duration_millis, &kv_pairs);
    }
}
