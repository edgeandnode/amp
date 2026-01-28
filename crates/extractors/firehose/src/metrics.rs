use datasets_common::network_id::NetworkId;
use monitoring::telemetry;

#[derive(Debug, Clone)]
pub struct MetricsRegistry {
    /// Total number of blocks received from Firehose
    pub blocks_received: telemetry::metrics::Counter,

    /// Duration of Firehose streaming sessions in milliseconds
    pub stream_duration: telemetry::metrics::Histogram<f64>,

    /// Total number of Firehose stream errors
    pub stream_errors: telemetry::metrics::Counter,
}

impl MetricsRegistry {
    pub fn new(meter: &telemetry::metrics::Meter) -> Self {
        Self {
            blocks_received: telemetry::metrics::Counter::new(
                meter,
                "firehose_blocks_received_total",
                "Total number of blocks received from Firehose",
            ),
            stream_duration: telemetry::metrics::Histogram::new_f64(
                meter,
                "firehose_stream_duration_milliseconds",
                "Duration of Firehose streaming sessions",
                "milliseconds",
            ),
            stream_errors: telemetry::metrics::Counter::new(
                meter,
                "firehose_stream_errors_total",
                "Total number of Firehose stream errors",
            ),
        }
    }

    /// Record a block received from Firehose
    pub(crate) fn record_block_received(&self, provider: &str, network: &NetworkId) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("provider", provider.to_string()),
            telemetry::metrics::KeyValue::new("network", network.to_string()),
        ];
        self.blocks_received.inc_with_kvs(&kv_pairs);
    }

    /// Record a stream error
    pub(crate) fn record_stream_error(
        &self,
        provider: &str,
        network: &NetworkId,
        error_type: &str,
    ) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("provider", provider.to_string()),
            telemetry::metrics::KeyValue::new("network", network.to_string()),
            telemetry::metrics::KeyValue::new("error_type", error_type.to_string()),
        ];
        self.stream_errors.inc_with_kvs(&kv_pairs);
    }

    /// Record stream session duration
    pub(crate) fn record_stream_duration(
        &self,
        duration_millis: f64,
        provider: &str,
        network: &NetworkId,
    ) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("provider", provider.to_string()),
            telemetry::metrics::KeyValue::new("network", network.to_string()),
        ];
        self.stream_duration
            .record_with_kvs(duration_millis, &kv_pairs);
    }
}
