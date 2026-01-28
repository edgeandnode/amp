use datasets_common::network_id::NetworkId;
use monitoring::telemetry;

#[derive(Debug, Clone)]
pub struct MetricsRegistry {
    /// Total number of EVM RPC requests made
    pub rpc_requests: telemetry::metrics::Counter,

    /// Duration of EVM RPC requests
    pub rpc_request_duration: telemetry::metrics::Histogram<f64>,

    /// Total number of EVM RPC errors encountered
    pub rpc_errors: telemetry::metrics::Counter,

    /// Size of RPC batch requests
    pub rpc_batch_size: telemetry::metrics::Histogram<u64>,

    /// Request payload size in bytes
    pub rpc_request_bytes: telemetry::metrics::Histogram<u64>,

    /// Response payload size in bytes
    pub rpc_response_bytes: telemetry::metrics::Histogram<u64>,
}

impl MetricsRegistry {
    pub fn new(meter: &telemetry::metrics::Meter) -> Self {
        Self {
            rpc_requests: telemetry::metrics::Counter::new(
                meter,
                "evm_rpc_requests_total",
                "Total number of EVM RPC requests",
            ),
            rpc_request_duration: telemetry::metrics::Histogram::new_f64(
                meter,
                "evm_rpc_request_duration",
                "Duration of EVM RPC requests",
                "milliseconds",
            ),
            rpc_errors: telemetry::metrics::Counter::new(
                meter,
                "evm_rpc_errors_total",
                "Total number of EVM RPC errors",
            ),
            rpc_batch_size: telemetry::metrics::Histogram::new_u64(
                meter,
                "evm_rpc_batch_size_requests",
                "Number of requests per RPC batch",
                "requests",
            ),
            rpc_request_bytes: telemetry::metrics::Histogram::new_u64(
                meter,
                "evm_rpc_request_bytes",
                "Size of RPC request payloads",
                "bytes",
            ),
            rpc_response_bytes: telemetry::metrics::Histogram::new_u64(
                meter,
                "evm_rpc_response_bytes",
                "Size of RPC response payloads",
                "bytes",
            ),
        }
    }

    /// Record a single RPC request
    pub(crate) fn record_single_request(
        &self,
        duration_millis: f64,
        provider: &str,
        network: &NetworkId,
        method: &str,
    ) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("provider", provider.to_string()),
            telemetry::metrics::KeyValue::new("network", network.to_string()),
            telemetry::metrics::KeyValue::new("method", method.to_string()),
        ];
        self.rpc_requests.inc_with_kvs(&kv_pairs);
        self.rpc_request_duration
            .record_with_kvs(duration_millis, &kv_pairs);
    }

    /// Record a batch RPC request
    pub(crate) fn record_batch_request(
        &self,
        duration_millis: f64,
        batch_size: u64,
        provider: &str,
        network: &NetworkId,
    ) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("provider", provider.to_string()),
            telemetry::metrics::KeyValue::new("network", network.to_string()),
        ];
        self.rpc_requests.inc_with_kvs(&kv_pairs);
        self.rpc_request_duration
            .record_with_kvs(duration_millis, &kv_pairs);
        self.rpc_batch_size.record_with_kvs(batch_size, &kv_pairs);
    }

    /// Record RPC error
    pub(crate) fn record_error(&self, provider: &str, network: &NetworkId) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("provider", provider.to_string()),
            telemetry::metrics::KeyValue::new("network", network.to_string()),
        ];
        self.rpc_errors.inc_with_kvs(&kv_pairs);
    }
}
