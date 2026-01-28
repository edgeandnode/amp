use datasets_common::network_id::NetworkId;
use monitoring::telemetry;

#[derive(Debug, Clone)]
pub struct MetricsRegistry {
    /// Total number of Solana RPC requests made
    pub rpc_requests: telemetry::metrics::Counter,
    /// Duration of Solana RPC requests
    pub rpc_request_duration: telemetry::metrics::Histogram<u64>,
    /// Total number of Solana RPC errors encountered
    pub rpc_errors: telemetry::metrics::Counter,

    /// Total number of OF1 CAR file downloads
    pub of1_car_downloads: telemetry::metrics::Counter,
    /// Duration of OF1 CAR file downloads
    pub of1_car_download_duration: telemetry::metrics::Histogram<f64>,
    /// Size of downloaded OF1 CAR files in bytes
    pub of1_car_download_bytes: telemetry::metrics::Histogram<u64>,
    /// Total number of OF1 CAR download errors
    pub of1_car_download_errors: telemetry::metrics::Counter,
}

impl MetricsRegistry {
    pub fn new(meter: &telemetry::metrics::Meter) -> Self {
        Self {
            rpc_requests: telemetry::metrics::Counter::new(
                meter,
                "solana_rpc_requests_total",
                "Total number of Solana RPC requests",
            ),
            rpc_request_duration: telemetry::metrics::Histogram::new_u64(
                meter,
                "solana_rpc_request_duration_milliseconds",
                "Duration of Solana RPC requests",
                "milliseconds",
            ),
            rpc_errors: telemetry::metrics::Counter::new(
                meter,
                "solana_rpc_errors_total",
                "Total number of Solana RPC errors",
            ),
            of1_car_downloads: telemetry::metrics::Counter::new(
                meter,
                "solana_of1_car_downloads_total",
                "Total number of OF1 CAR file downloads",
            ),
            of1_car_download_duration: telemetry::metrics::Histogram::new_f64(
                meter,
                "solana_of1_car_download_duration_seconds",
                "Duration of OF1 CAR file downloads",
                "seconds",
            ),
            of1_car_download_bytes: telemetry::metrics::Histogram::new_u64(
                meter,
                "solana_of1_car_download_bytes",
                "Size of downloaded OF1 CAR files",
                "bytes",
            ),
            of1_car_download_errors: telemetry::metrics::Counter::new(
                meter,
                "solana_of1_car_download_errors_total",
                "Total number of OF1 CAR download errors",
            ),
        }
    }

    /// Record a single RPC request
    pub(crate) fn record_rpc_request(
        &self,
        duration_millis: u64,
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

    /// Record RPC error
    pub(crate) fn record_rpc_error(&self, provider: &str, network: &NetworkId) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("provider", provider.to_string()),
            telemetry::metrics::KeyValue::new("network", network.to_string()),
        ];
        self.rpc_errors.inc_with_kvs(&kv_pairs);
    }

    /// Record OF1 CAR file download
    pub(crate) fn record_of1_car_download(
        &self,
        duration_secs: f64,
        epoch: u64,
        provider: &str,
        network: &NetworkId,
    ) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("provider", provider.to_string()),
            telemetry::metrics::KeyValue::new("network", network.to_string()),
            telemetry::metrics::KeyValue::new("epoch", epoch.to_string()),
        ];
        self.of1_car_downloads.inc_with_kvs(&kv_pairs);
        self.of1_car_download_duration
            .record_with_kvs(duration_secs, &kv_pairs);
    }

    /// Record OF1 CAR file downloaded bytes
    pub(crate) fn record_of1_car_download_bytes(
        &self,
        bytes: u64,
        epoch: u64,
        provider: &str,
        network: &NetworkId,
    ) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("provider", provider.to_string()),
            telemetry::metrics::KeyValue::new("network", network.to_string()),
            telemetry::metrics::KeyValue::new("epoch", epoch.to_string()),
        ];
        self.of1_car_download_bytes
            .record_with_kvs(bytes, &kv_pairs);
    }

    /// Record OF1 CAR download error
    pub(crate) fn record_of1_car_download_error(
        &self,
        epoch: u64,
        provider: &str,
        network: &NetworkId,
    ) {
        let kv_pairs = [
            telemetry::metrics::KeyValue::new("provider", provider.to_string()),
            telemetry::metrics::KeyValue::new("network", network.to_string()),
            telemetry::metrics::KeyValue::new("epoch", epoch.to_string()),
        ];
        self.of1_car_download_errors.inc_with_kvs(&kv_pairs);
    }
}
