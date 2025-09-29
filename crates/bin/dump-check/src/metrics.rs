use std::sync::Arc;

use monitoring::telemetry::metrics;

#[derive(Debug, Clone)]
pub struct Metrics {
    pub registry: Arc<MetricsRegistry>,
    pub meter: metrics::Meter,
}

#[derive(Debug)]
pub struct MetricsRegistry {
    pub bytes_read: metrics::ReadableCounter,
    pub blocks_read: metrics::ReadableCounter,
}

impl MetricsRegistry {
    pub fn new(meter: &metrics::Meter) -> Self {
        Self {
            bytes_read: metrics::ReadableCounter::new(
                meter,
                "BYTES_READ",
                "Tracks bytes read from dataset",
            ),
            blocks_read: metrics::ReadableCounter::new(
                meter,
                "BLOCKS_READ",
                "Tracks blocks read from dataset",
            ),
        }
    }
}
