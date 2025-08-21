use monitoring::telemetry::metrics;

pub struct MetricsRegistry {
    pub bytes_read: metrics::ReadableCounter,
    pub blocks_read: metrics::ReadableCounter,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self {
            bytes_read: metrics::ReadableCounter::new(
                "BYTES_READ",
                "Tracks bytes read from dataset",
            ),
            blocks_read: metrics::ReadableCounter::new(
                "BLOCKS_READ",
                "Tracks blocks read from dataset",
            ),
        }
    }
}
