use prometheus::{
    register_counter, Counter
};

pub struct MetricsRegistry {
    pub blocks_read: Counter,
    pub bytes_read: Counter,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        let blocks_read = register_counter!("blocks_read", "Number of blocks read from firehose")
            .expect("failed to create blocks_read counter");
        let bytes_read = register_counter!("bytes_read", "Number of bytes read from firehose")
            .expect("failed to create bytes_read counter");
        Self {
            blocks_read,
            bytes_read,
        }
    }
}