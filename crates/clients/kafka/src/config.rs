//! Configuration types for the Kafka client.

/// Default number of partitions for the Kafka topic.
pub const DEFAULT_PARTITIONS: u32 = 16;

/// Configuration for the Kafka producer.
#[derive(Debug, Clone)]
pub struct KafkaConfig {
    /// Kafka broker addresses (e.g., ["kafka-1:9092", "kafka-2:9092"])
    pub brokers: Vec<String>,

    /// Kafka topic name for worker events
    pub topic: String,

    /// Number of partitions for the Kafka topic (used for partition key hashing)
    pub partitions: u32,
}

impl KafkaConfig {
    /// Creates a new Kafka configuration.
    pub fn new(brokers: Vec<String>, topic: impl Into<String>, partitions: u32) -> Self {
        Self {
            brokers,
            topic: topic.into(),
            partitions,
        }
    }
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            brokers: vec!["localhost:9092".to_string()],
            topic: "amp.worker.events".to_string(),
            partitions: DEFAULT_PARTITIONS,
        }
    }
}
