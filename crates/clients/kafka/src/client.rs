//! Kafka producer for worker events.

use std::{sync::Arc, time::Duration};

use amp_config::KafkaEventsConfig;
use backon::{BackoffBuilder, Retryable};
use monitoring::logging;
use rskafka::{
    client::{
        Client, ClientBuilder,
        partition::{Compression, UnknownTopicHandling},
    },
    record::Record,
};

use crate::error::Error;

/// Fixed delay backoff policy: 1s, 5s, 60s.
///
/// This implements the documented retry policy for Kafka event sending.
struct FixedDelayBackoff;

/// Iterator that yields fixed delays: 1s, 5s, 60s.
struct FixedDelayIter {
    delays: std::vec::IntoIter<Duration>,
}

impl Iterator for FixedDelayIter {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        self.delays.next()
    }
}

impl BackoffBuilder for FixedDelayBackoff {
    type Backoff = FixedDelayIter;

    fn build(self) -> Self::Backoff {
        FixedDelayIter {
            delays: vec![
                Duration::from_secs(1),
                Duration::from_secs(5),
                Duration::from_secs(60),
            ]
            .into_iter(),
        }
    }
}

/// Kafka producer for sending worker events.
///
/// The producer is thread-safe and can be shared across tasks via `Arc`.
pub struct KafkaProducer {
    client: Arc<Client>,
    topic: String,
    partitions: u32,
}

impl KafkaProducer {
    /// Creates a new Kafka producer with the given configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection to the Kafka brokers fails.
    pub async fn new(config: &KafkaEventsConfig) -> Result<Self, Error> {
        let client = ClientBuilder::new(config.brokers.clone())
            .build()
            .await
            .map_err(Error::Connection)?;

        Ok(Self {
            client: Arc::new(client),
            topic: config.topic.clone(),
            partitions: config.partitions,
        })
    }

    /// Sends an event to Kafka with retry logic.
    ///
    /// Events are partitioned by the partition key (table discriminator).
    /// Retries 3 times with fixed delays: 1s, 5s, 60s.
    ///
    /// # Arguments
    ///
    /// * `partition_key` - The partition key for the event (e.g., "{namespace}/{name}/{manifest_hash}/{table_name}")
    /// * `payload` - The protobuf-encoded event payload
    ///
    /// # Errors
    ///
    /// Returns an error if all retry attempts fail.
    pub async fn send(&self, partition_key: &str, payload: &[u8]) -> Result<(), Error> {
        let partition = self.partition_for_key(partition_key);

        let partition_client = self
            .client
            .partition_client(&self.topic, partition, UnknownTopicHandling::Retry)
            .await
            .map_err(Error::PartitionClient)?;

        let record = Record {
            key: Some(partition_key.as_bytes().to_vec()),
            value: Some(payload.to_vec()),
            headers: Default::default(),
            timestamp: chrono::Utc::now(),
        };

        // Retry with fixed delays: 1s, 5s, 60s
        (|| async {
            partition_client
                .produce(vec![record.clone()], Compression::Gzip)
                .await
                .map_err(|e| Error::Send(e.to_string()))
        })
        .retry(Self::retry_policy())
        .when(|e| e.is_retryable())
        .notify(|err, dur| {
            tracing::warn!(
                error = %err,
                error_source = logging::error_source(&err),
                "Kafka send failed. Retrying in {:.1}s",
                dur.as_secs_f32()
            );
        })
        .await?;

        Ok(())
    }

    /// Returns the retry policy for Kafka sends.
    ///
    /// Retries 3 times with fixed delays: 1s, 5s, 60s.
    fn retry_policy() -> FixedDelayBackoff {
        FixedDelayBackoff
    }

    /// Computes the partition for a given key using consistent hashing.
    ///
    /// The partition count is configured via `KafkaEventsConfig::partitions`.
    fn partition_for_key(&self, key: &str) -> i32 {
        // Simple hash-based partition selection
        let hash: u32 = key
            .bytes()
            .fold(0u32, |acc, b| acc.wrapping_add(u32::from(b)));
        (hash % self.partitions) as i32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_delay_backoff_yields_correct_delays() {
        let backoff = FixedDelayBackoff;
        let mut iter = backoff.build();

        // Should yield exactly 1s, 5s, 60s
        assert_eq!(iter.next(), Some(Duration::from_secs(1)));
        assert_eq!(iter.next(), Some(Duration::from_secs(5)));
        assert_eq!(iter.next(), Some(Duration::from_secs(60)));

        // Should be exhausted after 3 delays
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None); // Confirm it stays exhausted
    }

    #[test]
    fn fixed_delay_backoff_has_exactly_three_retries() {
        let backoff = FixedDelayBackoff;
        let delays: Vec<_> = backoff.build().collect();

        assert_eq!(delays.len(), 3);
        assert_eq!(
            delays,
            vec![
                Duration::from_secs(1),
                Duration::from_secs(5),
                Duration::from_secs(60),
            ]
        );
    }
}
