//! Kafka producer for worker events.

use std::{sync::Arc, time::Duration};

use amp_config::KafkaEventsConfig;
use backon::{BackoffBuilder, Retryable};
use monitoring::logging;
use rskafka::{
    client::{
        Client, ClientBuilder, Credentials, SaslConfig,
        partition::{Compression, UnknownTopicHandling},
    },
    record::Record,
};
use rustls::ClientConfig;

/// Errors that can occur when working with the Kafka producer.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to connect to Kafka brokers
    #[error("failed to connect to Kafka brokers: {0}")]
    Connection(#[source] rskafka::client::error::Error),

    /// Failed to get partition client
    #[error("failed to get partition client: {0}")]
    PartitionClient(#[source] rskafka::client::error::Error),

    /// Failed to send event to Kafka
    #[error("failed to send event to Kafka: {0}")]
    Send(String),

    /// Failed to encode protobuf message
    #[error("failed to encode protobuf message")]
    Encode(#[source] prost::EncodeError),

    /// Invalid SASL configuration
    #[error("invalid SASL configuration: {0}")]
    InvalidSaslConfig(String),
}

impl Error {
    /// Returns true if the error is retryable.
    ///
    /// Retryable errors are transient failures that may succeed on retry:
    /// - `Send`: Network issues during message production
    /// - `PartitionClient`: Transient partition errors
    ///
    /// Non-retryable errors are permanent failures:
    /// - `Connection`: Initial broker connection failed (configuration issue)
    /// - `Encode`: Protobuf encoding failed (programming error)
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Send(_) | Self::PartitionClient(_))
    }
}

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
    /// Returns an error if the connection to the Kafka brokers fails,
    /// or if SASL configuration is invalid.
    pub async fn new(config: &KafkaEventsConfig) -> Result<Self, Error> {
        let mut builder = ClientBuilder::new(config.brokers.clone());

        // Configure SASL authentication if mechanism is specified
        if let Some(mechanism) = &config.sasl_mechanism {
            let sasl_config = Self::build_sasl_config(mechanism, config)?;
            builder = builder.sasl_config(sasl_config);
        }

        // Configure TLS if enabled
        if config.tls_enabled {
            let tls_config = Self::build_tls_config();
            builder = builder.tls_config(tls_config);
        }

        let client = builder.build().await.map_err(Error::Connection)?;

        Ok(Self {
            client: Arc::new(client),
            topic: config.topic.clone(),
            partitions: config.partitions,
        })
    }

    /// Builds SASL configuration from the provided mechanism and credentials.
    fn build_sasl_config(mechanism: &str, config: &KafkaEventsConfig) -> Result<SaslConfig, Error> {
        let username = config.sasl_username.as_ref().ok_or_else(|| {
            Error::InvalidSaslConfig("sasl_username is required when sasl_mechanism is set".into())
        })?;
        let password = config.sasl_password.as_ref().ok_or_else(|| {
            Error::InvalidSaslConfig("sasl_password is required when sasl_mechanism is set".into())
        })?;

        let credentials = Credentials::new(username.clone(), password.clone());

        match mechanism.to_uppercase().as_str() {
            "PLAIN" => Ok(SaslConfig::Plain(credentials)),
            "SCRAM-SHA-256" => Ok(SaslConfig::ScramSha256(credentials)),
            "SCRAM-SHA-512" => Ok(SaslConfig::ScramSha512(credentials)),
            _ => Err(Error::InvalidSaslConfig(format!(
                "unsupported SASL mechanism '{}'. Supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512",
                mechanism
            ))),
        }
    }

    /// Builds TLS configuration with system root certificates.
    fn build_tls_config() -> Arc<ClientConfig> {
        let root_store = rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };

        let tls_config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        Arc::new(tls_config)
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
    use prost::Message;

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

    #[test]
    fn send_error_is_retryable() {
        let err = Error::Send("network timeout".to_string());
        assert!(err.is_retryable(), "Send errors should be retryable");
    }

    #[test]
    fn encode_error_is_not_retryable() {
        // Create a prost encode error by encoding to a buffer that's too small
        let msg = crate::kafka::proto::WorkerEvent {
            event_id: "test".to_string(),
            event_type: "test".to_string(),
            event_version: "1.0".to_string(),
            timestamp: "2024-01-01T00:00:00Z".to_string(),
            source: None,
            payload: None,
        };

        // Encode to a buffer that's intentionally too small
        let mut buf = [0u8; 1]; // Way too small for the message
        let encode_result = msg.encode(&mut buf.as_mut_slice());

        // We expect this to fail with an EncodeError
        let encode_err = encode_result.expect_err("encoding to tiny buffer should fail");
        let err = Error::Encode(encode_err);
        assert!(!err.is_retryable(), "Encode errors should not be retryable");
    }

    fn make_kafka_config(
        mechanism: Option<&str>,
        username: Option<&str>,
        password: Option<&str>,
    ) -> KafkaEventsConfig {
        KafkaEventsConfig {
            brokers: vec!["localhost:9092".to_string()],
            topic: "test".to_string(),
            partitions: 1,
            sasl_mechanism: mechanism.map(String::from),
            sasl_username: username.map(String::from),
            sasl_password: password.map(String::from),
            tls_enabled: false,
        }
    }

    #[test]
    fn build_sasl_config_plain() {
        let config = make_kafka_config(Some("PLAIN"), Some("user"), Some("pass"));
        let result = KafkaProducer::build_sasl_config("PLAIN", &config);
        assert!(matches!(result, Ok(SaslConfig::Plain(_))));

        // Case-insensitive
        let result = KafkaProducer::build_sasl_config("plain", &config);
        assert!(matches!(result, Ok(SaslConfig::Plain(_))));
    }

    #[test]
    fn build_sasl_config_scram_sha_256() {
        let config = make_kafka_config(Some("SCRAM-SHA-256"), Some("user"), Some("pass"));
        let result = KafkaProducer::build_sasl_config("SCRAM-SHA-256", &config);
        assert!(matches!(result, Ok(SaslConfig::ScramSha256(_))));

        // Case-insensitive
        let result = KafkaProducer::build_sasl_config("scram-sha-256", &config);
        assert!(matches!(result, Ok(SaslConfig::ScramSha256(_))));
    }

    #[test]
    fn build_sasl_config_scram_sha_512() {
        let config = make_kafka_config(Some("SCRAM-SHA-512"), Some("user"), Some("pass"));
        let result = KafkaProducer::build_sasl_config("SCRAM-SHA-512", &config);
        assert!(matches!(result, Ok(SaslConfig::ScramSha512(_))));
    }

    #[test]
    fn build_sasl_config_validation_errors() {
        // Missing username
        let config = make_kafka_config(Some("PLAIN"), None, Some("pass"));
        assert!(KafkaProducer::build_sasl_config("PLAIN", &config).is_err());

        // Missing password
        let config = make_kafka_config(Some("PLAIN"), Some("user"), None);
        assert!(KafkaProducer::build_sasl_config("PLAIN", &config).is_err());

        // Unsupported mechanism
        let config = make_kafka_config(Some("GSSAPI"), Some("user"), Some("pass"));
        assert!(KafkaProducer::build_sasl_config("GSSAPI", &config).is_err());
    }
}
