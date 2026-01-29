//! Error types for the Kafka client.

/// Errors that can occur when working with the Kafka client.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_error_is_retryable() {
        let err = Error::Send("network timeout".to_string());
        assert!(err.is_retryable(), "Send errors should be retryable");
    }

    #[test]
    fn encode_error_is_not_retryable() {
        // Create a prost encode error by encoding to a buffer that's too small
        use prost::Message;

        // Create a simple message and try to encode to a limited buffer
        let msg = crate::proto::WorkerEvent {
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

    // Note: Connection and PartitionClient errors wrap rskafka::client::error::Error
    // which cannot be easily constructed in tests. The is_retryable() logic is
    // verified by the matches! macro pattern:
    // - PartitionClient: IS retryable (included in matches!)
    // - Connection: NOT retryable (excluded from matches!)
}
