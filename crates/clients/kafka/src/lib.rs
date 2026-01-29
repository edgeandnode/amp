//! Kafka client for worker event streaming.
//!
//! This crate provides a Kafka producer for emitting worker lifecycle events
//! (sync.started, sync.progress, sync.completed, sync.failed) to a Kafka topic.
//!
//! Events are encoded using Protocol Buffers for compact, schema-enforced messages.
//!
//! # Example
//!
//! ```ignore
//! use kafka_client::{KafkaConfig, KafkaProducer};
//!
//! let config = KafkaConfig::new(
//!     vec!["localhost:9092".to_string()],
//!     "amp.worker.events",
//! );
//!
//! let producer = KafkaProducer::new(&config).await?;
//!
//! // Send an event with partition key and protobuf payload
//! producer.send("ethereum/mainnet/abc123/blocks", &encoded_event).await?;
//! ```

mod client;
mod config;
mod error;
pub mod proto;

pub use client::KafkaProducer;
pub use config::KafkaConfig;
pub use error::Error;
