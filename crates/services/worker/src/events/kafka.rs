//! Kafka event emitter implementation.

use std::sync::Arc;

use async_trait::async_trait;
use monitoring::logging;
use prost::Message;

use super::emitter::EventEmitter;
use crate::{
    kafka::{KafkaProducer, proto},
    node_id::NodeId,
};

/// Creates the partition key for Kafka events.
///
/// Format: `{namespace}/{name}/{manifest_hash}/{table_name}`
fn partition_key(dataset: &proto::DatasetInfo, table_name: &str) -> String {
    format!(
        "{}/{}/{}/{}",
        dataset.namespace, dataset.name, dataset.manifest_hash, table_name
    )
}

/// Event type discriminator for worker events.
#[derive(Debug, Clone, Copy)]
enum EventType {
    Started,
    Progress,
    Completed,
    Failed,
}

impl std::fmt::Display for EventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Started => "sync.started",
            Self::Progress => "sync.progress",
            Self::Completed => "sync.completed",
            Self::Failed => "sync.failed",
        };
        f.write_str(s)
    }
}

/// Kafka-based event emitter.
///
/// Sends worker events to a Kafka topic using protobuf encoding.
pub struct KafkaEventEmitter {
    producer: Arc<KafkaProducer>,
    worker_id: NodeId,
}

impl KafkaEventEmitter {
    /// Creates a new Kafka event emitter.
    pub fn new(producer: Arc<KafkaProducer>, worker_id: NodeId) -> Self {
        Self {
            producer,
            worker_id,
        }
    }

    /// Sends an event to Kafka.
    ///
    /// Logs errors but does not fail - events are best-effort.
    async fn emit(&self, event_type: EventType, key: &str, event: proto::WorkerEvent) {
        let mut buf = Vec::with_capacity(event.encoded_len());
        if let Err(e) = event.encode(&mut buf) {
            tracing::error!(
                event_type = %event_type,
                error = %e,
                error_source = logging::error_source(&e),
                "failed to encode event"
            );
            return;
        }

        if let Err(e) = self.producer.send(key, &buf).await {
            // Log but don't fail - events are best-effort
            tracing::warn!(
                event_type = %event_type,
                key,
                error = %e,
                error_source = logging::error_source(&e),
                "failed to send event to Kafka (event dropped)"
            );
        }
    }

    /// Creates the event envelope with common metadata.
    fn create_envelope(
        &self,
        event_type: EventType,
        payload: proto::worker_event::Payload,
    ) -> proto::WorkerEvent {
        proto::WorkerEvent {
            event_id: uuid::Uuid::now_v7().to_string(),
            event_type: event_type.to_string(),
            event_version: "1.0".to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            source: Some(proto::EventSource {
                worker_id: self.worker_id.to_string(),
            }),
            payload: Some(payload),
        }
    }
}

#[async_trait]
impl EventEmitter for KafkaEventEmitter {
    async fn emit_sync_started(&self, event: proto::SyncStarted) {
        let key = event
            .dataset
            .as_ref()
            .map(|d| partition_key(d, &event.table_name))
            .unwrap_or_default();
        let envelope = self.create_envelope(
            EventType::Started,
            proto::worker_event::Payload::SyncStarted(event),
        );
        self.emit(EventType::Started, &key, envelope).await;
    }

    async fn emit_sync_progress(&self, event: proto::SyncProgress) {
        let key = event
            .dataset
            .as_ref()
            .map(|d| partition_key(d, &event.table_name))
            .unwrap_or_default();
        let envelope = self.create_envelope(
            EventType::Progress,
            proto::worker_event::Payload::SyncProgress(event),
        );
        self.emit(EventType::Progress, &key, envelope).await;
    }

    async fn emit_sync_completed(&self, event: proto::SyncCompleted) {
        let key = event
            .dataset
            .as_ref()
            .map(|d| partition_key(d, &event.table_name))
            .unwrap_or_default();
        let envelope = self.create_envelope(
            EventType::Completed,
            proto::worker_event::Payload::SyncCompleted(event),
        );
        self.emit(EventType::Completed, &key, envelope).await;
    }

    async fn emit_sync_failed(&self, event: proto::SyncFailed) {
        let key = event
            .dataset
            .as_ref()
            .map(|d| partition_key(d, &event.table_name))
            .unwrap_or_default();
        let envelope = self.create_envelope(
            EventType::Failed,
            proto::worker_event::Payload::SyncFailed(event),
        );
        self.emit(EventType::Failed, &key, envelope).await;
    }
}
