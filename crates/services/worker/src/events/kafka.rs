//! Kafka event emitter implementation.

use std::sync::Arc;

use async_trait::async_trait;
use kafka_client::{KafkaProducer, proto};
use prost::Message;

use super::{
    emitter::EventEmitter,
    types::{SyncCompletedEvent, SyncFailedEvent, SyncProgressEvent, SyncStartedEvent},
};
use crate::node_id::NodeId;

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
    async fn emit(&self, event_type: &str, partition_key: &str, event: proto::WorkerEvent) {
        let mut buf = Vec::with_capacity(event.encoded_len());
        if let Err(e) = event.encode(&mut buf) {
            tracing::error!(error = %e, event_type, "Failed to encode event");
            return;
        }

        if let Err(e) = self.producer.send(partition_key, &buf).await {
            // Log but don't fail - events are best-effort
            tracing::warn!(
                error = %e,
                event_type,
                partition_key,
                "Failed to send event to Kafka (event dropped)"
            );
        }
    }

    /// Creates the event envelope with common metadata.
    fn create_envelope(&self, event_type: &str) -> proto::WorkerEvent {
        proto::WorkerEvent {
            event_id: uuid::Uuid::now_v7().to_string(),
            event_type: event_type.to_string(),
            event_version: "1.0".to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            source: Some(proto::EventSource {
                worker_id: self.worker_id.to_string(),
            }),
            payload: None,
        }
    }
}

#[async_trait]
impl EventEmitter for KafkaEventEmitter {
    async fn emit_sync_started(&self, event: SyncStartedEvent) {
        let partition_key = event.dataset.partition_key(&event.table_name);
        let mut envelope = self.create_envelope("sync.started");
        envelope.payload = Some(proto::worker_event::Payload::SyncStarted(
            proto::SyncStarted {
                job_id: event.job_id,
                dataset: Some(proto::DatasetInfo {
                    namespace: event.dataset.namespace,
                    name: event.dataset.name,
                    manifest_hash: event.dataset.manifest_hash,
                }),
                table_name: event.table_name,
                start_block: event.start_block,
                end_block: event.end_block,
            },
        ));
        self.emit("sync.started", &partition_key, envelope).await;
    }

    async fn emit_sync_progress(&self, event: SyncProgressEvent) {
        let partition_key = event.dataset.partition_key(&event.table_name);
        let mut envelope = self.create_envelope("sync.progress");
        envelope.payload = Some(proto::worker_event::Payload::SyncProgress(
            proto::SyncProgress {
                job_id: event.job_id,
                dataset: Some(proto::DatasetInfo {
                    namespace: event.dataset.namespace,
                    name: event.dataset.name,
                    manifest_hash: event.dataset.manifest_hash,
                }),
                table_name: event.table_name,
                progress: Some(proto::ProgressInfo {
                    start_block: event.progress.start_block,
                    current_block: event.progress.current_block,
                    end_block: event.progress.end_block,
                    percentage: event.progress.percentage.map(|p| p as u32),
                    files_count: event.progress.files_count,
                    total_size_bytes: event.progress.total_size_bytes,
                }),
            },
        ));
        self.emit("sync.progress", &partition_key, envelope).await;
    }

    async fn emit_sync_completed(&self, event: SyncCompletedEvent) {
        let partition_key = event.dataset.partition_key(&event.table_name);
        let mut envelope = self.create_envelope("sync.completed");
        envelope.payload = Some(proto::worker_event::Payload::SyncCompleted(
            proto::SyncCompleted {
                job_id: event.job_id,
                dataset: Some(proto::DatasetInfo {
                    namespace: event.dataset.namespace,
                    name: event.dataset.name,
                    manifest_hash: event.dataset.manifest_hash,
                }),
                table_name: event.table_name,
                final_block: event.final_block,
                duration_millis: event.duration_millis,
            },
        ));
        self.emit("sync.completed", &partition_key, envelope).await;
    }

    async fn emit_sync_failed(&self, event: SyncFailedEvent) {
        let partition_key = event.dataset.partition_key(&event.table_name);
        let mut envelope = self.create_envelope("sync.failed");
        envelope.payload = Some(proto::worker_event::Payload::SyncFailed(
            proto::SyncFailed {
                job_id: event.job_id,
                dataset: Some(proto::DatasetInfo {
                    namespace: event.dataset.namespace,
                    name: event.dataset.name,
                    manifest_hash: event.dataset.manifest_hash,
                }),
                table_name: event.table_name,
                error_message: event.error_message,
                error_type: event.error_type,
            },
        ));
        self.emit("sync.failed", &partition_key, envelope).await;
    }
}
