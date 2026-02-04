//! Mock event emitter for testing worker event streaming.
//!
//! This fixture provides a mock implementation of the EventEmitter trait that
//! records all emitted events for verification in tests.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use worker::{events::EventEmitter, kafka::proto};

/// Recorded events from the mock emitter.
#[derive(Debug, Clone)]
pub enum RecordedEvent {
    Started(proto::SyncStarted),
    Progress(proto::SyncProgress),
    Completed(proto::SyncCompleted),
    Failed(proto::SyncFailed),
}

/// A mock event emitter that records all events for testing.
///
/// This implementation is thread-safe and can be shared across async tasks.
pub struct MockEventEmitter {
    events: Arc<Mutex<Vec<RecordedEvent>>>,
}

impl MockEventEmitter {
    /// Create a new mock event emitter.
    pub fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Get all recorded events.
    pub fn events(&self) -> Vec<RecordedEvent> {
        self.events.lock().unwrap().clone()
    }

    /// Get the number of recorded events.
    pub fn event_count(&self) -> usize {
        self.events.lock().unwrap().len()
    }

    /// Get all progress events.
    pub fn progress_events(&self) -> Vec<proto::SyncProgress> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                RecordedEvent::Progress(p) => Some(p.clone()),
                _ => None,
            })
            .collect()
    }

    /// Get all started events.
    pub fn started_events(&self) -> Vec<proto::SyncStarted> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                RecordedEvent::Started(s) => Some(s.clone()),
                _ => None,
            })
            .collect()
    }

    /// Get all completed events.
    pub fn completed_events(&self) -> Vec<proto::SyncCompleted> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                RecordedEvent::Completed(c) => Some(c.clone()),
                _ => None,
            })
            .collect()
    }

    /// Get all failed events.
    pub fn failed_events(&self) -> Vec<proto::SyncFailed> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                RecordedEvent::Failed(f) => Some(f.clone()),
                _ => None,
            })
            .collect()
    }

    /// Clear all recorded events.
    pub fn clear(&self) {
        self.events.lock().unwrap().clear();
    }
}

impl Default for MockEventEmitter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl EventEmitter for MockEventEmitter {
    async fn emit_sync_started(&self, event: proto::SyncStarted) {
        let dataset_str = event
            .dataset
            .as_ref()
            .map(|d| format!("{}/{}", d.namespace, d.name))
            .unwrap_or_default();
        tracing::info!(
            job_id = event.job_id,
            dataset = %dataset_str,
            table = %event.table_name,
            start_block = ?event.start_block,
            end_block = ?event.end_block,
            "[EVENT] sync.started"
        );
        self.events
            .lock()
            .unwrap()
            .push(RecordedEvent::Started(event));
    }

    async fn emit_sync_progress(&self, event: proto::SyncProgress) {
        let (current_block, end_block) = event
            .progress
            .as_ref()
            .map(|p| (p.current_block, p.end_block))
            .unwrap_or_default();
        tracing::info!(
            job_id = event.job_id,
            table = %event.table_name,
            current_block = current_block,
            end_block = ?end_block,
            "[EVENT] sync.progress"
        );
        self.events
            .lock()
            .unwrap()
            .push(RecordedEvent::Progress(event));
    }

    async fn emit_sync_completed(&self, event: proto::SyncCompleted) {
        let dataset_str = event
            .dataset
            .as_ref()
            .map(|d| format!("{}/{}", d.namespace, d.name))
            .unwrap_or_default();
        tracing::info!(
            job_id = event.job_id,
            dataset = %dataset_str,
            table = %event.table_name,
            final_block = event.final_block,
            duration_millis = event.duration_millis,
            "[EVENT] sync.completed"
        );
        self.events
            .lock()
            .unwrap()
            .push(RecordedEvent::Completed(event));
    }

    async fn emit_sync_failed(&self, event: proto::SyncFailed) {
        let dataset_str = event
            .dataset
            .as_ref()
            .map(|d| format!("{}/{}", d.namespace, d.name))
            .unwrap_or_default();
        tracing::warn!(
            job_id = event.job_id,
            dataset = %dataset_str,
            table = %event.table_name,
            error_message = %event.error_message,
            error_type = ?event.error_type,
            "[EVENT] sync.failed"
        );
        self.events
            .lock()
            .unwrap()
            .push(RecordedEvent::Failed(event));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test hash constant (64 hex characters).
    const TEST_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000001";

    /// Helper to create a test DatasetInfo.
    fn test_dataset_info() -> proto::DatasetInfo {
        proto::DatasetInfo {
            namespace: "test".to_string(),
            name: "dataset".to_string(),
            manifest_hash: TEST_HASH.to_string(),
        }
    }

    #[tokio::test]
    async fn test_mock_emitter_records_events() {
        let emitter = MockEventEmitter::new();

        emitter
            .emit_sync_started(proto::SyncStarted {
                job_id: 1,
                dataset: Some(test_dataset_info()),
                table_name: "blocks".to_string(),
                start_block: Some(0),
                end_block: Some(100),
            })
            .await;

        emitter
            .emit_sync_progress(proto::SyncProgress {
                job_id: 1,
                dataset: Some(test_dataset_info()),
                table_name: "blocks".to_string(),
                progress: Some(proto::ProgressInfo {
                    start_block: 0,
                    current_block: 50,
                    end_block: Some(100),
                    files_count: 1,
                    total_size_bytes: 1000,
                }),
            })
            .await;

        emitter
            .emit_sync_completed(proto::SyncCompleted {
                job_id: 1,
                dataset: Some(test_dataset_info()),
                table_name: "blocks".to_string(),
                final_block: 100,
                duration_millis: 5000,
            })
            .await;

        assert_eq!(emitter.event_count(), 3);
        assert_eq!(emitter.started_events().len(), 1);
        assert_eq!(emitter.progress_events().len(), 1);
        assert_eq!(emitter.completed_events().len(), 1);
    }
}
