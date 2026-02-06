//! Mock event emitter for testing worker event streaming.
//!
//! This fixture provides a mock implementation of the EventEmitter trait that
//! records all emitted events for verification in tests.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use worker::{events::EventEmitter, kafka::proto};

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

// --- Helper types below ---

/// Recorded events from the mock emitter.
#[derive(Debug, Clone)]
pub enum RecordedEvent {
    Started(proto::SyncStarted),
    Progress(proto::SyncProgress),
    Completed(proto::SyncCompleted),
    Failed(proto::SyncFailed),
}
