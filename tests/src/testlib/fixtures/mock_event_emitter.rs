//! Mock event emitter for testing worker event streaming.
//!
//! This fixture provides a mock implementation of the EventEmitter trait that
//! records all emitted events for verification in tests.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use worker::events::{
    EventEmitter, SyncCompletedEvent, SyncFailedEvent, SyncProgressEvent, SyncStartedEvent,
};

/// Recorded events from the mock emitter.
#[derive(Debug, Clone)]
pub enum RecordedEvent {
    Started(SyncStartedEvent),
    Progress(SyncProgressEvent),
    Completed(SyncCompletedEvent),
    Failed(SyncFailedEvent),
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
    pub fn progress_events(&self) -> Vec<SyncProgressEvent> {
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
    pub fn started_events(&self) -> Vec<SyncStartedEvent> {
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
    pub fn completed_events(&self) -> Vec<SyncCompletedEvent> {
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
    pub fn failed_events(&self) -> Vec<SyncFailedEvent> {
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

    /// Get unique progress percentages (calculated from current_block).
    ///
    /// Assumes start_block is at 0% and calculates percentage based on
    /// the difference between current_block and start_block.
    pub fn progress_percentages(&self, total_blocks: u64) -> Vec<u64> {
        self.progress_events()
            .iter()
            .map(|p| {
                let blocks_done = p
                    .progress
                    .current_block
                    .saturating_sub(p.progress.start_block);
                (blocks_done * 100) / total_blocks
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
    async fn emit_sync_started(&self, event: SyncStartedEvent) {
        tracing::info!(
            job_id = event.job_id,
            dataset = %format!("{}/{}", event.dataset.namespace, event.dataset.name),
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

    async fn emit_sync_progress(&self, event: SyncProgressEvent) {
        tracing::info!(
            job_id = event.job_id,
            table = %event.table_name,
            percentage = event.progress.percentage,
            current_block = event.progress.current_block,
            end_block = event.progress.end_block,
            "[EVENT] sync.progress"
        );
        self.events
            .lock()
            .unwrap()
            .push(RecordedEvent::Progress(event));
    }

    async fn emit_sync_completed(&self, event: SyncCompletedEvent) {
        tracing::info!(
            job_id = event.job_id,
            dataset = %format!("{}/{}", event.dataset.namespace, event.dataset.name),
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

    async fn emit_sync_failed(&self, event: SyncFailedEvent) {
        tracing::warn!(
            job_id = event.job_id,
            dataset = %format!("{}/{}", event.dataset.namespace, event.dataset.name),
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
    use datasets_common::{hash::Hash, name::Name, namespace::Namespace};
    use worker::events::{DatasetInfo, ProgressInfo};

    use super::*;

    /// Test hash constant (64 hex characters).
    const TEST_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000001";

    /// Helper to create a test DatasetInfo.
    fn test_dataset_info() -> DatasetInfo {
        DatasetInfo {
            namespace: "test".parse::<Namespace>().unwrap(),
            name: "dataset".parse::<Name>().unwrap(),
            manifest_hash: TEST_HASH.parse::<Hash>().unwrap(),
        }
    }

    #[tokio::test]
    async fn test_mock_emitter_records_events() {
        let emitter = MockEventEmitter::new();

        emitter
            .emit_sync_started(SyncStartedEvent {
                job_id: 1,
                dataset: test_dataset_info(),
                table_name: "blocks".to_string(),
                start_block: Some(0),
                end_block: Some(100),
            })
            .await;

        emitter
            .emit_sync_progress(SyncProgressEvent {
                job_id: 1,
                dataset: test_dataset_info(),
                table_name: "blocks".to_string(),
                progress: ProgressInfo {
                    start_block: 0,
                    current_block: 50,
                    end_block: Some(100),
                    percentage: Some(50),
                    files_count: 1,
                    total_size_bytes: 1000,
                },
            })
            .await;

        emitter
            .emit_sync_completed(SyncCompletedEvent {
                job_id: 1,
                dataset: test_dataset_info(),
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

    #[tokio::test]
    async fn test_progress_percentages() {
        let emitter = MockEventEmitter::new();
        let total_blocks = 100;

        // Emit progress at 25%, 50%, 75%
        for pct in [25u64, 50, 75] {
            emitter
                .emit_sync_progress(SyncProgressEvent {
                    job_id: 1,
                    dataset: test_dataset_info(),
                    table_name: "blocks".to_string(),
                    progress: ProgressInfo {
                        start_block: 0,
                        current_block: pct,
                        end_block: Some(100),
                        percentage: Some(pct as u8),
                        files_count: 0,
                        total_size_bytes: 0,
                    },
                })
                .await;
        }

        let percentages = emitter.progress_percentages(total_blocks);
        assert_eq!(percentages, vec![25, 50, 75]);
    }
}
