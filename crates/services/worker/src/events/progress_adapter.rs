//! Progress callback adapter for bridging dump crate progress to event emitter.
//!
//! This module provides a [`WorkerProgressCallback`] that implements the dump crate's
//! [`dump::ProgressCallback`] trait and forwards progress updates to the worker's
//! [`EventEmitter`] for Kafka streaming.

use std::sync::Arc;

use dump::{ProgressUpdate, SyncCompletedInfo, SyncFailedInfo, SyncStartedInfo};
use kafka_client::proto;

use super::EventEmitter;
use crate::job::JobId;

/// Adapter that bridges dump crate progress callbacks to worker event emission.
///
/// This struct implements [`dump::ProgressCallback`] and translates progress updates
/// into proto events that are sent to the configured [`EventEmitter`].
pub struct WorkerProgressCallback {
    job_id: JobId,
    dataset_info: proto::DatasetInfo,
    event_emitter: Arc<dyn EventEmitter>,
}

impl WorkerProgressCallback {
    /// Creates a new progress callback adapter.
    pub fn new(
        job_id: JobId,
        dataset_info: proto::DatasetInfo,
        event_emitter: Arc<dyn EventEmitter>,
    ) -> Self {
        Self {
            job_id,
            dataset_info,
            event_emitter,
        }
    }
}

impl dump::ProgressCallback for WorkerProgressCallback {
    fn on_progress(&self, update: ProgressUpdate) {
        // Calculate percentage only if we have an end block (not in continuous mode)
        let percentage = update.end_block.map(|end_block| {
            let total_blocks = end_block.saturating_sub(update.start_block) + 1;
            let blocks_done = update.current_block.saturating_sub(update.start_block) + 1;
            if total_blocks > 0 {
                ((blocks_done as f64 / total_blocks as f64) * 100.0).min(100.0) as u32
            } else {
                0
            }
        });

        let event = proto::SyncProgress {
            job_id: *self.job_id,
            dataset: Some(self.dataset_info.clone()),
            table_name: update.table_name.to_string(),
            progress: Some(proto::ProgressInfo {
                start_block: update.start_block,
                current_block: update.current_block,
                end_block: update.end_block,
                percentage,
                files_count: update.files_count,
                total_size_bytes: update.total_size_bytes,
            }),
        };

        // Clone the emitter and spawn an async task to emit the event.
        // This is necessary because ProgressCallback::on_progress is synchronous
        // but EventEmitter::emit_sync_progress is asynchronous.
        let emitter = Arc::clone(&self.event_emitter);
        tokio::spawn(async move {
            emitter.emit_sync_progress(event).await;
        });
    }

    fn on_sync_started(&self, info: SyncStartedInfo) {
        let event = proto::SyncStarted {
            job_id: *self.job_id,
            dataset: Some(self.dataset_info.clone()),
            table_name: info.table_name.to_string(),
            start_block: info.start_block,
            end_block: info.end_block,
        };

        let emitter = Arc::clone(&self.event_emitter);
        tokio::spawn(async move {
            emitter.emit_sync_started(event).await;
        });
    }

    fn on_sync_completed(&self, info: SyncCompletedInfo) {
        let event = proto::SyncCompleted {
            job_id: *self.job_id,
            dataset: Some(self.dataset_info.clone()),
            table_name: info.table_name.to_string(),
            final_block: info.final_block,
            duration_millis: info.duration_millis,
        };

        let emitter = Arc::clone(&self.event_emitter);
        tokio::spawn(async move {
            emitter.emit_sync_completed(event).await;
        });
    }

    fn on_sync_failed(&self, info: SyncFailedInfo) {
        let event = proto::SyncFailed {
            job_id: *self.job_id,
            dataset: Some(self.dataset_info.clone()),
            table_name: info.table_name.to_string(),
            error_message: info.error_message,
            error_type: info.error_type,
        };

        let emitter = Arc::clone(&self.event_emitter);
        tokio::spawn(async move {
            emitter.emit_sync_failed(event).await;
        });
    }
}
