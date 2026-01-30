//! Progress callback infrastructure for dump operations.
//!
//! This module provides the [`ProgressCallback`] trait that allows external code to receive
//! progress updates during dump operations. This enables real-time monitoring of sync jobs
//! through event streaming systems like Kafka.

use std::sync::Arc;

use common::BlockNum;
use datasets_common::table_name::TableName;

/// Progress information for a dump operation.
#[derive(Debug, Clone)]
pub struct ProgressUpdate {
    /// The table being processed.
    pub table_name: TableName,
    /// The starting block of the current range being processed.
    pub start_block: BlockNum,
    /// The current block that was just processed.
    pub current_block: BlockNum,
    /// The end block of the range being processed (for percentage calculation).
    /// None in continuous mode where there's no fixed end target.
    pub end_block: Option<BlockNum>,
    /// Total number of Parquet files written so far.
    pub files_count: u64,
    /// Total bytes written so far.
    pub total_size_bytes: u64,
}

/// Trait for receiving progress and lifecycle updates during dump operations.
///
/// Implementations must be thread-safe as callbacks may be invoked
/// from multiple concurrent partition tasks.
///
/// Lifecycle methods (`on_sync_started`, `on_sync_completed`, `on_sync_failed`)
/// are called per-table so consumers watching a specific table's partition
/// receive all relevant events.
pub trait ProgressCallback: Send + Sync {
    /// Called when progress is made on a dump operation.
    ///
    /// This method should be non-blocking and should not fail - progress
    /// reporting is best-effort and should not impact the dump operation.
    fn on_progress(&self, update: ProgressUpdate);

    /// Called when sync begins for a table.
    ///
    /// This is called once per table at the start of a dump operation,
    /// after the dataset and tables have been loaded.
    fn on_sync_started(&self, _info: SyncStartedInfo) {
        // Default: no-op
    }

    /// Called when sync completes successfully for a table.
    ///
    /// This is called once per table when the dump operation completes
    /// successfully.
    fn on_sync_completed(&self, _info: SyncCompletedInfo) {
        // Default: no-op
    }

    /// Called when sync fails for a table.
    ///
    /// This is called for each table when an error occurs during the
    /// dump operation.
    fn on_sync_failed(&self, _info: SyncFailedInfo) {
        // Default: no-op
    }
}

/// Information passed to `on_sync_started` callback.
#[derive(Debug, Clone)]
pub struct SyncStartedInfo {
    /// The table that is starting to sync.
    pub table_name: TableName,
    /// The starting block number for this sync.
    pub start_block: Option<BlockNum>,
    /// The target end block number (None for continuous mode).
    pub end_block: Option<BlockNum>,
}

/// Information passed to `on_sync_completed` callback.
#[derive(Debug, Clone)]
pub struct SyncCompletedInfo {
    /// The table that completed syncing.
    pub table_name: TableName,
    /// The final block number that was synced.
    pub final_block: BlockNum,
    /// Duration of the sync operation in milliseconds.
    pub duration_millis: u64,
}

/// Information passed to `on_sync_failed` callback.
#[derive(Debug, Clone)]
pub struct SyncFailedInfo {
    /// The table that failed to sync.
    pub table_name: TableName,
    /// Error message describing the failure.
    pub error_message: String,
    /// Optional error type classification.
    pub error_type: Option<String>,
}

/// A no-op progress callback that discards all progress updates.
///
/// Used when progress reporting is disabled.
pub struct NoOpProgressCallback;

impl ProgressCallback for NoOpProgressCallback {
    fn on_progress(&self, _update: ProgressUpdate) {
        // Intentionally empty - discard progress updates
    }
}

/// Extension trait for optional progress callbacks.
pub trait ProgressCallbackExt {
    /// Report progress if a callback is configured.
    fn report_progress(&self, update: ProgressUpdate);
}

impl ProgressCallbackExt for Option<Arc<dyn ProgressCallback>> {
    fn report_progress(&self, update: ProgressUpdate) {
        if let Some(callback) = self {
            callback.on_progress(update);
        }
    }
}
