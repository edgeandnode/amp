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

/// Trait for receiving progress updates during dump operations.
///
/// Implementations must be thread-safe as progress callbacks may be invoked
/// from multiple concurrent partition tasks.
pub trait ProgressCallback: Send + Sync {
    /// Called when progress is made on a dump operation.
    ///
    /// This method should be non-blocking and should not fail - progress
    /// reporting is best-effort and should not impact the dump operation.
    fn on_progress(&self, update: ProgressUpdate);
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
