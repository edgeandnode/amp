//! Progress reporting infrastructure for dump operations.
//!
//! This module provides the [`ProgressReporter`] trait that allows external code to receive
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

/// Trait for reporting progress and lifecycle events during dump operations.
///
/// Implementations must be thread-safe as reporting may happen from
/// multiple concurrent partition tasks.
///
/// Lifecycle methods (`report_sync_started`, `report_sync_completed`, `report_sync_failed`)
/// are called per-table so consumers watching a specific table's partition
/// receive all relevant events.
pub trait ProgressReporter: Send + Sync {
    /// Report progress on a dump operation.
    ///
    /// This method should be non-blocking and should not fail - progress
    /// reporting is best-effort and should not impact the dump operation.
    fn report_progress(&self, update: ProgressUpdate);

    /// Report that sync has started for a table.
    ///
    /// This is called once per table at the start of a dump operation,
    /// after the dataset and tables have been loaded.
    fn report_sync_started(&self, info: SyncStartedInfo);

    /// Report that sync completed successfully for a table.
    ///
    /// This is called once per table when the dump operation completes
    /// successfully.
    fn report_sync_completed(&self, info: SyncCompletedInfo);

    /// Report that sync failed for a table.
    ///
    /// This is called for each table when an error occurs during the
    /// dump operation.
    fn report_sync_failed(&self, info: SyncFailedInfo);
}

/// Information passed to `report_sync_started`.
#[derive(Debug, Clone)]
pub struct SyncStartedInfo {
    /// The table that is starting to sync.
    pub table_name: TableName,
    /// The starting block number for this sync.
    pub start_block: Option<BlockNum>,
    /// The target end block number (None for continuous mode).
    pub end_block: Option<BlockNum>,
}

/// Information passed to `report_sync_completed`.
#[derive(Debug, Clone)]
pub struct SyncCompletedInfo {
    /// The table that completed syncing.
    pub table_name: TableName,
    /// The final block number that was synced.
    pub final_block: BlockNum,
    /// Duration of the sync operation in milliseconds.
    pub duration_millis: u64,
}

/// Information passed to `report_sync_failed`.
#[derive(Debug, Clone)]
pub struct SyncFailedInfo {
    /// The table that failed to sync.
    pub table_name: TableName,
    /// Error message describing the failure.
    pub error_message: String,
    /// Optional error type classification.
    pub error_type: Option<String>,
}

/// A no-op progress reporter that discards all reports.
///
/// Used when progress reporting is disabled.
pub struct NoOpProgressReporter;

impl ProgressReporter for NoOpProgressReporter {
    fn report_progress(&self, _update: ProgressUpdate) {
        // Intentionally empty - discard progress updates
    }

    fn report_sync_started(&self, _info: SyncStartedInfo) {
        // Intentionally empty
    }

    fn report_sync_completed(&self, _info: SyncCompletedInfo) {
        // Intentionally empty
    }

    fn report_sync_failed(&self, _info: SyncFailedInfo) {
        // Intentionally empty
    }
}

/// Extension trait for optional progress reporters.
pub trait ProgressReporterExt {
    /// Report progress if a reporter is configured.
    fn report_progress(&self, update: ProgressUpdate);
    /// Report sync started if a reporter is configured.
    fn report_sync_started(&self, info: SyncStartedInfo);
    /// Report sync completed if a reporter is configured.
    fn report_sync_completed(&self, info: SyncCompletedInfo);
    /// Report sync failed if a reporter is configured.
    fn report_sync_failed(&self, info: SyncFailedInfo);
}

impl ProgressReporterExt for Option<Arc<dyn ProgressReporter>> {
    fn report_progress(&self, update: ProgressUpdate) {
        if let Some(reporter) = self {
            reporter.report_progress(update);
        }
    }

    fn report_sync_started(&self, info: SyncStartedInfo) {
        if let Some(reporter) = self {
            reporter.report_sync_started(info);
        }
    }

    fn report_sync_completed(&self, info: SyncCompletedInfo) {
        if let Some(reporter) = self {
            reporter.report_sync_completed(info);
        }
    }

    fn report_sync_failed(&self, info: SyncFailedInfo) {
        if let Some(reporter) = self {
            reporter.report_sync_failed(info);
        }
    }
}
