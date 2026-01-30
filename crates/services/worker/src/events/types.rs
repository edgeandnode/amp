//! Event types for worker event streaming.

use datasets_common::{hash::Hash, name::Name, namespace::Namespace};

/// Information about a dataset.
#[derive(Debug, Clone)]
pub struct DatasetInfo {
    /// Dataset namespace (e.g., "ethereum")
    pub namespace: Namespace,
    /// Dataset name (e.g., "mainnet")
    pub name: Name,
    /// Manifest hash
    pub manifest_hash: Hash,
}

impl DatasetInfo {
    /// Creates the partition key for Kafka.
    ///
    /// Format: `{namespace}/{name}/{manifest_hash}/{table_name}`
    pub fn partition_key(&self, table_name: &str) -> String {
        format!(
            "{}/{}/{}/{}",
            self.namespace, self.name, self.manifest_hash, table_name
        )
    }
}

/// Progress information for a sync job.
#[derive(Debug, Clone)]
pub struct ProgressInfo {
    /// Starting block number
    pub start_block: u64,
    /// Current block number
    pub current_block: u64,
    /// End block number (target) - None in continuous mode
    pub end_block: Option<u64>,
    /// Progress percentage (0-100) - None in continuous mode
    pub percentage: Option<u8>,
    /// Number of files written
    pub files_count: u64,
    /// Total size of files in bytes
    pub total_size_bytes: u64,
}

/// Event: Sync job started.
#[derive(Debug, Clone)]
pub struct SyncStartedEvent {
    /// Job ID
    pub job_id: i64,
    /// Dataset information
    pub dataset: DatasetInfo,
    /// Table name
    pub table_name: String,
    /// Starting block (if known)
    pub start_block: Option<u64>,
    /// Target end block (if known)
    pub end_block: Option<u64>,
}

/// Event: Sync progress update.
#[derive(Debug, Clone)]
pub struct SyncProgressEvent {
    /// Job ID
    pub job_id: i64,
    /// Dataset information
    pub dataset: DatasetInfo,
    /// Table name
    pub table_name: String,
    /// Progress information
    pub progress: ProgressInfo,
}

/// Event: Sync job completed successfully.
#[derive(Debug, Clone)]
pub struct SyncCompletedEvent {
    /// Job ID
    pub job_id: i64,
    /// Dataset information
    pub dataset: DatasetInfo,
    /// Table name
    pub table_name: String,
    /// Final block number
    pub final_block: u64,
    /// Duration in milliseconds
    pub duration_millis: u64,
}

/// Event: Sync job failed.
#[derive(Debug, Clone)]
pub struct SyncFailedEvent {
    /// Job ID
    pub job_id: i64,
    /// Dataset information
    pub dataset: DatasetInfo,
    /// Table name
    pub table_name: String,
    /// Error message
    pub error_message: String,
    /// Error type (optional)
    pub error_type: Option<String>,
}
