use std::sync::Arc;

use common::{BlockNum, arrow::array::RecordBatch};
use serde::{Deserialize, Serialize};

/// A Debezium CDC (Change Data Capture) record.
///
/// Represents a single change event in Debezium format with before/after states
/// and an operation type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebeziumRecord {
    /// The state of the record before the change (None for create operations)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before: Option<serde_json::Value>,

    /// The state of the record after the change (None for delete operations)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<serde_json::Value>,

    /// The operation type: "c" (create), "u" (update), or "d" (delete)
    pub op: DebeziumOp,
}

/// Debezium operation types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DebeziumOp {
    /// Create operation - new record inserted
    #[serde(rename = "c")]
    Create,

    /// Update operation - existing record modified
    #[serde(rename = "u")]
    Update,

    /// Delete operation - record removed
    #[serde(rename = "d")]
    Delete,
}

/// A composite key identifying a unique record.
///
/// Constructed from one or more primary key columns, hashed for efficient lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RecordKey(u128);

impl RecordKey {
    /// Create a new RecordKey from a 128-bit hash value.
    pub fn new(hash: u128) -> Self {
        Self(hash)
    }

    /// Get the raw hash value.
    pub fn hash(&self) -> u128 {
        self.0
    }
}

/// A stored record with its associated metadata.
///
/// Used by StateStore implementations to track emitted records for reorg handling.
#[derive(Debug, Clone)]
pub struct StoredRecord {
    /// The primary key identifying this record
    pub key: RecordKey,

    /// The Arrow RecordBatch containing this record
    pub batch: Arc<RecordBatch>,

    /// The row index within the batch
    pub row_idx: usize,

    /// The block number this record is associated with
    pub block_num: BlockNum,
}
