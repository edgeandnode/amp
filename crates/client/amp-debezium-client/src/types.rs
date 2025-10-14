use std::sync::Arc;

use common::{arrow::array::RecordBatch, metadata::segments::BlockRange};
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

/// A batch of records with associated block ranges.
///
/// A single batch can contain data from multiple networks at different block ranges.
/// When any network reorgs affecting any of the ranges, the entire batch is retracted.
/// Records are extracted on-demand during reorg handling, not stored redundantly.
#[derive(Debug, Clone)]
pub struct StoredBatch {
    /// The Arrow RecordBatch
    pub batch: Arc<RecordBatch>,

    /// Block ranges for all networks in this batch
    pub ranges: Vec<BlockRange>,
}
