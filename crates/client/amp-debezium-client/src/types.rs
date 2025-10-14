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

#[cfg(feature = "rocksdb")]
impl StoredBatch {
    /// Serialize to bytes for RocksDB storage using Arrow IPC format.
    pub fn to_bytes(&self) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        use common::arrow::ipc::writer::StreamWriter;

        let mut buf = Vec::new();

        // Write RecordBatch using Arrow IPC
        let mut writer = StreamWriter::try_new(&mut buf, &self.batch.schema())?;
        writer.write(&self.batch)?;
        writer.finish()?;
        drop(writer);

        // Append serialized ranges
        let ranges_json = serde_json::to_vec(&self.ranges)?;
        let ranges_len = (ranges_json.len() as u64).to_be_bytes();

        let mut result = Vec::new();
        result.extend_from_slice(&ranges_len);
        result.extend_from_slice(&ranges_json);
        result.extend_from_slice(&buf);

        Ok(result)
    }

    /// Deserialize from bytes stored in RocksDB.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        use common::arrow::ipc::reader::StreamReader;

        // Read ranges length prefix
        let ranges_len = u64::from_be_bytes(bytes[0..8].try_into()?);
        let ranges_end = 8 + ranges_len as usize;

        // Deserialize ranges
        let ranges: Vec<BlockRange> = serde_json::from_slice(&bytes[8..ranges_end])?;

        // Deserialize RecordBatch from Arrow IPC
        let batch_bytes = &bytes[ranges_end..];
        let mut reader = StreamReader::try_new(batch_bytes, None)?;

        let batch = reader.next().ok_or("No batch found in IPC stream")??;

        Ok(Self {
            batch: Arc::new(batch),
            ranges,
        })
    }
}
