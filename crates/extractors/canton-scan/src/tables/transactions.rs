//! Canton transactions table definition.
//!
//! The transactions table contains metadata about each transaction/update in the Canton ledger.
//! This corresponds to the `01_transactions.csv` data sample.
//!
//! CSV columns: offset, update_id, record_time, effective_at, synchronizer_id, event_count

use std::sync::{Arc, LazyLock};

use datasets_common::{
    block_num::RESERVED_BLOCK_NUM_COLUMN_NAME, block_range::BlockRange, dataset::Table,
    network_id::NetworkId,
};
use datasets_raw::{
    arrow::{
        ArrayRef, DataType, Field, Int64Builder, Schema, SchemaRef, StringBuilder, UInt32Builder,
        UInt64Builder,
    },
    rows::{TableRowError, TableRows},
};

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(schema()));

/// Table name for transactions
pub const TABLE_NAME: &str = "transactions";

/// Create the transactions table definition for a network.
pub fn table(network: NetworkId) -> Table {
    let name = TABLE_NAME.parse().expect("table name is valid");
    Table::new(
        name,
        SCHEMA.clone(),
        network,
        vec!["offset".to_string(), "record_time".to_string()],
    )
}

/// Arrow schema for the transactions table.
/// Matches CSV: offset, update_id, record_time, effective_at, synchronizer_id, event_count
fn schema() -> Schema {
    Schema::new(vec![
        // AMP partition key (required, derived from offset)
        Field::new(RESERVED_BLOCK_NUM_COLUMN_NAME, DataType::UInt64, false),
        // Ledger offset - position in ledger (like block number)
        Field::new("offset", DataType::Int64, false),
        // Update ID (globally unique transaction identifier/hash)
        Field::new("update_id", DataType::Utf8, false),
        // Record time as ISO 8601 string
        Field::new("record_time", DataType::Utf8, false),
        // Effective timestamp (ledger time)
        Field::new("effective_at", DataType::Utf8, true),
        // Synchronizer/domain ID where this update occurred
        Field::new("synchronizer_id", DataType::Utf8, false),
        // Total number of events in this transaction
        Field::new("event_count", DataType::UInt32, false),
    ])
}

/// A transaction record from the Canton ledger.
/// Corresponds to a row in 01_transactions.csv
#[derive(Debug, Default, Clone)]
pub struct Transaction {
    /// Ledger offset (position in ledger, like block number)
    pub offset: i64,
    /// Update ID (globally unique transaction identifier)
    pub update_id: String,
    /// Record time as ISO 8601 string
    pub record_time: String,
    /// Effective timestamp (ledger time)
    pub effective_at: Option<String>,
    /// Synchronizer/domain ID where this update occurred
    pub synchronizer_id: String,
    /// Total number of events in this transaction
    pub event_count: u32,
}

/// Builder for constructing Arrow arrays from Transaction records.
pub struct TransactionRowsBuilder {
    block_num: UInt64Builder,
    offset: Int64Builder,
    update_id: StringBuilder,
    record_time: StringBuilder,
    effective_at: StringBuilder,
    synchronizer_id: StringBuilder,
    event_count: UInt32Builder,
}

impl TransactionRowsBuilder {
    /// Create a new builder with the given capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            block_num: UInt64Builder::with_capacity(capacity),
            offset: Int64Builder::with_capacity(capacity),
            update_id: StringBuilder::with_capacity(capacity, capacity * 64),
            record_time: StringBuilder::with_capacity(capacity, capacity * 32),
            effective_at: StringBuilder::with_capacity(capacity, capacity * 32),
            synchronizer_id: StringBuilder::with_capacity(capacity, capacity * 64),
            event_count: UInt32Builder::with_capacity(capacity),
        }
    }

    /// Append a transaction record to the builder.
    pub fn append(&mut self, row: &Transaction) {
        // Use offset as block_num for AMP partitioning
        self.block_num.append_value(row.offset as u64);
        self.offset.append_value(row.offset);
        self.update_id.append_value(&row.update_id);
        self.record_time.append_value(&row.record_time);
        self.effective_at.append_option(row.effective_at.as_deref());
        self.synchronizer_id.append_value(&row.synchronizer_id);
        self.event_count.append_value(row.event_count);
    }

    /// Build TableRows from the accumulated data.
    pub fn build(mut self, range: BlockRange) -> Result<TableRows, TableRowError> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.block_num.finish()),
            Arc::new(self.offset.finish()),
            Arc::new(self.update_id.finish()),
            Arc::new(self.record_time.finish()),
            Arc::new(self.effective_at.finish()),
            Arc::new(self.synchronizer_id.finish()),
            Arc::new(self.event_count.finish()),
        ];

        TableRows::new(table(range.network.clone()), range, columns)
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::B256;

    use super::*;

    #[test]
    fn default_to_arrow() {
        let tx = Transaction::default();
        let rows = {
            let mut builder = TransactionRowsBuilder::with_capacity(1);
            builder.append(&tx);
            builder
                .build(BlockRange {
                    numbers: 0..=0,
                    network: "canton-test".parse().expect("valid network id"),
                    hash: B256::ZERO,
                    prev_hash: B256::ZERO,
                })
                .unwrap()
        };
        // 7 columns: _block_num, offset, update_id, record_time, effective_at, synchronizer_id, event_count
        assert_eq!(rows.rows.num_columns(), 7);
        assert_eq!(rows.rows.num_rows(), 1);
    }
}
