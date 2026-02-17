//! Canton mining_rounds table definition.
//!
//! The mining_rounds table contains mining round lifecycle data from the Canton ledger.
//! This corresponds to the `05_mining_rounds.csv` data sample.
//!
//! CSV columns: round_number, contract_id, amulet_price, opens_at, target_closes_at, tick_duration_us

use std::sync::{Arc, LazyLock};

use datasets_common::{
    block_num::RESERVED_BLOCK_NUM_COLUMN_NAME, block_range::BlockRange, dataset::Table,
    network_id::NetworkId,
};
use datasets_raw::{
    arrow::{
        ArrayRef, DataType, Field, Float64Builder, Int64Builder, Schema, SchemaRef, StringBuilder,
        UInt64Builder,
    },
    rows::{TableRowError, TableRows},
};

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(schema()));

/// Table name for mining_rounds
pub const TABLE_NAME: &str = "mining_rounds";

/// Create the mining_rounds table definition for a network.
pub fn table(network: NetworkId) -> Table {
    let name = TABLE_NAME.parse().expect("table name is valid");
    Table::new(
        name,
        SCHEMA.clone(),
        network,
        vec!["round_number".to_string()],
    )
}

/// Arrow schema for the mining_rounds table.
/// Matches CSV: round_number, contract_id, amulet_price, opens_at, target_closes_at, tick_duration_us
fn schema() -> Schema {
    Schema::new(vec![
        // AMP partition key (required) - use round_number
        Field::new(RESERVED_BLOCK_NUM_COLUMN_NAME, DataType::UInt64, false),
        // Sequential round ID
        Field::new("round_number", DataType::Int64, false),
        // Contract ID for this mining round
        Field::new("contract_id", DataType::Utf8, false),
        // Canton Coin price for this round
        Field::new("amulet_price", DataType::Float64, false),
        // When the round opens
        Field::new("opens_at", DataType::Utf8, false),
        // Target close time for the round
        Field::new("target_closes_at", DataType::Utf8, false),
        // Tick duration in microseconds
        Field::new("tick_duration_us", DataType::Int64, false),
    ])
}

/// A mining round from the Canton ledger.
/// Corresponds to a row in 05_mining_rounds.csv
#[derive(Debug, Default, Clone)]
pub struct MiningRound {
    /// Sequential round ID (also used as block_num)
    pub round_number: i64,
    /// Contract ID for this mining round
    pub contract_id: String,
    /// Canton Coin (Amulet) price for this round
    pub amulet_price: f64,
    /// When the round opens
    pub opens_at: String,
    /// Target close time for the round
    pub target_closes_at: String,
    /// Tick duration in microseconds
    pub tick_duration_us: i64,
}

/// Builder for constructing Arrow arrays from MiningRound records.
pub struct MiningRoundRowsBuilder {
    block_num: UInt64Builder,
    round_number: Int64Builder,
    contract_id: StringBuilder,
    amulet_price: Float64Builder,
    opens_at: StringBuilder,
    target_closes_at: StringBuilder,
    tick_duration_us: Int64Builder,
}

impl MiningRoundRowsBuilder {
    /// Create a new builder with the given capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            block_num: UInt64Builder::with_capacity(capacity),
            round_number: Int64Builder::with_capacity(capacity),
            contract_id: StringBuilder::with_capacity(capacity, capacity * 128),
            amulet_price: Float64Builder::with_capacity(capacity),
            opens_at: StringBuilder::with_capacity(capacity, capacity * 32),
            target_closes_at: StringBuilder::with_capacity(capacity, capacity * 32),
            tick_duration_us: Int64Builder::with_capacity(capacity),
        }
    }

    /// Append a mining round record to the builder.
    pub fn append(&mut self, row: &MiningRound) {
        // Use round_number as block_num for AMP partitioning
        self.block_num.append_value(row.round_number as u64);
        self.round_number.append_value(row.round_number);
        self.contract_id.append_value(&row.contract_id);
        self.amulet_price.append_value(row.amulet_price);
        self.opens_at.append_value(&row.opens_at);
        self.target_closes_at.append_value(&row.target_closes_at);
        self.tick_duration_us.append_value(row.tick_duration_us);
    }

    /// Build TableRows from the accumulated data.
    pub fn build(mut self, range: BlockRange) -> Result<TableRows, TableRowError> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.block_num.finish()),
            Arc::new(self.round_number.finish()),
            Arc::new(self.contract_id.finish()),
            Arc::new(self.amulet_price.finish()),
            Arc::new(self.opens_at.finish()),
            Arc::new(self.target_closes_at.finish()),
            Arc::new(self.tick_duration_us.finish()),
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
        let round = MiningRound::default();
        let rows = {
            let mut builder = MiningRoundRowsBuilder::with_capacity(1);
            builder.append(&round);
            builder
                .build(BlockRange {
                    numbers: 0..=0,
                    network: "canton-test".parse().expect("valid network id"),
                    hash: B256::ZERO,
                    prev_hash: B256::ZERO,
                })
                .unwrap()
        };
        // 7 columns: _block_num, round_number, contract_id, amulet_price, opens_at, target_closes_at, tick_duration_us
        assert_eq!(rows.rows.num_columns(), 7);
        assert_eq!(rows.rows.num_rows(), 1);
    }
}
