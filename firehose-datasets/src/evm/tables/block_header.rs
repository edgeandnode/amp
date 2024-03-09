use common::arrow::datatypes::{DataType, Field, Schema};
use common::{
    Bytes, Bytes32, EvmAddress as Address, Table, BYTES32_TYPE, EVM_ADDRESS_TYPE as ADDRESS_TYPE,
};
use serde::Serialize;

pub fn table() -> Table {
    Table {
        name: TABLE_NAME.to_string(),
        schema: schema(),
    }
}

const TABLE_NAME: &'static str = "blocks";

#[derive(Debug, Serialize)]
pub struct BlockHeader {
    pub number: u64,
    pub hash: Bytes32,

    pub parent_hash: Bytes32,
    pub uncle_hash: Bytes32,
    pub coinbase: Address,
    pub state_root: Bytes32,
    pub transactions_root: Bytes32,
    pub receipt_root: Bytes32,
    pub logs_bloom: Bytes,
    pub difficulty: Bytes,
    pub gas_limit: u64,
    pub gas_used: u64,
    pub timestamp: u64,
    pub extra_data: Bytes,
    pub mix_hash: Bytes32,
    pub nonce: u64,
    pub base_fee_per_gas: Option<u64>,
}

fn schema() -> Schema {
    let number = Field::new("number", DataType::UInt64, false);
    let hash = Field::new("hash", BYTES32_TYPE, false);
    let parent_hash = Field::new("parent_hash", BYTES32_TYPE, false);
    let uncle_hash = Field::new("uncle_hash", BYTES32_TYPE, false);
    let coinbase = Field::new("coinbase", ADDRESS_TYPE, false);
    let state_root = Field::new("state_root", BYTES32_TYPE, false);
    let transactions_root = Field::new("transactions_root", BYTES32_TYPE, false);
    let receipt_root = Field::new("receipt_root", BYTES32_TYPE, false);
    let logs_bloom = Field::new("logs_bloom", DataType::Binary, false);
    let difficulty = Field::new("difficulty", DataType::Binary, false);
    let gas_limit = Field::new("gas_limit", DataType::UInt64, false);
    let gas_used = Field::new("gas_used", DataType::UInt64, false);
    let timestamp = Field::new("timestamp", DataType::UInt64, false);
    let extra_data = Field::new("extra_data", DataType::Binary, false);
    let mix_hash = Field::new("mix_hash", BYTES32_TYPE, false);
    let nonce = Field::new("nonce", DataType::UInt64, false);
    let base_fee_per_gas = Field::new("base_fee_per_gas", DataType::UInt64, true);

    let fields = vec![
        number,
        hash,
        parent_hash,
        uncle_hash,
        coinbase,
        state_root,
        transactions_root,
        receipt_root,
        logs_bloom,
        difficulty,
        gas_limit,
        gas_used,
        timestamp,
        extra_data,
        mix_hash,
        nonce,
        base_fee_per_gas,
    ];

    Schema::new(fields)
}
