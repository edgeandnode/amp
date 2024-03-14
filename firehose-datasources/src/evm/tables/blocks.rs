use std::sync::Arc;

use common::arrow::array::RecordBatch;
use common::arrow::datatypes::{DataType, Field, Schema};
use common::arrow::error::ArrowError;
use common::arrow_helpers::ScalarToArray as _;
use common::{
    Bytes, Bytes32, EvmAddress as Address, Table, BYTES32_TYPE, EVM_ADDRESS_TYPE as ADDRESS_TYPE,
};

pub fn table() -> Table {
    Table {
        name: TABLE_NAME.to_string(),
        schema: schema(),
    }
}

pub const TABLE_NAME: &'static str = "blocks";

#[derive(Debug, Default)]
pub struct Block {
    pub(crate) block_num: u64,
    pub(crate) hash: Bytes32,

    pub(crate) parent_hash: Bytes32,
    pub(crate) uncle_hash: Bytes32,
    pub(crate) coinbase: Address,
    pub(crate) state_root: Bytes32,
    pub(crate) transactions_root: Bytes32,
    pub(crate) receipt_root: Bytes32,
    pub(crate) logs_bloom: Bytes,
    pub(crate) difficulty: Bytes,
    pub(crate) gas_limit: u64,
    pub(crate) gas_used: u64,
    pub(crate) timestamp: u64,
    pub(crate) extra_data: Bytes,
    pub(crate) mix_hash: Bytes32,
    pub(crate) nonce: u64,
    pub(crate) base_fee_per_gas: Option<u64>,
}

impl Block {
    pub fn to_arrow(&self) -> Result<RecordBatch, ArrowError> {
        let Block {
            block_num,
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
        } = self;

        let columns = vec![
            block_num.to_arrow()?,
            hash.to_arrow()?,
            parent_hash.to_arrow()?,
            uncle_hash.to_arrow()?,
            coinbase.to_arrow()?,
            state_root.to_arrow()?,
            transactions_root.to_arrow()?,
            receipt_root.to_arrow()?,
            logs_bloom.to_arrow()?,
            difficulty.to_arrow()?,
            gas_limit.to_arrow()?,
            gas_used.to_arrow()?,
            timestamp.to_arrow()?,
            extra_data.to_arrow()?,
            mix_hash.to_arrow()?,
            nonce.to_arrow()?,
            base_fee_per_gas.to_arrow()?,
        ];

        Ok(RecordBatch::try_new(Arc::new(schema()), columns)?)
    }
}

fn schema() -> Schema {
    let number = Field::new("block_num", DataType::UInt64, false);
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

#[test]
fn default_to_arrow() {
    let block = Block::default();
    let batch = block.to_arrow().unwrap();
    assert_eq!(batch.num_columns(), 17);
    assert_eq!(batch.num_rows(), 1);
}
