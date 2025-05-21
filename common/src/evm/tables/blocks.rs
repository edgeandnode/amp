use std::sync::{Arc, LazyLock};

use arrow::{
    array::ArrayRef,
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use datafusion::arrow::array::{BinaryArray, FixedSizeBinaryArray, UInt64Array};

use crate::{
    arrow, timestamp_type, BoxError, Bytes32, Bytes32ArrayBuilder, EvmAddress as Address,
    EvmCurrency, EvmCurrencyArrayBuilder, RawTableRows, Table, Timestamp, TimestampArrayBuilder,
    BLOCK_NUM, BYTES32_TYPE, EVM_ADDRESS_TYPE as ADDRESS_TYPE, EVM_CURRENCY_TYPE,
};

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(schema()));

pub fn table(network: String) -> Table {
    Table {
        name: TABLE_NAME.to_string(),
        schema: SCHEMA.clone(),
        network: Some(network),
    }
}

pub const TABLE_NAME: &'static str = "blocks";

/// Prefer using the pre-computed SCHEMA
fn schema() -> Schema {
    Schema::new(vec![
        Field::new(BLOCK_NUM, DataType::UInt64, false),
        Field::new("timestamp", timestamp_type(), false),
        Field::new("hash", BYTES32_TYPE, false),
        Field::new("parent_hash", BYTES32_TYPE, false),
        Field::new("ommers_hash", BYTES32_TYPE, false),
        Field::new("miner", ADDRESS_TYPE, false),
        Field::new("state_root", BYTES32_TYPE, false),
        Field::new("transactions_root", BYTES32_TYPE, false),
        Field::new("receipt_root", BYTES32_TYPE, false),
        Field::new("logs_bloom", DataType::Binary, false),
        Field::new("difficulty", EVM_CURRENCY_TYPE, false),
        Field::new("gas_limit", DataType::UInt64, false),
        Field::new("gas_used", DataType::UInt64, false),
        Field::new("extra_data", DataType::Binary, false),
        Field::new("mix_hash", BYTES32_TYPE, false),
        Field::new("nonce", DataType::UInt64, false),
        Field::new("base_fee_per_gas", EVM_CURRENCY_TYPE, true),
        Field::new("withdrawals_root", BYTES32_TYPE, true),
        Field::new("blob_gas_used", DataType::UInt64, true),
        Field::new("excess_blob_gas", DataType::UInt64, true),
        Field::new("parent_beacon_root", BYTES32_TYPE, true),
    ])
}

#[derive(Debug, Default)]
pub struct Block {
    pub block_num: u64,
    pub timestamp: Timestamp,

    pub hash: Bytes32,
    pub parent_hash: Bytes32,

    pub ommers_hash: Bytes32,
    pub miner: Address,
    pub state_root: Bytes32,
    pub transactions_root: Bytes32,
    pub receipt_root: Bytes32,
    pub logs_bloom: Vec<u8>,

    // Difficulty is not really currency, but fits in a i128 so EvmCurrency is convenient.
    pub difficulty: EvmCurrency,
    pub gas_limit: u64,
    pub gas_used: u64,
    pub extra_data: Vec<u8>,
    pub mix_hash: Bytes32,
    pub nonce: u64,
    pub base_fee_per_gas: Option<EvmCurrency>,
    pub withdrawals_root: Option<Bytes32>,
    pub blob_gas_used: Option<u64>,
    pub excess_blob_gas: Option<u64>,
    pub parent_beacon_root: Option<Bytes32>,
}

impl Block {
    pub fn raw_table_rows(&self, network: String) -> Result<RawTableRows, BoxError> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(vec![self.block_num])),
            Arc::new({
                let mut column = TimestampArrayBuilder::with_capacity(1);
                column.append_value(self.timestamp);
                column.finish()
            }),
            Arc::new(FixedSizeBinaryArray::from(vec![&self.hash])),
            Arc::new(FixedSizeBinaryArray::from(vec![&self.parent_hash])),
            Arc::new(FixedSizeBinaryArray::from(vec![&self.ommers_hash])),
            Arc::new(FixedSizeBinaryArray::from(vec![&self.miner])),
            Arc::new(FixedSizeBinaryArray::from(vec![&self.state_root])),
            Arc::new(FixedSizeBinaryArray::from(vec![&self.transactions_root])),
            Arc::new(FixedSizeBinaryArray::from(vec![&self.receipt_root])),
            Arc::new(BinaryArray::from_vec(vec![&self.logs_bloom])),
            Arc::new({
                let mut column = EvmCurrencyArrayBuilder::with_capacity(1);
                column.append_value(self.difficulty);
                column.finish()
            }),
            Arc::new(UInt64Array::from(vec![self.gas_limit])),
            Arc::new(UInt64Array::from(vec![self.gas_used])),
            Arc::new(BinaryArray::from_vec(vec![&self.extra_data])),
            Arc::new(FixedSizeBinaryArray::from(vec![&self.mix_hash])),
            Arc::new(UInt64Array::from(vec![self.nonce])),
            Arc::new({
                let mut column = EvmCurrencyArrayBuilder::with_capacity(1);
                column.append_option(self.base_fee_per_gas);
                column.finish()
            }),
            Arc::new({
                let mut column = Bytes32ArrayBuilder::with_capacity(1);
                column.append_option(self.withdrawals_root);
                column.finish()
            }),
            Arc::new(UInt64Array::from(vec![self.blob_gas_used])),
            Arc::new(UInt64Array::from(vec![self.excess_blob_gas])),
            Arc::new({
                let mut column = Bytes32ArrayBuilder::with_capacity(1);
                column.append_option(self.parent_beacon_root);
                column.finish()
            }),
        ];
        RawTableRows::new(table(network), columns)
    }
}

#[test]
fn default_to_arrow() {
    let block = Block::default();
    let rows = block.raw_table_rows("test_network".to_string()).unwrap();
    assert_eq!(rows.rows.num_columns(), 21);
    assert_eq!(rows.rows.num_rows(), 1);
}
