use std::sync::{Arc, LazyLock};

use crate::arrow;
use crate::{
    timestamp_type, Bytes32, Bytes32ArrayBuilder, EvmAddress as Address, EvmAddressArrayBuilder,
    EvmCurrency, EvmCurrencyArrayBuilder, Table, TableRows, Timestamp, TimestampArrayBuilder,
    BLOCK_NUM, BYTES32_TYPE, EVM_ADDRESS_TYPE as ADDRESS_TYPE, EVM_CURRENCY_TYPE,
};
use arrow::array::{ArrayRef, BinaryBuilder, UInt64Builder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;

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

pub struct BlockRowsBuilder {
    block_num: UInt64Builder,
    timestamp: TimestampArrayBuilder,
    hash: Bytes32ArrayBuilder,
    parent_hash: Bytes32ArrayBuilder,
    ommers_hash: Bytes32ArrayBuilder,
    miner: EvmAddressArrayBuilder,
    state_root: Bytes32ArrayBuilder,
    transactions_root: Bytes32ArrayBuilder,
    receipt_root: Bytes32ArrayBuilder,
    logs_bloom: BinaryBuilder,
    difficulty: EvmCurrencyArrayBuilder,
    gas_limit: UInt64Builder,
    gas_used: UInt64Builder,
    extra_data: BinaryBuilder,
    mix_hash: Bytes32ArrayBuilder,
    nonce: UInt64Builder,
    base_fee_per_gas: EvmCurrencyArrayBuilder,
    withdrawals_root: Bytes32ArrayBuilder,
    blob_gas_used: UInt64Builder,
    excess_blob_gas: UInt64Builder,
    parent_beacon_root: Bytes32ArrayBuilder,
}

impl BlockRowsBuilder {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            block_num: UInt64Builder::with_capacity(capacity),
            timestamp: TimestampArrayBuilder::with_capacity(capacity),
            hash: Bytes32ArrayBuilder::with_capacity(capacity),
            parent_hash: Bytes32ArrayBuilder::with_capacity(capacity),
            ommers_hash: Bytes32ArrayBuilder::with_capacity(capacity),
            miner: EvmAddressArrayBuilder::with_capacity(capacity),
            state_root: Bytes32ArrayBuilder::with_capacity(capacity),
            transactions_root: Bytes32ArrayBuilder::with_capacity(capacity),
            receipt_root: Bytes32ArrayBuilder::with_capacity(capacity),
            logs_bloom: BinaryBuilder::with_capacity(capacity, 0),
            difficulty: EvmCurrencyArrayBuilder::with_capacity(capacity),
            gas_limit: UInt64Builder::with_capacity(capacity),
            gas_used: UInt64Builder::with_capacity(capacity),
            extra_data: BinaryBuilder::with_capacity(capacity, 0),
            mix_hash: Bytes32ArrayBuilder::with_capacity(capacity),
            nonce: UInt64Builder::with_capacity(capacity),
            base_fee_per_gas: EvmCurrencyArrayBuilder::with_capacity(capacity),
            withdrawals_root: Bytes32ArrayBuilder::with_capacity(capacity),
            blob_gas_used: UInt64Builder::with_capacity(capacity),
            excess_blob_gas: UInt64Builder::with_capacity(capacity),
            parent_beacon_root: Bytes32ArrayBuilder::with_capacity(capacity),
        }
    }

    pub fn append(&mut self, row: &Block) {
        let Block {
            block_num,
            timestamp,
            hash,
            parent_hash,
            ommers_hash,
            miner,
            state_root,
            transactions_root,
            receipt_root,
            logs_bloom,
            difficulty,
            gas_limit,
            gas_used,
            extra_data,
            mix_hash,
            nonce,
            base_fee_per_gas,
            withdrawals_root,
            blob_gas_used,
            excess_blob_gas,
            parent_beacon_root,
        } = row;

        self.block_num.append_value(*block_num);
        self.timestamp.append_value(*timestamp);
        self.hash.append_value(*hash);
        self.parent_hash.append_value(*parent_hash);
        self.ommers_hash.append_value(*ommers_hash);
        self.miner.append_value(*miner);
        self.state_root.append_value(*state_root);
        self.transactions_root.append_value(*transactions_root);
        self.receipt_root.append_value(*receipt_root);
        self.logs_bloom.append_value(logs_bloom);
        self.difficulty.append_value(*difficulty);
        self.gas_limit.append_value(*gas_limit);
        self.gas_used.append_value(*gas_used);
        self.extra_data.append_value(extra_data);
        self.mix_hash.append_value(*mix_hash);
        self.nonce.append_value(*nonce);
        self.base_fee_per_gas.append_option(*base_fee_per_gas);
        self.withdrawals_root.append_option(*withdrawals_root);
        self.blob_gas_used.append_option(*blob_gas_used);
        self.excess_blob_gas.append_option(*excess_blob_gas);
        self.parent_beacon_root.append_option(*parent_beacon_root);
    }

    pub fn build(self, network: String) -> Result<TableRows, ArrowError> {
        let Self {
            mut block_num,
            mut timestamp,
            hash,
            parent_hash,
            ommers_hash,
            miner,
            state_root,
            transactions_root,
            receipt_root,
            mut logs_bloom,
            difficulty,
            mut gas_limit,
            mut gas_used,
            mut extra_data,
            mix_hash,
            mut nonce,
            base_fee_per_gas,
            withdrawals_root,
            mut blob_gas_used,
            mut excess_blob_gas,
            parent_beacon_root,
        } = self;

        let columns = vec![
            Arc::new(block_num.finish()) as ArrayRef,
            Arc::new(timestamp.finish()),
            Arc::new(hash.finish()),
            Arc::new(parent_hash.finish()),
            Arc::new(ommers_hash.finish()),
            Arc::new(miner.finish()),
            Arc::new(state_root.finish()),
            Arc::new(transactions_root.finish()),
            Arc::new(receipt_root.finish()),
            Arc::new(logs_bloom.finish()),
            Arc::new(difficulty.finish()),
            Arc::new(gas_limit.finish()),
            Arc::new(gas_used.finish()),
            Arc::new(extra_data.finish()),
            Arc::new(mix_hash.finish()),
            Arc::new(nonce.finish()),
            Arc::new(base_fee_per_gas.finish()),
            Arc::new(withdrawals_root.finish()),
            Arc::new(blob_gas_used.finish()),
            Arc::new(excess_blob_gas.finish()),
            Arc::new(parent_beacon_root.finish()),
        ];

        TableRows::new(table(network), columns)
    }
}

#[test]
fn default_to_arrow() {
    let block = Block::default();
    let rows = {
        let mut builder = BlockRowsBuilder::with_capacity(1);
        builder.append(&block);
        builder.build("test_network".to_string()).unwrap()
    };
    assert_eq!(rows.rows.num_columns(), 21);
    assert_eq!(rows.rows.num_rows(), 1);
}
