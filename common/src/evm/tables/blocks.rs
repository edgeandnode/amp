use std::sync::Arc;

use crate::arrow;
use crate::{
    timestamp_type, Bytes32, Bytes32ArrayBuilder, EvmAddress as Address, EvmAddressArrayBuilder,
    EvmCurrency, EvmCurrencyArrayBuilder, Table, TableRows, Timestamp, TimestampArrayBuilder,
    BLOCK_NUM, BYTES32_TYPE, EVM_ADDRESS_TYPE as ADDRESS_TYPE, EVM_CURRENCY_TYPE,
};
use arrow::array::{ArrayRef, BinaryBuilder, UInt64Builder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;

lazy_static::lazy_static! {
    static ref SCHEMA: SchemaRef = Arc::new(schema());
}

pub fn table() -> Table {
    Table {
        name: TABLE_NAME.to_string(),
        schema: SCHEMA.clone(),
    }
}

pub const TABLE_NAME: &'static str = "blocks";

/// Prefer using the pre-computed SCHEMA
fn schema() -> Schema {
    let number = Field::new(BLOCK_NUM, DataType::UInt64, false);
    let timestamp = Field::new("timestamp", timestamp_type(), false);
    let hash = Field::new("hash", BYTES32_TYPE, false);
    let parent_hash = Field::new("parent_hash", BYTES32_TYPE, false);
    let ommers_hash = Field::new("ommers_hash", BYTES32_TYPE, false);
    let miner = Field::new("miner", ADDRESS_TYPE, false);
    let state_root = Field::new("state_root", BYTES32_TYPE, false);
    let transactions_root = Field::new("transactions_root", BYTES32_TYPE, false);
    let receipt_root = Field::new("receipt_root", BYTES32_TYPE, false);
    let logs_bloom = Field::new("logs_bloom", DataType::Binary, false);
    let difficulty = Field::new("difficulty", EVM_CURRENCY_TYPE, false);
    let gas_limit = Field::new("gas_limit", DataType::UInt64, false);
    let gas_used = Field::new("gas_used", DataType::UInt64, false);
    let extra_data = Field::new("extra_data", DataType::Binary, false);
    let mix_hash = Field::new("mix_hash", BYTES32_TYPE, false);
    let nonce = Field::new("nonce", DataType::UInt64, false);
    let base_fee_per_gas = Field::new("base_fee_per_gas", EVM_CURRENCY_TYPE, true);

    let fields = vec![
        number,
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
    ];

    Schema::new(fields)
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
    }

    pub fn build(self) -> Result<TableRows, ArrowError> {
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
        ];

        TableRows::new(table(), columns)
    }
}

#[test]
fn default_to_arrow() {
    let block = Block::default();
    let rows = {
        let mut builder = BlockRowsBuilder::with_capacity(1);
        builder.append(&block);
        builder.build().unwrap()
    };
    assert_eq!(rows.rows.num_columns(), 17);
    assert_eq!(rows.rows.num_rows(), 1);
}
