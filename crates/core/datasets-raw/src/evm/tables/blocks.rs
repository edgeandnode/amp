use std::sync::{Arc, LazyLock};

use arrow::{
    array::{ArrayRef, BinaryBuilder, UInt64Builder},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use datasets_common::{
    block_range::BlockRange,
    dataset::{SPECIAL_BLOCK_NUM, Table},
};

use crate::{
    Timestamp,
    arrow::TimestampArrayBuilder,
    evm::{
        BYTES32_TYPE, Bytes32, EVM_ADDRESS_TYPE as ADDRESS_TYPE, EVM_CURRENCY_TYPE,
        EvmAddress as Address, EvmCurrency,
        helpers::{Bytes32ArrayBuilder, EvmAddressArrayBuilder, EvmCurrencyArrayBuilder},
    },
    rows::{TableRowError, TableRows},
    timestamp_type,
};

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(schema()));

pub fn table(network: String) -> Table {
    let name = TABLE_NAME.parse().expect("table name is valid");
    Table::new(
        name,
        SCHEMA.clone(),
        network,
        vec!["block_num".to_string(), "timestamp".to_string()],
    )
}

pub const TABLE_NAME: &str = "blocks";

/// Prefer using the pre-computed SCHEMA
fn schema() -> Schema {
    Schema::new(vec![
        Field::new(SPECIAL_BLOCK_NUM, DataType::UInt64, false),
        Field::new("block_num", DataType::UInt64, false),
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
        Field::new("total_difficulty", EVM_CURRENCY_TYPE, true),
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
        Field::new("requests_hash", BYTES32_TYPE, true),
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
    pub total_difficulty: Option<EvmCurrency>,
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
    pub requests_hash: Option<Bytes32>,
}

pub struct BlockRowsBuilder {
    special_block_num: UInt64Builder,
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
    total_difficulty: EvmCurrencyArrayBuilder,
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
    requests_hash: Bytes32ArrayBuilder,
}

impl BlockRowsBuilder {
    pub fn with_capacity_for(header: &Block) -> Self {
        Self {
            special_block_num: UInt64Builder::with_capacity(1),
            block_num: UInt64Builder::with_capacity(1),
            timestamp: TimestampArrayBuilder::with_capacity(1),
            hash: Bytes32ArrayBuilder::with_capacity(1),
            parent_hash: Bytes32ArrayBuilder::with_capacity(1),
            ommers_hash: Bytes32ArrayBuilder::with_capacity(1),
            miner: EvmAddressArrayBuilder::with_capacity(1),
            state_root: Bytes32ArrayBuilder::with_capacity(1),
            transactions_root: Bytes32ArrayBuilder::with_capacity(1),
            receipt_root: Bytes32ArrayBuilder::with_capacity(1),
            logs_bloom: BinaryBuilder::with_capacity(1, header.logs_bloom.len()),
            difficulty: EvmCurrencyArrayBuilder::with_capacity(1),
            total_difficulty: EvmCurrencyArrayBuilder::with_capacity(1),
            gas_limit: UInt64Builder::with_capacity(1),
            gas_used: UInt64Builder::with_capacity(1),
            extra_data: BinaryBuilder::with_capacity(1, header.extra_data.len()),
            mix_hash: Bytes32ArrayBuilder::with_capacity(1),
            nonce: UInt64Builder::with_capacity(1),
            base_fee_per_gas: EvmCurrencyArrayBuilder::with_capacity(1),
            withdrawals_root: Bytes32ArrayBuilder::with_capacity(1),
            blob_gas_used: UInt64Builder::with_capacity(1),
            excess_blob_gas: UInt64Builder::with_capacity(1),
            parent_beacon_root: Bytes32ArrayBuilder::with_capacity(1),
            requests_hash: Bytes32ArrayBuilder::with_capacity(1),
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
            total_difficulty,
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
            requests_hash,
        } = row;

        self.special_block_num.append_value(*block_num);
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
        self.total_difficulty.append_option(*total_difficulty);
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
        self.requests_hash.append_option(*requests_hash);
    }

    pub fn build(self, range: BlockRange) -> Result<TableRows, TableRowError> {
        let Self {
            mut special_block_num,
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
            total_difficulty,
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
            requests_hash,
        } = self;

        let columns = vec![
            Arc::new(special_block_num.finish()) as ArrayRef,
            Arc::new(block_num.finish()),
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
            Arc::new(total_difficulty.finish()),
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
            Arc::new(requests_hash.finish()),
        ];

        TableRows::new(table(range.network.clone()), range, columns)
    }
}

#[test]
fn default_to_arrow() {
    let block = Block::default();
    let rows = {
        let mut builder = BlockRowsBuilder::with_capacity_for(&block);
        builder.append(&block);
        builder
            .build(BlockRange {
                numbers: block.block_num..=block.block_num,
                network: "test_network".to_string(),
                hash: block.hash.into(),
                prev_hash: Some(block.parent_hash.into()),
            })
            .unwrap()
    };
    assert_eq!(rows.rows.num_columns(), 24);
    assert_eq!(rows.rows.num_rows(), 1);
}
