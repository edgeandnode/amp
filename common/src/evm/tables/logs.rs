use std::sync::{Arc, LazyLock};

use arrow::{
    array::{ArrayRef, BinaryBuilder, UInt32Builder, UInt64Builder},
    datatypes::{DataType, Field, Schema, SchemaRef},
};

use crate::{
    arrow, timestamp_type, BoxError, Bytes32, Bytes32ArrayBuilder, EvmAddress as Address,
    EvmAddressArrayBuilder, RawTableBlock, RawTableRows, Table, Timestamp, TimestampArrayBuilder,
    BLOCK_NUM, BYTES32_TYPE, EVM_ADDRESS_TYPE as ADDRESS_TYPE,
};

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(schema()));

pub fn table(network: String) -> Table {
    Table {
        name: TABLE_NAME.to_string(),
        schema: SCHEMA.clone(),
        network: network,
    }
}

pub const TABLE_NAME: &'static str = "logs";

/// Prefer using the pre-computed SCHEMA
fn schema() -> Schema {
    let block_hash = Field::new("block_hash", BYTES32_TYPE, false);
    let block_num = Field::new(BLOCK_NUM, DataType::UInt64, false);
    let timestamp = Field::new("timestamp", timestamp_type(), false);
    let tx_hash = Field::new("tx_hash", BYTES32_TYPE, false);
    let tx_index = Field::new("tx_index", DataType::UInt32, false);
    let log_index = Field::new("log_index", DataType::UInt32, false);
    let address = Field::new("address", ADDRESS_TYPE, false);
    let topic0 = Field::new("topic0", BYTES32_TYPE, true);
    let topic1 = Field::new("topic1", BYTES32_TYPE, true);
    let topic2 = Field::new("topic2", BYTES32_TYPE, true);
    let topic3 = Field::new("topic3", BYTES32_TYPE, true);
    let data = Field::new("data", DataType::Binary, false);

    let fields = vec![
        block_hash, block_num, timestamp, tx_hash, tx_index, log_index, address, topic0, topic1,
        topic2, topic3, data,
    ];

    Schema::new(fields)
}

#[derive(Debug, Default)]
pub struct Log {
    pub block_hash: Bytes32,
    pub block_num: u64,
    pub timestamp: Timestamp,
    pub tx_index: u32,
    pub tx_hash: Bytes32,

    // Index of the log relative to the block, 0 if the state was reverted.
    pub log_index: u32,

    pub address: Address,
    pub topic0: Option<Bytes32>,
    pub topic1: Option<Bytes32>,
    pub topic2: Option<Bytes32>,
    pub topic3: Option<Bytes32>,
    pub data: Vec<u8>,
}

#[derive(Debug)]
pub struct LogRowsBuilder {
    block_hash: Bytes32ArrayBuilder,
    block_num: UInt64Builder,
    timestamp: TimestampArrayBuilder,
    tx_index: UInt32Builder,
    tx_hash: Bytes32ArrayBuilder,
    address: EvmAddressArrayBuilder,
    topic0: Bytes32ArrayBuilder,
    topic1: Bytes32ArrayBuilder,
    topic2: Bytes32ArrayBuilder,
    topic3: Bytes32ArrayBuilder,
    data: BinaryBuilder,
    log_index: UInt32Builder,
}

impl LogRowsBuilder {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            block_hash: Bytes32ArrayBuilder::with_capacity(capacity),
            block_num: UInt64Builder::with_capacity(capacity),
            timestamp: TimestampArrayBuilder::with_capacity(capacity),
            tx_index: UInt32Builder::with_capacity(capacity),
            tx_hash: Bytes32ArrayBuilder::with_capacity(capacity),
            address: EvmAddressArrayBuilder::with_capacity(capacity),
            topic0: Bytes32ArrayBuilder::with_capacity(capacity),
            topic1: Bytes32ArrayBuilder::with_capacity(capacity),
            topic2: Bytes32ArrayBuilder::with_capacity(capacity),
            topic3: Bytes32ArrayBuilder::with_capacity(capacity),
            data: BinaryBuilder::with_capacity(capacity, 0),
            log_index: UInt32Builder::with_capacity(capacity),
        }
    }

    pub fn append(&mut self, log: &Log) {
        let Log {
            block_hash,
            block_num,
            timestamp,
            tx_index,
            tx_hash,
            address,
            topic0,
            topic1,
            topic2,
            topic3,
            data,
            log_index,
        } = log;

        self.block_hash.append_value(*block_hash);
        self.block_num.append_value(*block_num);
        self.timestamp.append_value(*timestamp);
        self.tx_index.append_value(*tx_index);
        self.tx_hash.append_value(*tx_hash);
        self.address.append_value(*address);
        self.topic0.append_option(*topic0);
        self.topic1.append_option(*topic1);
        self.topic2.append_option(*topic2);
        self.topic3.append_option(*topic3);
        self.data.append_value(data);
        self.log_index.append_value(*log_index);
    }

    pub fn build(self, block: RawTableBlock) -> Result<RawTableRows, BoxError> {
        let Self {
            block_hash,
            mut block_num,
            mut timestamp,
            mut tx_index,
            tx_hash,
            address,
            topic0,
            topic1,
            topic2,
            topic3,
            mut data,
            mut log_index,
        } = self;

        let columns = vec![
            Arc::new(block_hash.finish()) as ArrayRef,
            Arc::new(block_num.finish()),
            Arc::new(timestamp.finish()),
            Arc::new(tx_hash.finish()),
            Arc::new(tx_index.finish()),
            Arc::new(log_index.finish()),
            Arc::new(address.finish()),
            Arc::new(topic0.finish()),
            Arc::new(topic1.finish()),
            Arc::new(topic2.finish()),
            Arc::new(topic3.finish()),
            Arc::new(data.finish()),
        ];

        RawTableRows::new(table(block.network.clone()), block, columns)
    }
}

#[test]
fn default_to_arrow() {
    let log = Log::default();
    let rows = {
        let mut builder = LogRowsBuilder::with_capacity(1);
        builder.append(&log);
        builder
            .build(RawTableBlock {
                number: log.block_num,
                network: "test_network".to_string(),
            })
            .unwrap()
    };
    assert_eq!(rows.rows.num_columns(), 12);
    assert_eq!(rows.rows.num_rows(), 1);
}
