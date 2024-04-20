use std::sync::Arc;

use common::arrow::array::{ArrayRef, BinaryBuilder, UInt32Builder, UInt64Builder};
use common::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use common::arrow::error::ArrowError;
use common::{
    Bytes, Bytes32, Bytes32ArrayBuilder, EvmAddress as Address, EvmAddressArrayBuilder, Table,
    TableRows, Timestamp, TimestampArrayBuilder, BLOCK_NUM, BYTES32_TYPE,
    EVM_ADDRESS_TYPE as ADDRESS_TYPE,
};

lazy_static::lazy_static! {
    static ref SCHEMA: SchemaRef = Arc::new(schema());
}

pub fn table() -> Table {
    Table {
        name: TABLE_NAME.to_string(),
        schema: SCHEMA.clone(),
    }
}

pub const TABLE_NAME: &'static str = "logs";

/// Prefer using the pre-computed SCHEMA
fn schema() -> Schema {
    let block_num = Field::new(BLOCK_NUM, DataType::UInt64, false);
    let timestamp = Field::new("timestamp", common::timestamp_type(), false);
    let tx_index = Field::new("tx_index", DataType::UInt32, false);
    let tx_hash = Field::new("tx_hash", BYTES32_TYPE, false);
    let address = Field::new("address", ADDRESS_TYPE, false);
    let topic0 = Field::new("topic0", BYTES32_TYPE, true);
    let topic1 = Field::new("topic1", BYTES32_TYPE, true);
    let topic2 = Field::new("topic2", BYTES32_TYPE, true);
    let topic3 = Field::new("topic3", BYTES32_TYPE, true);
    let data = Field::new("data", DataType::Binary, false);
    let block_index = Field::new("block_index", DataType::UInt32, false);
    let index = Field::new("index", DataType::UInt32, false);
    let call_index = Field::new("call_index", DataType::UInt32, false);
    let ordinal = Field::new("ordinal", DataType::UInt64, false);

    let fields = vec![
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
        block_index,
        index,
        call_index,
        ordinal,
    ];

    Schema::new(fields)
}

#[derive(Debug, Default)]
pub struct Log {
    pub(crate) block_num: u64,
    pub(crate) timestamp: Timestamp,
    pub(crate) tx_index: u32,
    pub(crate) tx_hash: Bytes32,

    pub(crate) address: Address,
    pub(crate) topic0: Option<Bytes32>,
    pub(crate) topic1: Option<Bytes32>,
    pub(crate) topic2: Option<Bytes32>,
    pub(crate) topic3: Option<Bytes32>,

    pub(crate) data: Bytes,

    // Index of the log relative to the block, 0 if the state was reverted.
    pub(crate) block_index: u32,

    // Firehose specific.
    //
    // Index of the log relative to the transaction.
    pub(crate) index: u32,
    pub(crate) call_index: u32,
    pub(crate) ordinal: u64,
}

#[derive(Debug)]
pub(crate) struct LogRowsBuilder {
    block_num: UInt64Builder,
    timestamp: TimestampArrayBuilder,
    tx_index: UInt32Builder,
    call_index: UInt32Builder,
    tx_hash: Bytes32ArrayBuilder,
    address: EvmAddressArrayBuilder,
    topic0: Bytes32ArrayBuilder,
    topic1: Bytes32ArrayBuilder,
    topic2: Bytes32ArrayBuilder,
    topic3: Bytes32ArrayBuilder,
    data: BinaryBuilder,
    index: UInt32Builder,
    block_index: UInt32Builder,
    ordinal: UInt64Builder,
}

impl LogRowsBuilder {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            block_num: UInt64Builder::with_capacity(capacity),
            timestamp: TimestampArrayBuilder::with_capacity(capacity),
            tx_index: UInt32Builder::with_capacity(capacity),
            call_index: UInt32Builder::with_capacity(capacity),
            tx_hash: Bytes32ArrayBuilder::with_capacity(capacity),
            address: EvmAddressArrayBuilder::with_capacity(capacity),
            topic0: Bytes32ArrayBuilder::with_capacity(capacity),
            topic1: Bytes32ArrayBuilder::with_capacity(capacity),
            topic2: Bytes32ArrayBuilder::with_capacity(capacity),
            topic3: Bytes32ArrayBuilder::with_capacity(capacity),
            data: BinaryBuilder::with_capacity(capacity, 0),
            index: UInt32Builder::with_capacity(capacity),
            block_index: UInt32Builder::with_capacity(capacity),
            ordinal: UInt64Builder::with_capacity(capacity),
        }
    }

    pub(crate) fn append(&mut self, log: &Log) {
        let Log {
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
            block_index,
            index,
            call_index,
            ordinal,
        } = log;

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
        self.block_index.append_value(*block_index);
        self.index.append_value(*index);
        self.call_index.append_value(*call_index);
        self.ordinal.append_value(*ordinal);
    }

    pub(crate) fn build(self) -> Result<TableRows, ArrowError> {
        let Self {
            mut block_num,
            timestamp,
            mut tx_index,
            tx_hash,
            address,
            topic0,
            topic1,
            topic2,
            topic3,
            mut data,
            mut block_index,
            mut index,
            mut call_index,
            mut ordinal,
        } = self;

        let columns = vec![
            Arc::new(block_num.finish()) as ArrayRef,
            Arc::new(timestamp.finish()),
            Arc::new(tx_index.finish()),
            Arc::new(tx_hash.finish()),
            Arc::new(address.finish()),
            Arc::new(topic0.finish()),
            Arc::new(topic1.finish()),
            Arc::new(topic2.finish()),
            Arc::new(topic3.finish()),
            Arc::new(data.finish()),
            Arc::new(block_index.finish()),
            Arc::new(index.finish()),
            Arc::new(call_index.finish()),
            Arc::new(ordinal.finish()),
        ];

        TableRows::new(table(), columns)
    }
}

#[test]
fn default_to_arrow() {
    let log = Log::default();
    let rows = {
        let mut builder = LogRowsBuilder::with_capacity(1);
        builder.append(&log);
        builder.build().unwrap()
    };
    assert_eq!(rows.rows.num_columns(), 14);
    assert_eq!(rows.rows.num_rows(), 1);
}
