use std::sync::Arc;

use common::arrow::array::RecordBatch;
use common::arrow::datatypes::{DataType, Field, Schema};
use common::arrow::error::ArrowError;
use common::arrow_helpers::ScalarToArray as _;
use common::{
    Bytes, Bytes32, EvmAddress as Address, Table, TimestampSecond, BYTES32_TYPE,
    EVM_ADDRESS_TYPE as ADDRESS_TYPE,
};

pub fn table() -> Table {
    Table {
        name: TABLE_NAME.to_string(),
        schema: schema(),
    }
}

pub const TABLE_NAME: &'static str = "logs";

#[derive(Debug, Default)]
pub struct Log {
    pub(crate) block_num: u64,
    pub(crate) timestamp: TimestampSecond,
    pub(crate) tx_index: u32,
    pub(crate) call_index: u32,
    pub(crate) tx_hash: Bytes32,

    pub(crate) address: Address,
    pub(crate) topic0: Option<Bytes32>,
    pub(crate) topic1: Option<Bytes32>,
    pub(crate) topic2: Option<Bytes32>,
    pub(crate) topic3: Option<Bytes32>,

    pub(crate) data: Bytes,

    // Index of the log relative to the transaction.
    pub(crate) index: u32,

    // Index of the log relative to the block, 0 if the state was reverted.
    pub(crate) block_index: u32,

    // Unique identifier for the log's position in the blockchain.
    pub(crate) ordinal: u64,
}

impl Log {
    pub fn to_arrow(&self) -> Result<RecordBatch, ArrowError> {
        let Log {
            block_num,
            timestamp,
            tx_index,
            call_index,
            tx_hash,
            address,
            topic0,
            topic1,
            topic2,
            topic3,
            data,
            index,
            block_index,
            ordinal,
        } = self;

        let columns = vec![
            block_num.to_arrow()?,
            timestamp.to_arrow()?,
            tx_index.to_arrow()?,
            call_index.to_arrow()?,
            tx_hash.to_arrow()?,
            address.to_arrow()?,
            topic0.to_arrow()?,
            topic1.to_arrow()?,
            topic2.to_arrow()?,
            topic3.to_arrow()?,
            data.to_arrow()?,
            index.to_arrow()?,
            block_index.to_arrow()?,
            ordinal.to_arrow()?,
        ];

        RecordBatch::try_new(Arc::new(schema()), columns)
    }
}

pub fn schema() -> Schema {
    let block_num = Field::new("block_num", DataType::UInt64, false);
    let timestamp = Field::new("timestamp", common::timestamp_type(), false);
    let tx_index = Field::new("tx_index", DataType::UInt32, false);
    let call_index = Field::new("call_index", DataType::UInt32, false);
    let tx_hash = Field::new("tx_hash", BYTES32_TYPE, false);
    let address = Field::new("address", ADDRESS_TYPE, false);
    let topic0 = Field::new("topic0", BYTES32_TYPE, true);
    let topic1 = Field::new("topic1", BYTES32_TYPE, true);
    let topic2 = Field::new("topic2", BYTES32_TYPE, true);
    let topic3 = Field::new("topic3", BYTES32_TYPE, true);
    let data = Field::new("data", DataType::Binary, false);
    let index = Field::new("index", DataType::UInt32, false);
    let block_index = Field::new("block_index", DataType::UInt32, false);
    let ordinal = Field::new("ordinal", DataType::UInt64, false);

    let fields = vec![
        block_num,
        timestamp,
        tx_index,
        call_index,
        tx_hash,
        address,
        topic0,
        topic1,
        topic2,
        topic3,
        data,
        index,
        block_index,
        ordinal,
    ];

    Schema::new(fields)
}

#[test]
fn default_to_arrow() {
    let log = Log::default();
    let batch = log.to_arrow().unwrap();
    assert_eq!(batch.num_columns(), 14);
    assert_eq!(batch.num_rows(), 1);
}
