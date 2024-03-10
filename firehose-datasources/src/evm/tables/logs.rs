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

pub const TABLE_NAME: &'static str = "logs";

#[derive(Debug, Serialize)]
pub struct Log {
    pub(crate) block_num: u64,
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

fn schema() -> Schema {
    let block_num = Field::new("block_num", DataType::UInt64, false);
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
