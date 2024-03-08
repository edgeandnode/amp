use common::arrow::datatypes::{DataType, Field, Schema};
use common::{
    Bytes, Bytes32, EvmAddress as Address, Table, BYTES32_TYPE, EVM_ADDRESS_TYPE as ADDRESS_TYPE,
};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct Log {
    pub block_num: u64,
    pub tx_index: u32,
    pub call_index: u32,
    pub tx_hash: Bytes32,

    pub address: Address,
    pub topic0: Option<Bytes32>,
    pub topic1: Option<Bytes32>,
    pub topic2: Option<Bytes32>,
    pub topic3: Option<Bytes32>,

    pub data: Bytes,

    // Index of the log relative to the transaction.
    pub index: u32,

    // Index of the log relative to the block, 0 if the state was reverted.
    pub block_index: u32,

    // Unique identifier for the log's position in the blockchain.
    pub ordinal: u64,
}

impl Table for Log {
    const TABLE_NAME: &'static str = "logs";

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
}
