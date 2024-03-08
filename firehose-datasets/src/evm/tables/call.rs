use common::arrow::datatypes::{DataType, Field, Schema};
use common::{
    Bytes, Bytes32, EvmAddress as Address, EvmCurrency, Table, BYTES32_TYPE,
    EVM_ADDRESS_TYPE as ADDRESS_TYPE, EVM_CURRENCY_TYPE,
};
use serde::Serialize;

/// Represents successful calls.
///
/// The Firehose call model is much richer, and there is more we can easily add here.
#[derive(Debug, Serialize)]
pub struct Call {
    pub block_num: u64,
    pub tx_index: u32,
    pub tx_hash: Bytes32,
    pub index: u32,

    pub parent_index: u32,
    pub depth: u32,
    pub caller: Address,
    pub address: Address,
    pub value: Option<EvmCurrency>,
    pub gas_limit: u64,
    pub gas_consumed: u64,
    pub return_data: Bytes,
    pub input: Bytes,
    pub executed_code: bool,
    pub selfdestruct: bool,
    pub begin_ordinal: u64,
    pub end_ordinal: u64,
}

impl Table for Call {
    const TABLE_NAME: &'static str = "call";

    fn schema() -> Schema {
        let block_num = Field::new("block_num", DataType::UInt64, false);
        let tx_index = Field::new("tx_index", DataType::UInt32, false);
        let tx_hash = Field::new("tx_hash", BYTES32_TYPE, false);
        let index = Field::new("index", DataType::UInt32, false);
        let parent_index = Field::new("parent_index", DataType::UInt32, false);
        let depth = Field::new("depth", DataType::UInt32, false);
        let caller = Field::new("caller", ADDRESS_TYPE, false);
        let address = Field::new("address", ADDRESS_TYPE, false);
        let value = Field::new("value", EVM_CURRENCY_TYPE, true);
        let gas_limit = Field::new("gas_limit", DataType::UInt64, false);
        let gas_consumed = Field::new("gas_consumed", DataType::UInt64, false);
        let return_data = Field::new("return_data", DataType::Binary, false);
        let input = Field::new("input", DataType::Binary, false);
        let executed_code = Field::new("executed_code", DataType::Boolean, false);
        let selfdestruct = Field::new("selfdestruct", DataType::Boolean, false);
        let begin_ordinal = Field::new("begin_ordinal", DataType::UInt64, false);
        let end_ordinal = Field::new("end_ordinal", DataType::UInt64, false);

        let fields = vec![
            block_num,
            tx_index,
            tx_hash,
            index,
            parent_index,
            depth,
            caller,
            address,
            value,
            gas_limit,
            gas_consumed,
            return_data,
            input,
            executed_code,
            selfdestruct,
            begin_ordinal,
            end_ordinal,
        ];

        Schema::new(fields)
    }
}
