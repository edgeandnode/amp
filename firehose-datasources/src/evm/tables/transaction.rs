use common::arrow::datatypes::{DataType, Field, Schema};
use common::{
    Bytes, Bytes32, EvmAddress as Address, EvmCurrency, Table, BYTES32_TYPE,
    EVM_ADDRESS_TYPE as ADDRESS_TYPE, EVM_CURRENCY_TYPE,
};
use serde::Serialize;

pub fn table() -> Table {
    Table {
        name: TABLE_NAME.to_string(),
        schema: schema(),
    }
}

pub const TABLE_NAME: &'static str = "transactions";

#[derive(Debug, Serialize)]
pub struct Transaction {
    pub(crate) block_num: u64,
    pub(crate) tx_index: u32,
    pub(crate) tx_hash: Bytes32,

    pub(crate) to: Bytes,
    pub(crate) nonce: u64,
    pub(crate) gas_price: EvmCurrency,
    pub(crate) gas_limit: u64,

    // Value is the amount of Ether transferred as part of this transaction.
    pub(crate) value: EvmCurrency,

    // Input data the transaction will receive for EVM execution.
    pub(crate) input: Bytes,

    // Elliptic curve parameters.
    pub(crate) v: Bytes,
    pub(crate) r: Bytes,
    pub(crate) s: Bytes,

    // GasUsed is the total amount of gas unit used for the whole execution of the transaction.
    pub(crate) gas_used: u64,

    pub(crate) r#type: i32,
    pub(crate) max_fee_per_gas: EvmCurrency,
    pub(crate) max_priority_fee_per_gas: Option<EvmCurrency>,
    pub(crate) from: Address,
    pub(crate) return_data: Bytes,
    pub(crate) public_key: Bytes,
    pub(crate) begin_ordinal: u64,
    pub(crate) end_ordinal: u64,
}

fn schema() -> Schema {
    let block_num = Field::new("block_num", DataType::UInt64, false);
    let tx_index = Field::new("tx_index", DataType::UInt32, false);
    let tx_hash = Field::new("tx_hash", BYTES32_TYPE, false);
    let to = Field::new("to", DataType::Binary, false);
    let nonce = Field::new("nonce", DataType::UInt64, false);
    let gas_price = Field::new("gas_price", EVM_CURRENCY_TYPE, false);
    let gas_limit = Field::new("gas_limit", DataType::UInt64, false);
    let value = Field::new("value", EVM_CURRENCY_TYPE, false);
    let input = Field::new("input", DataType::Binary, false);
    let v = Field::new("v", DataType::Binary, false);
    let r = Field::new("r", DataType::Binary, false);
    let s = Field::new("s", DataType::Binary, false);
    let gas_used = Field::new("gas_used", DataType::UInt64, false);
    let r#type = Field::new("type", DataType::Int32, false);
    let max_fee_per_gas = Field::new("max_fee_per_gas", EVM_CURRENCY_TYPE, false);
    let max_priority_fee_per_gas = Field::new("max_priority_fee_per_gas", EVM_CURRENCY_TYPE, true);
    let from = Field::new("from", ADDRESS_TYPE, false);
    let return_data = Field::new("return_data", DataType::Binary, false);
    let public_key = Field::new("public_key", DataType::Binary, false);
    let begin_ordinal = Field::new("begin_ordinal", DataType::UInt64, false);
    let end_ordinal = Field::new("end_ordinal", DataType::UInt64, false);

    let fields = vec![
        block_num,
        tx_index,
        tx_hash,
        to,
        nonce,
        gas_price,
        gas_limit,
        value,
        input,
        v,
        r,
        s,
        gas_used,
        r#type,
        max_fee_per_gas,
        max_priority_fee_per_gas,
        from,
        return_data,
        public_key,
        begin_ordinal,
        end_ordinal,
    ];

    Schema::new(fields)
}
