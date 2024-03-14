use std::sync::Arc;

use common::arrow::array::RecordBatch;
use common::arrow::datatypes::{DataType, Field, Schema};
use common::arrow::error::ArrowError;
use common::arrow_helpers::ScalarToArray as _;
use common::{
    Bytes, Bytes32, EvmAddress as Address, EvmCurrency, Table, TimestampSecond, BYTES32_TYPE,
    EVM_ADDRESS_TYPE as ADDRESS_TYPE, EVM_CURRENCY_TYPE,
};

pub fn table() -> Table {
    Table {
        name: TABLE_NAME.to_string(),
        schema: schema(),
    }
}

pub const TABLE_NAME: &'static str = "transactions";

#[derive(Debug, Default)]
pub struct Transaction {
    pub(crate) block_num: u64,
    pub(crate) timestamp: TimestampSecond,
    pub(crate) tx_index: u32,
    pub(crate) tx_hash: Bytes32,

    pub(crate) to: Bytes,
    pub(crate) nonce: u64,

    // Unsure why this is optional, firehose doesn't document that.
    pub(crate) gas_price: Option<EvmCurrency>,
    pub(crate) gas_limit: u64,

    // Value is the amount of Ether transferred as part of this transaction.
    pub(crate) value: Option<EvmCurrency>,

    // Input data the transaction will receive for EVM execution.
    pub(crate) input: Bytes,

    // Elliptic curve parameters.
    pub(crate) v: Bytes,
    pub(crate) r: Bytes,
    pub(crate) s: Bytes,

    // GasUsed is the total amount of gas unit used for the whole execution of the transaction.
    pub(crate) gas_used: u64,

    pub(crate) r#type: i32,
    pub(crate) max_fee_per_gas: Option<EvmCurrency>,
    pub(crate) max_priority_fee_per_gas: Option<EvmCurrency>,
    pub(crate) from: Address,
    pub(crate) return_data: Bytes,
    pub(crate) public_key: Bytes,
    pub(crate) begin_ordinal: u64,
    pub(crate) end_ordinal: u64,
}

impl Transaction {
    pub fn to_arrow(&self) -> Result<RecordBatch, ArrowError> {
        let Transaction {
            block_num,
            timestamp,
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
        } = self;

        let columns = vec![
            block_num.to_arrow()?,
            timestamp.to_arrow()?,
            tx_index.to_arrow()?,
            tx_hash.to_arrow()?,
            to.to_arrow()?,
            nonce.to_arrow()?,
            gas_price.to_arrow()?,
            gas_limit.to_arrow()?,
            value.to_arrow()?,
            input.to_arrow()?,
            v.to_arrow()?,
            r.to_arrow()?,
            s.to_arrow()?,
            gas_used.to_arrow()?,
            r#type.to_arrow()?,
            max_fee_per_gas.to_arrow()?,
            max_priority_fee_per_gas.to_arrow()?,
            from.to_arrow()?,
            return_data.to_arrow()?,
            public_key.to_arrow()?,
            begin_ordinal.to_arrow()?,
            end_ordinal.to_arrow()?,
        ];

        RecordBatch::try_new(Arc::new(schema()), columns)
    }
}

pub fn schema() -> Schema {
    let block_num = Field::new("block_num", DataType::UInt64, false);
    let timestamp = Field::new("timestamp", common::timestamp_type(), false);
    let tx_index = Field::new("tx_index", DataType::UInt32, false);
    let tx_hash = Field::new("tx_hash", BYTES32_TYPE, false);
    let to = Field::new("to", DataType::Binary, false);
    let nonce = Field::new("nonce", DataType::UInt64, false);
    let gas_price = Field::new("gas_price", EVM_CURRENCY_TYPE, true);
    let gas_limit = Field::new("gas_limit", DataType::UInt64, false);
    let value = Field::new("value", EVM_CURRENCY_TYPE, true);
    let input = Field::new("input", DataType::Binary, false);
    let v = Field::new("v", DataType::Binary, false);
    let r = Field::new("r", DataType::Binary, false);
    let s = Field::new("s", DataType::Binary, false);
    let gas_used = Field::new("gas_used", DataType::UInt64, false);
    let r#type = Field::new("type", DataType::Int32, false);
    let max_fee_per_gas = Field::new("max_fee_per_gas", EVM_CURRENCY_TYPE, true);
    let max_priority_fee_per_gas = Field::new("max_priority_fee_per_gas", EVM_CURRENCY_TYPE, true);
    let from = Field::new("from", ADDRESS_TYPE, false);
    let return_data = Field::new("return_data", DataType::Binary, false);
    let public_key = Field::new("public_key", DataType::Binary, false);
    let begin_ordinal = Field::new("begin_ordinal", DataType::UInt64, false);
    let end_ordinal = Field::new("end_ordinal", DataType::UInt64, false);

    let fields = vec![
        block_num,
        timestamp,
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

#[test]
fn default_to_arrow() {
    let tx = Transaction::default();
    let batch = tx.to_arrow().unwrap();
    assert_eq!(batch.num_columns(), 22);
    assert_eq!(batch.num_rows(), 1);
}
