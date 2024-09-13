use std::sync::{Arc, LazyLock};

use common::arrow::array::{ArrayRef, BinaryBuilder, Int32Builder, UInt32Builder, UInt64Builder};
use common::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use common::arrow::error::ArrowError;
use common::{
    Bytes32, Bytes32ArrayBuilder, EvmAddress as Address, EvmAddressArrayBuilder, EvmCurrency,
    EvmCurrencyArrayBuilder, Table, TableRows, Timestamp, TimestampArrayBuilder, BLOCK_NUM,
    BYTES32_TYPE, EVM_ADDRESS_TYPE as ADDRESS_TYPE, EVM_CURRENCY_TYPE,
};

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(schema()));

pub fn table(network: String) -> Table {
    Table {
        name: TABLE_NAME.to_string(),
        schema: SCHEMA.clone(),
        network,
    }
}

pub const TABLE_NAME: &'static str = "transactions";

/// Prefer using the pre-computed SCHEMA
fn schema() -> Schema {
    let block_hash = Field::new("block_hash", BYTES32_TYPE, false);
    let block_num = Field::new(BLOCK_NUM, DataType::UInt64, false);
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
        block_hash,
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

#[derive(Debug, Default)]
pub(crate) struct Transaction {
    pub(crate) block_hash: Bytes32,
    pub(crate) block_num: u64,
    pub(crate) timestamp: Timestamp,
    pub(crate) tx_index: u32,
    pub(crate) tx_hash: Bytes32,

    pub(crate) to: Vec<u8>,
    pub(crate) nonce: u64,

    // Unsure why this is optional, firehose doesn't document that.
    pub(crate) gas_price: Option<EvmCurrency>,
    pub(crate) gas_limit: u64,

    // Value is the amount of Ether transferred as part of this transaction.
    pub(crate) value: Option<EvmCurrency>,

    // Input data the transaction will receive for EVM execution.
    pub(crate) input: Vec<u8>,

    // Elliptic curve parameters.
    pub(crate) v: Vec<u8>,
    pub(crate) r: Vec<u8>,
    pub(crate) s: Vec<u8>,

    // The total amount of gas unit used for the whole execution of the transaction.
    pub(crate) receipt_cumulative_gas_used: u64,

    pub(crate) r#type: i32,
    pub(crate) max_fee_per_gas: Option<EvmCurrency>,
    pub(crate) max_priority_fee_per_gas: Option<EvmCurrency>,
    pub(crate) from: Address,
    pub(crate) return_data: Vec<u8>,
    pub(crate) public_key: Vec<u8>,

    // Firehose specific.
    pub(crate) begin_ordinal: u64,
    pub(crate) end_ordinal: u64,
}

pub(crate) struct TransactionRowsBuilder {
    block_hash: Bytes32ArrayBuilder,
    block_num: UInt64Builder,
    timestamp: TimestampArrayBuilder,
    tx_index: UInt32Builder,
    tx_hash: Bytes32ArrayBuilder,
    to: BinaryBuilder,
    nonce: UInt64Builder,
    gas_price: EvmCurrencyArrayBuilder,
    gas_limit: UInt64Builder,
    value: EvmCurrencyArrayBuilder,
    input: BinaryBuilder,
    v: BinaryBuilder,
    r: BinaryBuilder,
    s: BinaryBuilder,
    gas_used: UInt64Builder,
    r#type: Int32Builder,
    max_fee_per_gas: EvmCurrencyArrayBuilder,
    max_priority_fee_per_gas: EvmCurrencyArrayBuilder,
    from: EvmAddressArrayBuilder,
    return_data: BinaryBuilder,
    public_key: BinaryBuilder,
    begin_ordinal: UInt64Builder,
    end_ordinal: UInt64Builder,
}

impl TransactionRowsBuilder {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            block_hash: Bytes32ArrayBuilder::with_capacity(capacity),
            block_num: UInt64Builder::with_capacity(capacity),
            timestamp: TimestampArrayBuilder::with_capacity(capacity),
            tx_index: UInt32Builder::with_capacity(capacity),
            tx_hash: Bytes32ArrayBuilder::with_capacity(capacity),
            to: BinaryBuilder::with_capacity(capacity, 0),
            nonce: UInt64Builder::with_capacity(capacity),
            gas_price: EvmCurrencyArrayBuilder::with_capacity(capacity),
            gas_limit: UInt64Builder::with_capacity(capacity),
            value: EvmCurrencyArrayBuilder::with_capacity(capacity),
            input: BinaryBuilder::with_capacity(capacity, 0),
            v: BinaryBuilder::with_capacity(capacity, 0),
            r: BinaryBuilder::with_capacity(capacity, 0),
            s: BinaryBuilder::with_capacity(capacity, 0),
            gas_used: UInt64Builder::with_capacity(capacity),
            r#type: Int32Builder::with_capacity(capacity),
            max_fee_per_gas: EvmCurrencyArrayBuilder::with_capacity(capacity),
            max_priority_fee_per_gas: EvmCurrencyArrayBuilder::with_capacity(capacity),
            from: EvmAddressArrayBuilder::with_capacity(capacity),
            return_data: BinaryBuilder::with_capacity(capacity, 0),
            public_key: BinaryBuilder::with_capacity(capacity, 0),
            begin_ordinal: UInt64Builder::with_capacity(capacity),
            end_ordinal: UInt64Builder::with_capacity(capacity),
        }
    }

    pub(crate) fn append(&mut self, tx: &Transaction) {
        let Transaction {
            block_hash,
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
            receipt_cumulative_gas_used,
            r#type,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            from,
            return_data,
            public_key,
            begin_ordinal,
            end_ordinal,
        } = tx;

        self.block_hash.append_value(*block_hash);
        self.block_num.append_value(*block_num);
        self.timestamp.append_value(*timestamp);
        self.tx_index.append_value(*tx_index);
        self.tx_hash.append_value(*tx_hash);
        self.to.append_value(to);
        self.nonce.append_value(*nonce);
        self.gas_price.append_option(*gas_price);
        self.gas_limit.append_value(*gas_limit);
        self.value.append_option(*value);
        self.input.append_value(input);
        self.v.append_value(v);
        self.r.append_value(r);
        self.s.append_value(s);
        self.gas_used.append_value(*receipt_cumulative_gas_used);
        self.r#type.append_value(*r#type);
        self.max_fee_per_gas.append_option(*max_fee_per_gas);
        self.max_priority_fee_per_gas
            .append_option(*max_priority_fee_per_gas);
        self.from.append_value(*from);
        self.return_data.append_value(return_data);
        self.public_key.append_value(public_key);
        self.begin_ordinal.append_value(*begin_ordinal);
        self.end_ordinal.append_value(*end_ordinal);
    }

    pub(crate) fn build(self, network: String) -> Result<TableRows, ArrowError> {
        let Self {
            block_hash,
            mut block_num,
            mut timestamp,
            mut tx_index,
            tx_hash,
            mut to,
            mut nonce,
            gas_price,
            mut gas_limit,
            value,
            mut input,
            mut v,
            mut r,
            mut s,
            mut gas_used,
            mut r#type,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            from,
            mut return_data,
            mut public_key,
            mut begin_ordinal,
            mut end_ordinal,
        } = self;

        let columns = vec![
            Arc::new(block_hash.finish()) as ArrayRef,
            Arc::new(block_num.finish()),
            Arc::new(timestamp.finish()),
            Arc::new(tx_index.finish()),
            Arc::new(tx_hash.finish()),
            Arc::new(to.finish()),
            Arc::new(nonce.finish()),
            Arc::new(gas_price.finish()),
            Arc::new(gas_limit.finish()),
            Arc::new(value.finish()),
            Arc::new(input.finish()),
            Arc::new(v.finish()),
            Arc::new(r.finish()),
            Arc::new(s.finish()),
            Arc::new(gas_used.finish()),
            Arc::new(r#type.finish()),
            Arc::new(max_fee_per_gas.finish()),
            Arc::new(max_priority_fee_per_gas.finish()),
            Arc::new(from.finish()),
            Arc::new(return_data.finish()),
            Arc::new(public_key.finish()),
            Arc::new(begin_ordinal.finish()),
            Arc::new(end_ordinal.finish()),
        ];

        TableRows::new(table(network), columns)
    }
}

#[test]
fn default_to_arrow() {
    let tx = Transaction::default();
    let rows = {
        let mut builder = TransactionRowsBuilder::with_capacity(1);
        builder.append(&tx);
        builder.build("test_network".to_string()).unwrap()
    };
    assert_eq!(rows.rows.num_columns(), 23);
    assert_eq!(rows.rows.num_rows(), 1);
}
