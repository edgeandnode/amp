use common::arrow::datatypes::{DataType, Field, Schema};
use common::{
    Bytes, Bytes32, EvmAddress as Address, EvmCurrency, Table, BYTES32_TYPE,
    EVM_ADDRESS_TYPE as ADDRESS_TYPE, EVM_CURRENCY_TYPE,
};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct BlockHeader {
    pub number: u64,
    pub hash: Bytes32,

    pub parent_hash: Bytes32,
    pub uncle_hash: Bytes32,
    pub coinbase: Address,
    pub state_root: Bytes32,
    pub transactions_root: Bytes32,
    pub receipt_root: Bytes32,
    pub logs_bloom: Bytes,
    pub difficulty: Bytes,
    pub gas_limit: u64,
    pub gas_used: u64,
    pub timestamp: u64,
    pub extra_data: Bytes,
    pub mix_hash: Bytes32,
    pub nonce: u64,
    pub base_fee_per_gas: Option<u64>,
}

impl Table for BlockHeader {
    fn schema() -> Schema {
        let number = Field::new("number", DataType::UInt64, false);
        let hash = Field::new("hash", BYTES32_TYPE, false);
        let parent_hash = Field::new("parent_hash", BYTES32_TYPE, false);
        let uncle_hash = Field::new("uncle_hash", BYTES32_TYPE, false);
        let coinbase = Field::new("coinbase", ADDRESS_TYPE, false);
        let state_root = Field::new("state_root", BYTES32_TYPE, false);
        let transactions_root = Field::new("transactions_root", BYTES32_TYPE, false);
        let receipt_root = Field::new("receipt_root", BYTES32_TYPE, false);
        let logs_bloom = Field::new("logs_bloom", DataType::Binary, false);
        let difficulty = Field::new("difficulty", DataType::Binary, false);
        let gas_limit = Field::new("gas_limit", DataType::UInt64, false);
        let gas_used = Field::new("gas_used", DataType::UInt64, false);
        let timestamp = Field::new("timestamp", DataType::UInt64, false);
        let extra_data = Field::new("extra_data", DataType::Binary, false);
        let mix_hash = Field::new("mix_hash", BYTES32_TYPE, false);
        let nonce = Field::new("nonce", DataType::UInt64, false);
        let base_fee_per_gas = Field::new("base_fee_per_gas", DataType::UInt64, true);

        let fields = vec![
            number,
            hash,
            parent_hash,
            uncle_hash,
            coinbase,
            state_root,
            transactions_root,
            receipt_root,
            logs_bloom,
            difficulty,
            gas_limit,
            gas_used,
            timestamp,
            extra_data,
            mix_hash,
            nonce,
            base_fee_per_gas,
        ];

        Schema::new(fields)
    }
}

#[derive(Debug, Serialize)]
pub struct TransactionTrace {
    pub block_num: u64,
    pub tx_index: u32,
    pub tx_hash: Bytes32,

    pub to: Bytes,
    pub nonce: u64,
    pub gas_price: EvmCurrency,
    pub gas_limit: u64,

    // Value is the amount of Ether transferred as part of this transaction.
    pub value: EvmCurrency,

    // Input data the transaction will receive for EVM execution.
    pub input: Bytes,

    // Elliptic curve parameters.
    pub v: Bytes,
    pub r: Bytes,
    pub s: Bytes,

    // GasUsed is the total amount of gas unit used for the whole execution of the transaction.
    pub gas_used: u64,

    pub r#type: i32,
    pub max_fee_per_gas: EvmCurrency,
    pub max_priority_fee_per_gas: Option<EvmCurrency>,
    pub from: Address,
    pub return_data: Bytes,
    pub public_key: Bytes,
    pub begin_ordinal: u64,
    pub end_ordinal: u64,
}

impl Table for TransactionTrace {
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
        let max_priority_fee_per_gas =
            Field::new("max_priority_fee_per_gas", EVM_CURRENCY_TYPE, true);
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
}

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
