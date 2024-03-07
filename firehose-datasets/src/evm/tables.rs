use serde::Serialize;
use types::{Bytes, Bytes32, EvmAddress as Address, EvmCurrency};

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

#[derive(Debug, Serialize)]
pub struct Log {
    pub block_num: u64,
    pub tx_index: u32,
    pub call_index: u32,
    pub tx_hash: Bytes32,

    pub address: Address,
    pub topics: Vec<Bytes32>,
    pub data: Bytes,

    // Index of the log relative to the transaction.
    pub index: u32,

    // Index of the log relative to the block, 0 if the state was reverted.
    pub block_index: u32,

    // Unique identifier for the log's position in the blockchain.
    pub ordinal: u64,
}
