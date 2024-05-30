use std::time::Duration;

use super::tables::calls::Call;
use super::{pbethereum, tables::transactions::Transaction};
use crate::evm::tables::calls::CallRowsBuilder;
use crate::evm::tables::transactions::TransactionRowsBuilder;
use anyhow::anyhow;
use common::arrow::error::ArrowError;
use common::evm::tables::blocks::Block;
use common::evm::tables::blocks::BlockRowsBuilder;
use common::evm::tables::logs::Log;
use common::evm::tables::logs::LogRowsBuilder;
use common::{Bytes32, DatasetRows, EvmCurrency, Timestamp};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProtobufToRowError {
    #[error("malformed field {0}, bytes: {}", hex::encode(.1))]
    Malformed(&'static str, Vec<u8>),
    #[error("overflow in field {0}, bytes: {}", hex::encode(.1))]
    Overflow(&'static str, Vec<u8>),
    #[error("missing field: {0}")]
    Missing(&'static str),
    #[error("assertion failure: {0}")]
    AssertFail(anyhow::Error),
    #[error("error serializing to arrow: {0}")]
    ArrowError(#[from] ArrowError),
}

pub fn protobufs_to_rows(block: pbethereum::Block) -> Result<DatasetRows, ProtobufToRowError> {
    use ProtobufToRowError::*;

    let transaction_count = block.transaction_traces.len();
    let mut transactions: TransactionRowsBuilder =
        TransactionRowsBuilder::with_capacity(transaction_count);

    let call_count = block
        .transaction_traces
        .iter()
        .map(|tx| tx.calls.len())
        .sum();
    let mut calls: CallRowsBuilder = CallRowsBuilder::with_capacity(call_count);

    let log_count = block
        .transaction_traces
        .iter()
        .map(|tx| tx.calls.iter().map(|call| call.logs.len()).sum::<usize>())
        .sum();
    let mut logs: LogRowsBuilder = LogRowsBuilder::with_capacity(log_count);

    let header = header_from_pb(block.header.ok_or(Missing("header"))?)?;

    for tx in block.transaction_traces {
        let tx_index = tx.index;
        let tx_hash: Bytes32 = tx.hash.try_into().map_err(|b| Malformed("tx.hash", b))?;
        let tx_trace_row = Transaction {
            block_hash: header.hash,
            block_num: header.block_num,
            timestamp: header.timestamp,
            tx_index,
            tx_hash: tx_hash.clone(),

            to: tx.to.into(),
            nonce: tx.nonce,
            gas_price: tx.gas_price.and_then(|b| {
                // Null `gas_price` on overflow
                //
                // Unfourtnately, we have seen overflows values in the wild. Case in point:
                // Arbitrum block 16464264 has one tx, with a nonsensically high gas price in
                // both Firehose and JSON-RPC. Since the value is nonsense but the tx seems
                // otherwise valid, 'NULL' seems like the best option. If more cases appear, we
                // may revisit this behaviour.
                match non_negative_pb_bigint_to_evm_currency("tx.gas_price", b) {
                    Ok(v) => Some(v),
                    Err(Overflow(_, _)) => None,
                    Err(e) => unreachable!("expected ok or overflow, got: {}", e),
                }
            }),
            gas_limit: tx.gas_limit,
            value: tx
                .value
                .map(|b| non_negative_pb_bigint_to_evm_currency("tx.value", b))
                .transpose()?,
            input: tx.input.into(),

            v: tx.v.into(),
            r: tx.r.into(),
            s: tx.s.into(),
            receipt_cumulative_gas_used: tx.gas_used,

            r#type: tx.r#type,
            max_fee_per_gas: tx
                .max_fee_per_gas
                .map(|b| non_negative_pb_bigint_to_evm_currency("tx.max_fee_per_gas", b))
                .transpose()?,
            max_priority_fee_per_gas: tx
                .max_priority_fee_per_gas
                .map(|b| non_negative_pb_bigint_to_evm_currency("tx.max_priority_fee_per_gas", b))
                .transpose()?,
            from: tx.from.try_into().map_err(|b| Malformed("tx.from", b))?,
            return_data: tx.return_data.into(),
            public_key: tx.public_key.into(),
            begin_ordinal: tx.begin_ordinal,
            end_ordinal: tx.end_ordinal,
        };
        transactions.append(&tx_trace_row);

        // Also push the calls associated with each trace.
        for call in tx.calls {
            // Skip failed calls.
            if call.state_reverted {
                continue;
            }

            let call_index = call.index;
            let call_row = Call {
                block_hash: header.hash,
                block_num: header.block_num,
                timestamp: header.timestamp,
                tx_index,
                tx_hash: tx_hash.clone(),
                index: call_index,
                parent_index: call.parent_index,
                depth: call.depth,
                caller: call
                    .caller
                    .try_into()
                    .map_err(|b| Malformed("call.caller", b))?,
                address: call
                    .address
                    .try_into()
                    .map_err(|b| Malformed("call.address", b))?,
                value: call
                    .value
                    .map(|b| non_negative_pb_bigint_to_evm_currency("call.value", b))
                    .transpose()?,
                gas_limit: call.gas_limit,
                gas_consumed: call.gas_consumed,
                return_data: call.return_data.into(),
                input: call.input.into(),
                executed_code: call.executed_code,
                selfdestruct: call.suicide,
                begin_ordinal: call.begin_ordinal,
                end_ordinal: call.end_ordinal,
            };
            calls.append(&call_row);

            // And the logs associated with each call.
            for log in call.logs {
                let mut topic0 = None;
                let mut topic1 = None;
                let mut topic2 = None;
                let mut topic3 = None;

                for t in log.topics {
                    let topic = Bytes32::try_from(t).map_err(|b| Malformed("log.topicN", b))?;
                    match (topic0, topic1, topic2, topic3) {
                        (None, _, _, _) => topic0 = Some(topic),
                        (Some(_), None, _, _) => topic1 = Some(topic),
                        (Some(_), Some(_), None, _) => topic2 = Some(topic),
                        (Some(_), Some(_), Some(_), None) => topic3 = Some(topic),
                        (Some(_), Some(_), Some(_), Some(_)) => {
                            return Err(AssertFail(anyhow!("log has more than four topics")))
                        }
                    }
                }

                let log = Log {
                    block_hash: header.hash,
                    block_num: header.block_num,
                    timestamp: header.timestamp,
                    tx_index,
                    tx_hash: tx_hash.clone(),
                    address: log
                        .address
                        .try_into()
                        .map_err(|b| Malformed("log.address", b))?,
                    topic0,
                    topic1,
                    topic2,
                    topic3,
                    data: log.data.into(),
                    log_index: log.block_index,
                };
                logs.append(&log);
            }
        }
    }

    let header_row = {
        let mut builder = BlockRowsBuilder::with_capacity(1);
        builder.append(&header);
        builder.build()?
    };
    let transactions_rows = transactions.build()?;
    let calls_rows = calls.build()?;
    let logs_rows = logs.build()?;

    Ok(DatasetRows(vec![
        header_row,
        transactions_rows,
        calls_rows,
        logs_rows,
    ]))
}

fn header_from_pb(header: pbethereum::BlockHeader) -> Result<Block, ProtobufToRowError> {
    use ProtobufToRowError::*;

    let timestamp = header.timestamp.ok_or(Missing("timestamp"))?.seconds as u64;

    // `base_fee_per_gas` is annoying. It is optional, it is represented as a byte array in the
    // protobuf, and it can actually just fit in a u64.
    let base_fee_per_gas = header
        .base_fee_per_gas
        .map(|b| pb_bigint_to_u64("base_fee_per_gas", b))
        .transpose()?;

    let header = Block {
        block_num: header.number,
        timestamp: Timestamp(Duration::from_secs(timestamp)),
        hash: header.hash.try_into().map_err(|b| Malformed("hash", b))?,
        parent_hash: header
            .parent_hash
            .try_into()
            .map_err(|b| Malformed("parent_hash", b))?,
        ommers_hash: header
            .uncle_hash
            .try_into()
            .map_err(|b| Malformed("uncle_hash", b))?,
        miner: header
            .coinbase
            .try_into()
            .map_err(|b| Malformed("coinbase", b))?,
        state_root: header
            .state_root
            .try_into()
            .map_err(|b| Malformed("state_root", b))?,
        transactions_root: header
            .transactions_root
            .try_into()
            .map_err(|b| Malformed("transactions_root", b))?,
        receipt_root: header
            .receipt_root
            .try_into()
            .map_err(|b| Malformed("receipt_root", b))?,
        logs_bloom: header.logs_bloom.into(),
        difficulty: header
            .difficulty
            .ok_or(Missing("difficulty"))
            .and_then(|b| non_negative_pb_bigint_to_evm_currency("difficulty", b))?,
        gas_limit: header.gas_limit,
        gas_used: header.gas_used,
        extra_data: header.extra_data.into(),
        mix_hash: header
            .mix_hash
            .try_into()
            .map_err(|b| Malformed("mix_hash", b))?,
        nonce: header.nonce,
        base_fee_per_gas: base_fee_per_gas.map(|b| b as i128),
    };

    Ok(header)
}

// Assume `bytes` is big-endian number that will fit into a u64.
fn pb_bigint_to_u64(
    field: &'static str,
    bytes: pbethereum::BigInt,
) -> Result<u64, ProtobufToRowError> {
    use ProtobufToRowError::*;
    let len = bytes.bytes.len();

    if len > 8 {
        return Err(Malformed(field, bytes.bytes.clone()));
    }

    // If bytes has len < 8, pad with zeroes.
    let mut bytes8 = [0u8; 8];
    let start = 8 - len;
    bytes8[start..].copy_from_slice(&bytes.bytes);

    Ok(u64::from_be_bytes(bytes8))
}

// Assume that `bytes` is a non-negative big-endian number that will fit into an i128.
//
// Errors:
// - `ProtobufToRowError::Overflow` if `bytes` is too long to fit into an i128.
fn non_negative_pb_bigint_to_evm_currency(
    field: &'static str,
    bytes: pbethereum::BigInt,
) -> Result<EvmCurrency, ProtobufToRowError> {
    use ProtobufToRowError::*;
    let len = bytes.bytes.len();

    if len > 16 {
        return Err(Overflow(field, bytes.bytes.clone()));
    }

    // If bytes has len < 16, pad with zeroes.
    let mut bytes16 = [0u8; 16];
    let start = 16 - len;
    bytes16[start..].copy_from_slice(&bytes.bytes);

    let unsigned = u128::from_be_bytes(bytes16);
    Ok(unsigned as i128)
}
