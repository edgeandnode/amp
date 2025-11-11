use std::time::Duration;

use common::{
    BoxError, Bytes32, EvmCurrency, RawDatasetRows, Timestamp,
    evm::tables::{
        blocks::{Block, BlockRowsBuilder},
        logs::{Log, LogRowsBuilder},
    },
    metadata::segments::BlockRange,
};
use thiserror::Error;

use super::{
    pbethereum,
    tables::{calls::Call, transactions::Transaction},
};
use crate::evm::tables::{calls::CallRowsBuilder, transactions::TransactionRowsBuilder};

#[derive(Error, Debug)]
pub enum ProtobufToRowError {
    #[error("malformed field {}, bytes: {}", .0, hex::encode(.1))]
    Malformed(&'static str, Vec<u8>),
    #[error("overflow in field {}, bytes: {}", .0, hex::encode(.1))]
    Overflow(&'static str, Vec<u8>),
    #[error("missing field: {0}")]
    Missing(&'static str),
    #[error("assertion failure: {0}")]
    AssertFail(BoxError),
    #[error("error serializing to arrow: {0}")]
    ArrowError(BoxError),
}

pub fn protobufs_to_rows(
    block: pbethereum::Block,
    network: &str,
) -> Result<RawDatasetRows, ProtobufToRowError> {
    use ProtobufToRowError::*;

    fn transactions_iter(
        block: &pbethereum::Block,
    ) -> impl Iterator<Item = &pbethereum::TransactionTrace> {
        block.transaction_traces.iter()
    }

    fn calls_iter(block: &pbethereum::Block) -> impl Iterator<Item = &pbethereum::Call> {
        block
            .transaction_traces
            .iter()
            .flat_map(|tx| tx.calls.iter())
    }

    fn logs_iter(block: &pbethereum::Block) -> impl Iterator<Item = &pbethereum::Log> {
        block
            .transaction_traces
            .iter()
            .flat_map(|tx| tx.calls.iter().flat_map(|call| call.logs.iter()))
    }

    let transaction_count = transactions_iter(&block).count();
    let total_input_size: usize = transactions_iter(&block).map(|tx| tx.input.len()).sum();
    let total_v_size: usize = transactions_iter(&block).map(|tx| tx.v.len()).sum();
    let total_r_size: usize = transactions_iter(&block).map(|tx| tx.r.len()).sum();
    let total_s_size: usize = transactions_iter(&block).map(|tx| tx.s.len()).sum();
    let total_return_data_size: usize = transactions_iter(&block)
        .map(|tx| tx.return_data.len())
        .sum();
    let total_public_key_size: usize = transactions_iter(&block)
        .map(|tx| tx.public_key.len())
        .sum();
    let mut transactions: TransactionRowsBuilder = TransactionRowsBuilder::with_capacity(
        transaction_count,
        total_input_size,
        total_v_size,
        total_r_size,
        total_s_size,
        total_return_data_size,
        total_public_key_size,
    );

    let call_count = calls_iter(&block).count();
    let total_return_data_size: usize = calls_iter(&block).map(|call| call.return_data.len()).sum();
    let total_input_size: usize = calls_iter(&block).map(|call| call.input.len()).sum();
    let mut calls: CallRowsBuilder =
        CallRowsBuilder::with_capacity(call_count, total_return_data_size, total_input_size);

    let logs_count = logs_iter(&block).count();
    let total_data_size = logs_iter(&block).map(|log| log.data.len()).sum();

    let mut logs: LogRowsBuilder = LogRowsBuilder::with_capacity(logs_count, total_data_size);

    let header = header_from_pb(block.header.ok_or(Missing("header"))?)?;

    for tx in block.transaction_traces {
        // Skip failed or reverted transactions.
        if tx.status != 1 {
            continue;
        }

        let tx_index = tx.index;
        let tx_hash: Bytes32 = tx.hash.try_into().map_err(|b| Malformed("tx.hash", b))?;
        let tx_trace_row = Transaction {
            block_hash: header.hash,
            block_num: header.block_num,
            timestamp: header.timestamp,
            tx_index,
            tx_hash,

            to: if tx.to.is_empty() {
                None
            } else {
                Some(tx.to.try_into().map_err(|b| Malformed("tx.to", b))?)
            },
            nonce: tx.nonce,

            gas_limit: tx.gas_limit,
            value: tx
                .value
                .map(|b| non_negative_pb_bigint_to_evm_currency("tx.value", b))
                .transpose()?,
            input: tx.input,

            v: tx.v,
            r: tx.r,
            s: tx.s,
            receipt_cumulative_gas_used: tx.gas_used,

            r#type: tx.r#type,
            from: tx.from.try_into().map_err(|b| Malformed("tx.from", b))?,
            status: tx.status,
            return_data: tx.return_data,
            public_key: tx.public_key,
            begin_ordinal: tx.begin_ordinal,
            end_ordinal: tx.end_ordinal,

            // We've seen nonsensically high values in the wild, probably due to bugs in the chain.
            // Example: Arbitrum One block 16464264.
            gas_price: tx
                .gas_price
                .and_then(|b| non_negative_pb_bigint_to_evm_currency("tx.gas_price", b).ok()),

            // Gas fee params are `null` on overflow. By EIP-1559 semantics, these can be huge but
            // never actually be paid, allowing for nonsense values that overflow a Decimal128.
            //
            // For an example, see block `317116119` on Arbitrum One.
            max_fee_per_gas: tx
                .max_fee_per_gas
                .and_then(|b| non_negative_pb_bigint_to_evm_currency("tx.max_fee_per_gas", b).ok()),
            max_priority_fee_per_gas: tx.max_priority_fee_per_gas.and_then(|b| {
                non_negative_pb_bigint_to_evm_currency("tx.max_priority_fee_per_gas", b).ok()
            }),
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
                tx_hash,
                index: call_index,
                parent_index: call.parent_index,
                depth: call.depth,
                call_type: call.call_type,
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
                return_data: call.return_data,
                input: call.input,
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
                            return Err(AssertFail("log has more than four topics".into()));
                        }
                    }
                }

                let log = Log {
                    block_hash: header.hash,
                    block_num: header.block_num,
                    timestamp: header.timestamp,
                    tx_index,
                    tx_hash,
                    address: log
                        .address
                        .try_into()
                        .map_err(|b| Malformed("log.address", b))?,
                    topic0,
                    topic1,
                    topic2,
                    topic3,
                    data: log.data,
                    log_index: log.block_index,
                };
                logs.append(&log);
            }
        }
    }

    let block = BlockRange {
        numbers: header.block_num..=header.block_num,
        network: network.to_string(),
        hash: header.hash.into(),
        prev_hash: if header.block_num == 0 {
            None
        } else {
            Some(header.parent_hash.into())
        },
    };
    let header_row = {
        let mut builder = BlockRowsBuilder::with_capacity_for(&header);
        builder.append(&header);
        builder.build(block.clone()).map_err(ArrowError)?
    };
    let transactions_rows = transactions.build(block.clone()).map_err(ArrowError)?;
    let calls_rows = calls.build(block.clone()).map_err(ArrowError)?;
    let logs_rows = logs.build(block.clone()).map_err(ArrowError)?;

    Ok(RawDatasetRows::new(vec![
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
        logs_bloom: header.logs_bloom,
        difficulty: header
            .difficulty
            .ok_or(Missing("difficulty"))
            .and_then(|b| non_negative_pb_bigint_to_evm_currency("difficulty", b))?,
        total_difficulty: header
            .total_difficulty
            .map(|b| non_negative_pb_bigint_to_evm_currency("total_difficulty", b))
            .transpose()?,
        gas_limit: header.gas_limit,
        gas_used: header.gas_used,
        extra_data: header.extra_data,
        mix_hash: header
            .mix_hash
            .try_into()
            .map_err(|b| Malformed("mix_hash", b))?,
        nonce: header.nonce,
        base_fee_per_gas: base_fee_per_gas.map(|b| b as i128),
        withdrawals_root: match header.withdrawals_root.is_empty() {
            true => None,
            false => Some(
                header
                    .withdrawals_root
                    .try_into()
                    .map_err(|b| Malformed("withdrawals_root", b))?,
            ),
        },
        blob_gas_used: header.blob_gas_used,
        excess_blob_gas: header.excess_blob_gas,
        parent_beacon_root: match header.parent_beacon_root.is_empty() {
            true => None,
            false => Some(
                header
                    .parent_beacon_root
                    .try_into()
                    .map_err(|b| Malformed("parent_beacon_root", b))?,
            ),
        },
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
