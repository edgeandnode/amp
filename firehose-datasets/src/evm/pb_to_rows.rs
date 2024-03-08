use super::pbethereum;
use super::tables::*;
use anyhow::anyhow;
use common::{Bytes32, EvmCurrency};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProtobufToRowError {
    #[error("Malformed bytes: {}", hex::encode(.0))]
    Malformed(Vec<u8>),
    #[error("Missing field: {0}")]
    Missing(&'static str),
    #[error("Assertion failure: {0}")]
    AssertFail(anyhow::Error),
}

pub fn protobufs_to_rows(
    block: pbethereum::Block,
) -> Result<(BlockHeader, Vec<TransactionTrace>, Vec<Call>, Vec<Log>), ProtobufToRowError> {
    use ProtobufToRowError::*;

    let mut transactions: Vec<TransactionTrace> = vec![];
    let mut calls: Vec<Call> = vec![];
    let mut logs: Vec<Log> = vec![];

    let header = header_from_pb(block.header.ok_or(Missing("header"))?)?;

    for tx in block.transaction_traces {
        let tx_index = tx.index;
        let tx_hash: Bytes32 = tx.hash.try_into().map_err(Malformed)?;
        let tx_trace_row = TransactionTrace {
            block_num: header.number,
            tx_index,
            tx_hash: tx_hash.clone(),

            to: tx.to.into(),
            nonce: tx.nonce,
            gas_price: non_negative_pb_bigint_to_evm_currency(
                tx.gas_price.ok_or(Missing("gas_price"))?,
            )?,
            gas_limit: tx.gas_limit,
            value: non_negative_pb_bigint_to_evm_currency(tx.value.ok_or(Missing("value"))?)?,
            input: tx.input.into(),

            v: tx.v.into(),
            r: tx.r.into(),
            s: tx.s.into(),
            gas_used: tx.gas_used,

            r#type: tx.r#type,
            max_fee_per_gas: non_negative_pb_bigint_to_evm_currency(
                tx.max_fee_per_gas.ok_or(Missing("max_fee_per_gas"))?,
            )?,
            max_priority_fee_per_gas: tx
                .max_priority_fee_per_gas
                .map(non_negative_pb_bigint_to_evm_currency)
                .transpose()?,
            from: tx.from.try_into().map_err(Malformed)?,
            return_data: tx.return_data.into(),
            public_key: tx.public_key.into(),
            begin_ordinal: tx.begin_ordinal,
            end_ordinal: tx.end_ordinal,
        };
        transactions.push(tx_trace_row);

        // Also push the calls associated with each trace.
        for call in tx.calls {
            // Skip failed calls.
            if call.state_reverted {
                continue;
            }

            let call_index = call.index;
            let call_row = Call {
                block_num: header.number,
                tx_index,
                tx_hash: tx_hash.clone(),
                index: call_index,
                parent_index: call.parent_index,
                depth: call.depth,
                caller: call.caller.try_into().map_err(Malformed)?,
                address: call.address.try_into().map_err(Malformed)?,
                value: call
                    .value
                    .map(non_negative_pb_bigint_to_evm_currency)
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
            calls.push(call_row);

            // And the logs associated with each call.
            for log in call.logs {
                let mut topic0 = None;
                let mut topic1 = None;
                let mut topic2 = None;
                let mut topic3 = None;

                for t in log.topics {
                    let topic = Bytes32::try_from(t).map_err(Malformed)?;
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
                    block_num: header.number,
                    tx_index,
                    call_index,
                    tx_hash: tx_hash.clone(),
                    address: log.address.try_into().map_err(Malformed)?,
                    topic0,
                    topic1,
                    topic2,
                    topic3,
                    data: log.data.into(),
                    index: log.index,
                    block_index: log.block_index,
                    ordinal: log.ordinal,
                };
                logs.push(log)
            }
        }
    }

    Ok((header, transactions, calls, logs))
}

fn header_from_pb(header: pbethereum::BlockHeader) -> Result<BlockHeader, ProtobufToRowError> {
    use ProtobufToRowError::*;

    let timestamp = header.timestamp.ok_or(Missing("timestamp"))?.seconds as u64;

    // `base_fee_per_gas` is annoying. It is optional, it is represented as a byte array in the
    // protobuf, and it can actually just fit in a u64.
    let base_fee_per_gas = header.base_fee_per_gas.map(pb_bigint_to_u64).transpose()?;

    let header = BlockHeader {
        hash: header.hash.try_into().map_err(Malformed)?,
        parent_hash: header.parent_hash.try_into().map_err(Malformed)?,
        uncle_hash: header.uncle_hash.try_into().map_err(Malformed)?,
        coinbase: header.coinbase.try_into().map_err(Malformed)?,
        state_root: header.state_root.try_into().map_err(Malformed)?,
        transactions_root: header.transactions_root.try_into().map_err(Malformed)?,
        receipt_root: header.receipt_root.try_into().map_err(Malformed)?,
        logs_bloom: header.logs_bloom.into(),
        difficulty: header.difficulty.ok_or(Missing("difficulty"))?.bytes.into(),
        number: header.number,
        gas_limit: header.gas_limit,
        gas_used: header.gas_used,
        timestamp: timestamp,
        extra_data: header.extra_data.into(),
        mix_hash: header.mix_hash.try_into().map_err(Malformed)?,
        nonce: header.nonce,
        base_fee_per_gas,
    };

    Ok(header)
}

// Assume `bytes` is big-endian number that will fit into a u64.
fn pb_bigint_to_u64(bytes: pbethereum::BigInt) -> Result<u64, ProtobufToRowError> {
    use ProtobufToRowError::*;
    let len = bytes.bytes.len();
    let slice = bytes
        .bytes
        .get((len - 8)..)
        .ok_or_else(|| Malformed(bytes.bytes.clone()))?;
    let bytes8: [u8; 8] = slice
        .try_into()
        .map_err(|_| Malformed(bytes.bytes.clone()))?;
    Ok(u64::from_be_bytes(bytes8))
}

// Assume that `bytes` is a non-negative big-endian number that will fit into an i128.
fn non_negative_pb_bigint_to_evm_currency(
    bytes: pbethereum::BigInt,
) -> Result<EvmCurrency, ProtobufToRowError> {
    use ProtobufToRowError::*;
    let len = bytes.bytes.len();
    let slice = bytes
        .bytes
        .get((len - 16)..)
        .ok_or_else(|| Malformed(bytes.bytes.clone()))?;
    let bytes16: [u8; 16] = slice
        .try_into()
        .map_err(|_| Malformed(bytes.bytes.clone()))?;
    let unsigned = u128::from_be_bytes(bytes16);
    Ok(unsigned as i128)
}
