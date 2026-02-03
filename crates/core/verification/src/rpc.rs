use alloy::primitives::{Address, B256, BlockNumber, Bytes, U8, U64, U256};
use anyhow::Context as _;
use reqwest::Url;
use serde::{Deserialize, Serialize};

use crate::client::{Block, Log, Transaction};

/// Fetch a block from an Ethereum JSON-RPC endpoint
///
/// Calls eth_getBlockByNumber and eth_getBlockReceipts to construct a complete Block.
pub async fn fetch_rpc_block(rpc_url: &Url, block_number: BlockNumber) -> anyhow::Result<Block> {
    let client = reqwest::Client::new();

    // Format block number as hex string with 0x prefix
    let block_number_hex = format!("0x{:x}", block_number);

    // Call eth_getBlockByNumber with full transactions (true)
    let rpc_block: RpcBlock = call_rpc(
        &client,
        rpc_url,
        "eth_getBlockByNumber",
        vec![
            serde_json::Value::String(block_number_hex.clone()),
            serde_json::Value::Bool(true),
        ],
    )
    .await
    .context("eth_getBlockByNumber")?;

    // Call eth_getBlockReceipts
    let rpc_receipts: Vec<RpcReceipt> = call_rpc(
        &client,
        rpc_url,
        "eth_getBlockReceipts",
        vec![serde_json::Value::String(block_number_hex)],
    )
    .await
    .context("eth_getBlockReceipts")?;

    // Convert RPC types to our internal types
    convert_rpc_block_to_block(rpc_block, rpc_receipts)
}

#[derive(Serialize)]
struct JsonRpcRequest {
    jsonrpc: String,
    id: u32,
    method: String,
    params: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
struct JsonRpcResponse<T> {
    #[serde(default)]
    result: Option<T>,
    #[serde(default)]
    error: Option<JsonRpcError>,
}

#[derive(Deserialize, Debug)]
struct JsonRpcError {
    code: i32,
    message: String,
}

#[derive(Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase")]
struct RpcBlock {
    number: U64,
    timestamp: U64,
    hash: B256,
    parent_hash: B256,
    sha3_uncles: B256,
    miner: Address,
    state_root: B256,
    transactions_root: B256,
    receipts_root: B256,
    logs_bloom: Bytes,
    difficulty: U256,
    #[serde(default)]
    total_difficulty: Option<U256>,
    gas_limit: U64,
    gas_used: U64,
    extra_data: Bytes,
    mix_hash: B256,
    nonce: U64,
    #[serde(default)]
    base_fee_per_gas: Option<U64>,
    #[serde(default)]
    withdrawals_root: Option<B256>,
    #[serde(default)]
    blob_gas_used: Option<U64>,
    #[serde(default)]
    excess_blob_gas: Option<U64>,
    #[serde(default)]
    parent_beacon_block_root: Option<B256>,
    #[serde(default)]
    requests_hash: Option<B256>,
    transactions: Vec<RpcTransaction>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct RpcTransaction {
    transaction_index: U64,
    hash: B256,
    #[serde(rename = "type")]
    tx_type: U8,
    nonce: U64,
    #[serde(default)]
    gas_price: Option<U256>,
    #[serde(default)]
    max_fee_per_gas: Option<U256>,
    #[serde(default)]
    max_priority_fee_per_gas: Option<U256>,
    #[serde(default)]
    max_fee_per_blob_gas: Option<U256>,
    gas: U64,
    #[serde(default)]
    to: Option<Address>,
    value: U256,
    input: Bytes,
    #[serde(deserialize_with = "deserialize_b256_lenient")]
    r: B256,
    #[serde(deserialize_with = "deserialize_b256_lenient")]
    s: B256,
    v: U64,
    #[serde(default)]
    y_parity: Option<U64>,
    #[serde(default)]
    chain_id: Option<U64>,
    from: Address,
    #[serde(default)]
    access_list: Option<Vec<RpcAccessListItem>>,
    #[serde(default, deserialize_with = "deserialize_b256_vec_lenient")]
    blob_versioned_hashes: Option<Vec<B256>>,
    #[serde(default)]
    authorization_list: Option<Vec<RpcAuthorizationItem>>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct RpcAccessListItem {
    address: Address,
    #[serde(deserialize_with = "deserialize_b256_vec_required")]
    storage_keys: Vec<B256>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct RpcAuthorizationItem {
    chain_id: U64,
    address: Address,
    nonce: U64,
    y_parity: U64,
    #[serde(deserialize_with = "deserialize_b256_lenient")]
    r: B256,
    #[serde(deserialize_with = "deserialize_b256_lenient")]
    s: B256,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct RpcReceipt {
    transaction_index: U64,
    cumulative_gas_used: U64,
    #[serde(default)]
    status: Option<U64>,
    #[serde(default)]
    root: Option<B256>,
    logs: Vec<RpcLog>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct RpcLog {
    log_index: U64,
    address: Address,
    #[serde(deserialize_with = "deserialize_b256_vec_required")]
    topics: Vec<B256>,
    data: Bytes,
}

async fn call_rpc<T: serde::de::DeserializeOwned + Default>(
    client: &reqwest::Client,
    rpc_url: &Url,
    method: &str,
    params: Vec<serde_json::Value>,
) -> anyhow::Result<T> {
    let request = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        id: 1,
        method: method.to_string(),
        params,
    };

    let response: JsonRpcResponse<T> = client
        .post(rpc_url.as_str())
        .json(&request)
        .send()
        .await
        .context(format!("failed to send {} request", method))?
        .json()
        .await
        .context(format!("failed to parse {} response", method))?;

    if let Some(error) = response.error {
        anyhow::bail!("RPC error: {} (code: {})", error.message, error.code);
    }

    response
        .result
        .ok_or_else(|| anyhow::anyhow!("RPC response missing result field"))
}

fn convert_rpc_block_to_block(
    rpc_block: RpcBlock,
    rpc_receipts: Vec<RpcReceipt>,
) -> anyhow::Result<Block> {
    let mut receipt_map = std::collections::HashMap::new();
    for receipt in &rpc_receipts {
        receipt_map.insert(receipt.transaction_index, receipt);
    }

    let mut transactions = Vec::new();
    for rpc_tx in &rpc_block.transactions {
        let receipt = receipt_map.get(&rpc_tx.transaction_index).ok_or_else(|| {
            anyhow::anyhow!("missing receipt for tx_index {}", rpc_tx.transaction_index)
        })?;

        let v = rpc_tx.v.to::<u64>();
        let v_parity = if let Some(y_parity) = rpc_tx.y_parity {
            y_parity.to::<u64>() != 0
        } else {
            match v {
                27 | 28 => v == 28, // Pre-EIP-155: 27=false, 28=true
                _ => {
                    // EIP-155: v = chainId * 2 + 35 + {0, 1}
                    // To extract parity: (v - 35) % 2
                    ((v.saturating_sub(35)) & 1) == 1
                }
            }
        };

        let transaction = Transaction {
            block_number: rpc_block.number.to(),
            tx_index: rpc_tx.transaction_index.to::<u32>(),
            tx_hash: rpc_tx.hash,
            tx_type: rpc_tx.tx_type.to::<u8>() as i32,
            nonce: rpc_tx.nonce.to(),
            gas_price: rpc_tx.gas_price.map(|g| g.to()),
            max_fee_per_gas: rpc_tx.max_fee_per_gas.map(|g| g.to()),
            max_priority_fee_per_gas: rpc_tx.max_priority_fee_per_gas.map(|g| g.to()),
            max_fee_per_blob_gas: rpc_tx.max_fee_per_blob_gas.map(|g| g.to()),
            gas_limit: rpc_tx.gas.to(),
            to: rpc_tx.to,
            value: rpc_tx.value,
            input: rpc_tx.input.clone(),
            r: rpc_tx.r,
            s: rpc_tx.s,
            v_parity,
            chain_id: rpc_tx.chain_id.map(|c| c.to()),
            from: rpc_tx.from,
            access_list: rpc_tx.access_list.as_ref().map(|list| {
                list.iter()
                    .map(|item| (item.address, item.storage_keys.clone()))
                    .collect()
            }),
            blob_versioned_hashes: rpc_tx.blob_versioned_hashes.clone(),
            gas_used: receipt.cumulative_gas_used.to(),
            status: receipt.status.map(|s| s.to::<u64>() != 0).unwrap_or(true),
            state_root: receipt.root,
            authorization_list: rpc_tx.authorization_list.as_ref().map(|list| {
                list.iter()
                    .map(|item| {
                        (
                            item.chain_id.to(),
                            item.address,
                            item.nonce.to(),
                            item.y_parity.to::<u64>() != 0,
                            item.r,
                            item.s,
                        )
                    })
                    .collect()
            }),
        };

        transactions.push(transaction);
    }

    let mut logs = Vec::new();
    for receipt in &rpc_receipts {
        for rpc_log in &receipt.logs {
            let log = Log {
                block_number: rpc_block.number.to(),
                tx_index: receipt.transaction_index.to::<u32>(),
                log_index: rpc_log.log_index.to::<u32>(),
                address: rpc_log.address,
                topics: rpc_log.topics.clone(),
                data: rpc_log.data.clone(),
            };
            logs.push(log);
        }
    }

    Ok(Block {
        number: rpc_block.number.to(),
        timestamp: rpc_block.timestamp.to(),
        hash: rpc_block.hash,
        parent_hash: rpc_block.parent_hash,
        ommers_hash: rpc_block.sha3_uncles,
        miner: rpc_block.miner,
        state_root: rpc_block.state_root,
        transactions_root: rpc_block.transactions_root,
        receipts_root: rpc_block.receipts_root,
        logs_bloom: rpc_block.logs_bloom,
        difficulty: rpc_block.difficulty,
        total_difficulty: rpc_block.total_difficulty,
        gas_limit: rpc_block.gas_limit.to(),
        gas_used: rpc_block.gas_used.to(),
        extra_data: rpc_block.extra_data,
        mix_hash: rpc_block.mix_hash,
        nonce: rpc_block.nonce.to(),
        base_fee_per_gas: rpc_block.base_fee_per_gas.map(|b| b.to()),
        withdrawals_root: rpc_block.withdrawals_root,
        blob_gas_used: rpc_block.blob_gas_used.map(|b| b.to()),
        excess_blob_gas: rpc_block.excess_blob_gas.map(|e| e.to()),
        parent_beacon_root: rpc_block.parent_beacon_block_root,
        requests_hash: rpc_block.requests_hash,
        transactions,
        logs,
    })
}

/// Custom deserializers for B256 fields that may have odd-length hex strings.
/// Some RPC providers return values like "0x1a2b3c" instead of "0x00...01a2b3c".
///
/// Parse a single B256, padding with leading zeros if needed
fn parse_b256_lenient(s: &str) -> Result<B256, alloy::hex::FromHexError> {
    let hex = s.trim_start_matches("0x");
    if hex.len() < 64 {
        format!("{:0>64}", hex).parse()
    } else {
        hex.parse()
    }
}

fn deserialize_b256_lenient<'de, D>(deserializer: D) -> Result<B256, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    parse_b256_lenient(&s).map_err(serde::de::Error::custom)
}

fn deserialize_b256_vec_lenient<'de, D>(deserializer: D) -> Result<Option<Vec<B256>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt: Option<Vec<String>> = Option::deserialize(deserializer)?;
    opt.map(|vec| {
        vec.iter()
            .map(|s| parse_b256_lenient(s))
            .collect::<Result<Vec<_>, _>>()
    })
    .transpose()
    .map_err(serde::de::Error::custom)
}

fn deserialize_b256_vec_required<'de, D>(deserializer: D) -> Result<Vec<B256>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let vec: Vec<String> = Vec::deserialize(deserializer)?;
    vec.iter()
        .map(|s| parse_b256_lenient(s))
        .collect::<Result<Vec<_>, _>>()
        .map_err(serde::de::Error::custom)
}
