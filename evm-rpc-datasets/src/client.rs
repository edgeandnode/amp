use std::{mem, sync::Arc, time::Duration};

use alloy::{
    consensus::{Transaction as _, TxEnvelope},
    eips::BlockNumberOrTag,
    hex::{self, ToHexExt},
    providers::Provider as _,
    rpc::{
        client::BatchRequest,
        json_rpc::{RpcParam, RpcReturn},
        types::{BlockTransactionsKind, Header, Log as RpcLog, TransactionReceipt},
    },
    transports::http::reqwest::Url,
};
use common::{
    evm::tables::{
        blocks::{Block, BlockRowsBuilder},
        logs::{Log, LogRowsBuilder},
    },
    BlockNum, BlockStreamer, BoxError, DatasetRows, EvmCurrency, Timestamp,
};
use futures::future::try_join_all;
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::tables::transactions::{Transaction, TransactionRowsBuilder};

#[derive(Error, Debug)]
pub enum ToRowError {
    #[error("missing field: {0}")]
    Missing(&'static str),
    #[error("overflow in field {0}: {1}")]
    Overflow(&'static str, BoxError),
}
pub struct BatchingRpcWrapper {
    client: alloy::providers::ReqwestProvider,
    batch_size: usize,
    retries: usize,
    limiter: Arc<tokio::sync::Semaphore>,
}

impl BatchingRpcWrapper {
    pub fn new(
        client: alloy::providers::ReqwestProvider,
        batch_size: usize,
        retries: usize,
        limiter: Arc<tokio::sync::Semaphore>,
    ) -> Self {
        assert!(batch_size > 0, "batch_size must be > 0");
        Self {
            client,
            batch_size,
            retries,
            limiter,
        }
    }

    /// Execute a batch of RPC calls with retries on failure.
    /// calls: Vec<(&'static str, P)> - a vector of tuples containing the method name and parameters.
    pub async fn execute<T: RpcReturn, P: RpcParam>(
        &self,
        calls: Vec<(&'static str, P)>,
    ) -> Result<Vec<T>, BoxError> {
        let mut results = Vec::new();
        let mut remaining_calls = calls;
        let mut remaining_attempts = self.retries;

        while !remaining_calls.is_empty() {
            let chunk: Vec<_> = remaining_calls
                .drain(..self.batch_size.min(remaining_calls.len()))
                .collect();

            // Acquire semaphore permit for the batch, which will be one request
            let _permit = self.limiter.acquire().await?;

            let mut batch = BatchRequest::new(self.client.client());
            let mut waiters = Vec::new();

            for (method, params) in chunk.iter() {
                waiters.push(batch.add_call(*method, &params)?);
            }

            let batch_then_waiters = async move {
                batch.send().await?;
                let responses = try_join_all(waiters).await?;
                Ok::<_, BoxError>(responses)
            };

            match batch_then_waiters.await {
                Ok(responses) => {
                    results.extend(responses);
                }
                Err(e) if remaining_attempts > 0 && self.batch_size > 1 => {
                    warn!(
                        "Batch failed. Error({:?}) Batch size {}. Retries left: {}",
                        e, self.batch_size, remaining_attempts
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await; // Avoid spamming
                    remaining_calls.splice(0..0, chunk); // Reinsert failed chunk
                    remaining_attempts -= 1;
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }

        Ok(results)
    }
}

#[derive(Clone)]
pub struct JsonRpcClient {
    client: alloy::providers::ReqwestProvider,
    network: String,
    limiter: Arc<tokio::sync::Semaphore>,
    batch_size: usize,
}

impl JsonRpcClient {
    pub fn new(
        url: Url,
        network: String,
        request_limit: u16,
        batch_size: usize,
    ) -> Result<Self, BoxError> {
        assert!(request_limit >= 1);
        let client = alloy::providers::ProviderBuilder::new().on_http(url);
        let limiter = tokio::sync::Semaphore::new(request_limit as usize).into();
        Ok(Self {
            client,
            network,
            limiter,
            batch_size,
        })
    }

    /// Fetch blocks from start_block to end_block. One method is called at a time.
    /// This is used when batch_size is set to 1 in the provider config.
    async fn unbatched_block_stream(
        self,
        start_block: u64,
        end_block: u64,
        tx: mpsc::Sender<DatasetRows>,
    ) -> Result<(), BoxError> {
        info!(
            "Fetching blocks (not batched) {} to {}",
            start_block, end_block
        );
        for block_num in start_block..=end_block {
            let client_permit = self.limiter.acquire().await;
            let block = self
                .client
                .get_block_by_number(
                    BlockNumberOrTag::Number(block_num),
                    BlockTransactionsKind::Full,
                )
                .await?
                .ok_or_else(|| format!("block {} not found", block_num))?;
            let receipts = try_join_all(
                block
                    .transactions
                    .hashes()
                    .map(|hash| self.client.get_transaction_receipt(hash)),
            )
            .await?;
            drop(client_permit);

            let rows = rpc_to_rows(block, receipts, &self.network)?;

            // Send the block and check if the receiver has gone away.
            if tx.send(rows).await.is_err() {
                break;
            }
        }
        Ok(())
    }

    /// Fetch blocks in batches to avoid overwhelming the RPC server.
    /// This is used when rpc_batch_size is set > 1 in the provider config.
    async fn batched_block_stream(
        self,
        start_block: u64,
        end_block: u64,
        tx: mpsc::Sender<DatasetRows>,
    ) -> Result<(), BoxError> {
        info!("Fetching blocks (batched) {} to {}", start_block, end_block);
        let batching_client = BatchingRpcWrapper::new(
            self.client.clone(),
            self.batch_size,
            10,
            self.limiter.clone(),
        );

        let block_calls: Vec<_> = (start_block..=end_block)
            .map(|block_num| ("eth_getBlockByNumber", (block_num, true)))
            .collect();

        let blocks: Vec<alloy::rpc::types::Block> = batching_client.execute(block_calls).await?;

        for block in blocks {
            let transaction_hashes = block.transactions.hashes();

            // Fetch receipts in batch
            let receipt_calls: Vec<_> = transaction_hashes
                .map(|hash| {
                    (
                        "eth_getTransactionReceipt",
                        [format!("0x{}", hex::encode(hash))],
                    )
                })
                .collect();

            let receipts: Vec<Option<TransactionReceipt>> =
                batching_client.execute(receipt_calls).await?;

            let rows = rpc_to_rows(block, receipts, &self.network)?;

            // Send the block and check if the receiver has gone away.
            if tx.send(rows).await.is_err() {
                break;
            }
        }
        Ok(())
    }
}

impl AsRef<alloy::providers::ReqwestProvider> for JsonRpcClient {
    fn as_ref(&self) -> &alloy::providers::ReqwestProvider {
        &self.client
    }
}

impl BlockStreamer for JsonRpcClient {
    async fn block_stream(
        self,
        start: BlockNum,
        end: BlockNum,
        tx: mpsc::Sender<common::DatasetRows>,
    ) -> Result<(), BoxError> {
        if self.batch_size > 1 {
            self.batched_block_stream(start, end, tx).await
        } else {
            self.unbatched_block_stream(start, end, tx).await
        }
    }

    async fn latest_block(&mut self, finalized: bool) -> Result<BlockNum, BoxError> {
        let number = match finalized {
            true => BlockNumberOrTag::Finalized,
            false => BlockNumberOrTag::Latest,
        };
        let kind = BlockTransactionsKind::Hashes;
        let _permit = self.limiter.acquire();
        let block = self.client.get_block_by_number(number, kind).await?;
        Ok(block.map(|b| b.header.number).unwrap_or(0))
    }
}

fn rpc_to_rows(
    block: alloy::rpc::types::Block,
    receipts: Vec<Option<TransactionReceipt>>,
    network: &str,
) -> Result<DatasetRows, BoxError> {
    let header = rpc_header_to_row(block.header)?;
    let mut logs = Vec::new();
    let mut transactions = Vec::new();

    for (idx, (tx, receipt)) in block
        .transactions
        .into_transactions()
        .zip(receipts)
        .enumerate()
    {
        let mut receipt = receipt.ok_or_else(|| {
            format!(
                "receipt not found for tx {:?}",
                tx.inner.tx_hash().encode_hex()
            )
        })?;
        // Move the logs out of the nested structure.
        let receipt_logs = match &mut receipt.inner {
            alloy::consensus::ReceiptEnvelope::Legacy(receipt_with_bloom) => {
                mem::take(&mut receipt_with_bloom.receipt.logs)
            }
            alloy::consensus::ReceiptEnvelope::Eip2930(receipt_with_bloom) => {
                mem::take(&mut receipt_with_bloom.receipt.logs)
            }
            alloy::consensus::ReceiptEnvelope::Eip1559(receipt_with_bloom) => {
                mem::take(&mut receipt_with_bloom.receipt.logs)
            }
            alloy::consensus::ReceiptEnvelope::Eip4844(receipt_with_bloom) => {
                mem::take(&mut receipt_with_bloom.receipt.logs)
            }
            alloy::consensus::ReceiptEnvelope::Eip7702(receipt_with_bloom) => {
                mem::take(&mut receipt_with_bloom.receipt.logs)
            }
            _ => {
                tracing::warn!("unexpected receipt type");
                vec![]
            }
        };
        for log in receipt_logs {
            logs.push(rpc_log_to_row(log, header.timestamp)?);
        }
        transactions.push(rpc_transaction_to_row(&header, tx, receipt, idx)?);
    }

    let header_row = {
        let mut builder = BlockRowsBuilder::with_capacity(1);
        builder.append(&header);
        builder.build(network.to_string())?
    };

    let logs_row = {
        let mut builder = LogRowsBuilder::with_capacity(logs.len());
        for log in logs {
            builder.append(&log);
        }
        builder.build(network.to_string())?
    };

    let transactions_row = {
        let mut builder = TransactionRowsBuilder::with_capacity(transactions.len());
        for tx in transactions {
            if let Some(tx) = tx {
                builder.append(&tx);
            }
        }
        builder.build(network.to_string())?
    };

    Ok(DatasetRows(vec![header_row, logs_row, transactions_row]))
}

fn rpc_header_to_row(header: Header) -> Result<Block, ToRowError> {
    Ok(Block {
        block_num: header.number,
        timestamp: Timestamp(Duration::from_secs(header.timestamp)),
        hash: header.hash.into(),
        parent_hash: header.parent_hash.into(),
        ommers_hash: header.ommers_hash.into(),
        miner: header.beneficiary.into(),
        state_root: header.state_root.into(),
        transactions_root: header.transactions_root.into(),
        receipt_root: header.receipts_root.into(),
        logs_bloom: <[u8; 256]>::from(header.logs_bloom).into(),
        difficulty: EvmCurrency::try_from(header.difficulty)
            .map_err(|e| ToRowError::Overflow("difficulty", e.into()))?,
        gas_limit: u64::try_from(header.gas_limit)
            .map_err(|e| ToRowError::Overflow("gas_limit", e.into()))?,
        gas_used: u64::try_from(header.gas_used)
            .map_err(|e| ToRowError::Overflow("gas_used", e.into()))?,
        extra_data: header.extra_data.0.to_vec(),
        mix_hash: header.mix_hash.into(),
        nonce: header.nonce.into(),
        base_fee_per_gas: header
            .base_fee_per_gas
            .map(|b| {
                EvmCurrency::try_from(b)
                    .map_err(|e| ToRowError::Overflow("base_fee_per_gas", e.into()))
            })
            .transpose()?,
        withdrawals_root: header.withdrawals_root.map(Into::into),
        blob_gas_used: header.blob_gas_used,
        excess_blob_gas: header.excess_blob_gas,
        parent_beacon_root: header.parent_beacon_block_root.map(Into::into),
    })
}

fn rpc_log_to_row(log: RpcLog, timestamp: Timestamp) -> Result<Log, ToRowError> {
    Ok(Log {
        block_hash: log
            .block_hash
            .ok_or(ToRowError::Missing("block_hash"))?
            .into(),
        block_num: log
            .block_number
            .ok_or(ToRowError::Missing("block_number"))?,
        timestamp,
        tx_index: u32::try_from(
            log.transaction_index
                .ok_or(ToRowError::Missing("transaction_index"))?,
        )
        .map_err(|e| ToRowError::Overflow("transaction_index", e.into()))?,
        tx_hash: log
            .transaction_hash
            .ok_or(ToRowError::Missing("transaction_hash"))?
            .into(),
        log_index: u32::try_from(log.log_index.ok_or(ToRowError::Missing("log_index"))?)
            .map_err(|e| ToRowError::Overflow("log_index", e.into()))?,
        address: log.address().into(),
        topic0: log.topics().get(0).cloned().map(Into::into),
        topic1: log.topics().get(1).cloned().map(Into::into),
        topic2: log.topics().get(2).cloned().map(Into::into),
        topic3: log.topics().get(3).cloned().map(Into::into),
        data: log.data().data.to_vec(),
    })
}

fn rpc_transaction_to_row(
    block: &Block,
    tx: alloy::rpc::types::Transaction,
    receipt: TransactionReceipt,
    tx_index: usize,
) -> Result<Option<Transaction>, ToRowError> {
    let sig = match &tx.inner {
        TxEnvelope::Legacy(signed) => signed.signature(),
        TxEnvelope::Eip2930(signed) => signed.signature(),
        TxEnvelope::Eip1559(signed) => signed.signature(),
        TxEnvelope::Eip4844(signed) => signed.signature(),
        TxEnvelope::Eip7702(signed) => signed.signature(),
        _ => {
            tracing::warn!("unexpected tx type");
            return Ok(None);
        }
    };
    Ok(Some(Transaction {
        block_hash: block.hash,
        block_num: block.block_num,
        timestamp: block.timestamp,
        tx_index: u32::try_from(tx_index)
            .map_err(|e| ToRowError::Overflow("tx_index", e.into()))?,
        tx_hash: tx.inner.tx_hash().0,
        to: tx.to().map(|addr| addr.0 .0).unwrap_or_default(),
        nonce: tx.nonce(),
        gas_price: tx
            .gas_price()
            .map(i128::try_from)
            .transpose()
            .map_err(|e| ToRowError::Overflow("gas_price", e.into()))?,
        gas_limit: tx.gas_limit(),
        value: i128::try_from(tx.value()).map_err(|e| ToRowError::Overflow("value", e.into()))?,
        input: tx.input().to_vec(),
        v: if sig.v() { vec![1] } else { vec![] },
        r: sig.r().to_be_bytes_vec(),
        s: sig.s().to_be_bytes_vec(),
        receipt_cumulative_gas_used: u64::try_from(receipt.inner.cumulative_gas_used())
            .map_err(|e| ToRowError::Overflow("cumulative_gas_used", e.into()))?,
        r#type: tx.ty().into(),
        max_fee_per_gas: i128::try_from(tx.max_fee_per_gas())
            .map_err(|e| ToRowError::Overflow("max_fee_per_gas", e.into()))?,
        max_priority_fee_per_gas: tx
            .max_priority_fee_per_gas()
            .map(i128::try_from)
            .transpose()
            .map_err(|e| ToRowError::Overflow("max_priority_fee_per_gas", e.into()))?,
        max_fee_per_blob_gas: tx
            .max_fee_per_blob_gas()
            .map(i128::try_from)
            .transpose()
            .map_err(|e| ToRowError::Overflow("max_fee_per_blob_gas", e.into()))?,
        from: tx.from.0 .0,
        status: receipt.status().into(),
    }))
}
