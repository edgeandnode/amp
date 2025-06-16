use std::collections::HashMap;
use std::{
    mem,
    sync::Arc,
    time::{Duration, Instant},
};

use alloy::{
    consensus::{EthereumTxEnvelope, Transaction as _},
    eips::{BlockNumberOrTag, Typed2718},
    hex::{self, ToHexExt},
    primitives::FixedBytes,
    providers::Provider as _,
    rpc::{
        client::BatchRequest,
        json_rpc::{RpcRecv, RpcSend},
        types::{Header, Log as RpcLog, TransactionReceipt},
    },
    transports::http::reqwest::Url,
};
use async_stream::stream;
use common::{
    evm::tables::{
        blocks::{Block, BlockRowsBuilder},
        logs::{Log, LogRowsBuilder},
    },
    metadata::range::BlockRange,
    BlockNum, BlockStreamer, BoxError, EvmCurrency, RawDatasetRows, Timestamp,
};
use futures::{future::try_join_all, Stream};
use thiserror::Error;
use tracing::{error, instrument, warn};

use crate::tables::transactions::{Transaction, TransactionRowsBuilder};
use prometheus::{register_histogram, Histogram};

lazy_static::lazy_static! {
    static ref RPC_REQUEST_DURATION: Histogram = register_histogram!(
        "rpc_request_duration_seconds",
        "Histogram of RPC request durations in seconds"
    ).unwrap();
    static ref RPC_REQUEST_ERRORS: prometheus::IntCounter = prometheus::register_int_counter!(
        "rpc_request_errors_total",
        "Total number of errors encountered during RPC requests"
    ).unwrap();
}

#[derive(Error, Debug)]
pub enum ToRowError {
    #[error("missing field: {0}")]
    Missing(&'static str),
    #[error("overflow in field {0}: {1}")]
    Overflow(&'static str, BoxError),
}
pub struct BatchingRpcWrapper {
    client: alloy::providers::RootProvider,
    batch_size: usize,
    retries: usize,
    limiter: Arc<tokio::sync::Semaphore>,
}

impl BatchingRpcWrapper {
    pub fn new(
        client: alloy::providers::RootProvider,
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
    /// calls: Vec<(&'static str, Params)> - a vector of tuples containing the method name and parameters.
    #[instrument(name = "batch_execute", skip(self, calls))]
    pub async fn execute<T: RpcRecv, Params: RpcSend>(
        &self,
        calls: Vec<(&'static str, Params)>,
    ) -> Result<Vec<T>, BoxError> {
        if calls.is_empty() {
            warn!("No calls to execute, returning empty result");
            return Ok(Vec::new());
        }
        let mut results = Vec::new();
        let mut remaining_calls = calls;
        let mut remaining_attempts = self.retries;

        let all_start = Instant::now();
        while !remaining_calls.is_empty() {
            let start = Instant::now();
            let starting_calls = remaining_calls.len();
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

            let timer = RPC_REQUEST_DURATION.start_timer();
            match batch_then_waiters.await {
                Ok(responses) => {
                    tracing::info!(
                        "batched block stream: processed {}/{} calls in {}ms",
                        responses.len(),
                        starting_calls,
                        start.elapsed().as_millis()
                    );
                    results.extend(responses);
                    timer.observe_duration();
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
                    RPC_REQUEST_ERRORS.inc();
                    tracing::error!("Batch execution failed: {:?}", e);
                    return Err(e.into());
                }
            }
        }
        tracing::info!(
            "BatchingRpcWrapper executed {} calls in {}ms",
            results.len(),
            all_start.elapsed().as_millis()
        );

        Ok(results)
    }
}

#[derive(Clone)]
pub struct JsonRpcClient {
    client: alloy::providers::RootProvider,
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
        let client = alloy::providers::RootProvider::new_http(url);
        let limiter = tokio::sync::Semaphore::new(request_limit as usize).into();
        Ok(Self {
            client,
            network,
            limiter,
            batch_size,
        })
    }

    /// Create a stream that fetches blocks from start_block to end_block. One method is called at a time.
    /// This is used when batch_size is set to 1 in the provider config.
    #[instrument(name = "unbatched_block_stream", skip(self))]
    fn unbatched_block_stream(
        self,
        start_block: u64,
        end_block: u64,
    ) -> impl Stream<Item = Result<RawDatasetRows, BoxError>> + Send {
        tracing::info!(
            "Fetching blocks (not batched) {} to {}",
            start_block,
            end_block
        );
        stream! {
            for block_num in start_block..=end_block {
                let client_permit = self.limiter.acquire().await;
                let timer = RPC_REQUEST_DURATION.start_timer();
                let block_result = self
                    .client
                    .get_block_by_number(BlockNumberOrTag::Number(block_num))
                    .full()
                    .await;

                let block = match block_result {
                    Ok(Some(block)) => {
                        timer.observe_duration();
                        tracing::info!("Block {} fetched successfully", block_num);
                        block
                    }
                    Ok(None) => {
                        timer.observe_duration();
                        tracing::warn!("Block {} not found", block_num);
                        yield Err(format!("block {} not found", block_num).into());
                        continue;
                    }
                    Err(err) => {
                        timer.observe_duration();
                        tracing::error!("Failed to fetch block {}: {:?}", block_num, err);
                        yield Err(err.into());
                        continue;
                    }
                };

                let receipts_result = try_join_all(
                    block
                        .transactions
                        .hashes()
                        .map(|hash| self.client.get_transaction_receipt(hash)),
                )
                .await;

                drop(client_permit);

                let receipts = match receipts_result {
                    Ok(receipts) => receipts,
                    Err(err) => {
                        tracing::error!("Failed to fetch receipts for block {}: {:?}", block_num, err);
                        yield Err(err.into());
                        continue;
                    }
                };

                yield rpc_to_rows(block, receipts, &self.network);
            }
        }
    }

    /// Create a stream that fetches blocks in batches to avoid overwhelming the RPC server.
    /// This is used when rpc_batch_size is set > 1 in the provider config.
    fn batched_block_stream(
        self,
        start_block: u64,
        end_block: u64,
    ) -> impl Stream<Item = Result<RawDatasetRows, BoxError>> + Send {
        tracing::info!("Fetching blocks (batched) {} to {}", start_block, end_block);
        let batching_client = BatchingRpcWrapper::new(
            self.client.clone(),
            self.batch_size,
            10,
            self.limiter.clone(),
        );

        let block_calls: Vec<_> = (start_block..=end_block)
            .map(|block_num| ("eth_getBlockByNumber", (block_num, true)))
            .collect();

        stream! {
            // Collect blocks and their transaction hashes together
            let blocks_result: Result<Vec<alloy::rpc::types::Block>, BoxError> = batching_client.execute(block_calls).await;
            let blocks = match blocks_result {
                Ok(blocks) => blocks,
                Err(err) => {
                    yield Err(err);
                    return;
                }
            };

            // Collect all transaction hashes from the blocks, and those should be fetched in a big batch
            let mut block_tx_hashes: HashMap<u64, Vec<FixedBytes<32>>> = HashMap::new();
            let mut all_transaction_hashes = Vec::new();
            for block in &blocks {
                let block_num = block.header.number;
                let tx_hashes: Vec<FixedBytes<32>> = block.transactions.hashes().collect();
                all_transaction_hashes.extend(&tx_hashes);
                block_tx_hashes.insert(block_num, tx_hashes);
            }

            if !all_transaction_hashes.is_empty() {
                // Fetch receipts in batch for all transaction hashes
                let receipt_calls: Vec<_> = all_transaction_hashes.iter()
                    .map(|hash: &FixedBytes<32>| (
                        "eth_getTransactionReceipt",
                        [format!("0x{}", hex::encode(hash))],
                    ))
                    .collect();

                tracing::info!("Fetching {} receipts for {} transactions", receipt_calls.len(), all_transaction_hashes.len());
                let receipts_result: Result<Vec<Option<TransactionReceipt>>, BoxError> = batching_client.execute(receipt_calls).await;
                let receipts = match receipts_result {
                    Ok(receipts) => receipts,
                    Err(err) => {
                        yield Err(err);
                        return;
                    }
                };

                // Map receipts to their tx_hash for fast lookup
                let tx_hash_to_receipt: HashMap<_, _> = all_transaction_hashes
                    .iter()
                    .cloned()
                    .zip(receipts.into_iter())
                    .collect();

                // For each block, reconstruct the per-block receipt vector by looking up each tx hash
                for block in blocks.into_iter() {
                    let tx_hashes = &block_tx_hashes[&block.header.number];
                    let block_receipts: Vec<_> = tx_hashes.iter().map(|h| tx_hash_to_receipt.get(h).cloned().unwrap_or(None)).collect();
                    yield rpc_to_rows(block, block_receipts, &self.network);
                }
            } else {
                // No transactions in any block, just yield the block rows
                for block in blocks.into_iter() {
                    yield rpc_to_rows(block, Vec::new(), &self.network);
                }
            }
        }
    }
}

impl AsRef<alloy::providers::RootProvider> for JsonRpcClient {
    fn as_ref(&self) -> &alloy::providers::RootProvider {
        &self.client
    }
}

impl BlockStreamer for JsonRpcClient {
    async fn block_stream(
        self,
        start: BlockNum,
        end: BlockNum,
    ) -> impl Stream<Item = Result<RawDatasetRows, BoxError>> + Send {
        // Each function returns a different concrete stream type, so we
        // use `stream!` to unify them into a wrapper stream
        stream! {
            if self.batch_size > 1 {
                for await item in self.batched_block_stream(start, end) {
                    yield item;
                }
            } else {
                for await item in self.unbatched_block_stream(start, end) {
                    yield item;
                }
            }
        }
    }

    async fn latest_block(&mut self, finalized: bool) -> Result<BlockNum, BoxError> {
        let number = match finalized {
            true => BlockNumberOrTag::Finalized,
            false => BlockNumberOrTag::Latest,
        };
        let _permit = self.limiter.acquire();
        let block = self.client.get_block_by_number(number).await?;
        Ok(block.map(|b| b.header.number).unwrap_or(0))
    }
}

fn rpc_to_rows(
    block: alloy::rpc::types::Block,
    receipts: Vec<Option<TransactionReceipt>>,
    network: &str,
) -> Result<RawDatasetRows, BoxError> {
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
        };
        for log in receipt_logs {
            logs.push(rpc_log_to_row(log, header.timestamp)?);
        }
        transactions.push(rpc_transaction_to_row(&header, tx, receipt, idx)?);
    }

    let block = BlockRange {
        numbers: header.block_num..=header.block_num,
        network: network.to_string(),
        hash: header.hash.into(),
        prev_hash: Some(header.parent_hash.into()),
    };

    let header_row = {
        let mut builder = BlockRowsBuilder::with_capacity(1);
        builder.append(&header);
        builder.build(block.clone())?
    };

    let logs_row = {
        let mut builder = LogRowsBuilder::with_capacity(logs.len());
        for log in logs {
            builder.append(&log);
        }
        builder.build(block.clone())?
    };

    let transactions_row = {
        let mut builder = TransactionRowsBuilder::with_capacity(transactions.len());
        for tx in transactions {
            if let Some(tx) = tx {
                builder.append(&tx);
            }
        }
        builder.build(block.clone())?
    };

    Ok(RawDatasetRows::new(vec![
        header_row,
        logs_row,
        transactions_row,
    ]))
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
    let sig = match &*tx.inner {
        EthereumTxEnvelope::Legacy(signed) => signed.signature(),
        EthereumTxEnvelope::Eip2930(signed) => signed.signature(),
        EthereumTxEnvelope::Eip1559(signed) => signed.signature(),
        EthereumTxEnvelope::Eip4844(signed) => signed.signature(),
        EthereumTxEnvelope::Eip7702(signed) => signed.signature(),
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
        from: tx.as_recovered().signer().into(),
        status: receipt.status().into(),
    }))
}
