use std::{
    num::NonZeroU32,
    ops::Deref,
    sync::Arc,
    time::{Duration, Instant},
};

use alloy::{
    consensus::{EthereumTxEnvelope, Transaction as _},
    eips::{BlockId, BlockNumberOrTag, Typed2718},
    hex::{self, ToHexExt},
    network::{
        AnyHeader, AnyNetwork, AnyReceiptEnvelope, AnyRpcBlock, AnyRpcTransaction, AnyTxEnvelope,
        TransactionResponse,
    },
    providers::Provider as _,
    rpc::{
        self,
        client::BatchRequest,
        json_rpc::{RpcRecv, RpcSend},
        types::{Header, Log as RpcLog, TransactionReceipt},
    },
    transports::http::reqwest::Url,
};
use async_stream::stream;
use common::{
    BlockNum, BlockStreamer, BoxError, BoxResult, EvmCurrency, RawDatasetRows, Timestamp,
    evm::{
        self,
        tables::{
            blocks::{Block, BlockRowsBuilder},
            logs::{self, LogRowsBuilder},
        },
    },
    metadata::segments::BlockRange,
};
use futures::{Stream, future::try_join_all};
use thiserror::Error;
use tracing::{error, instrument, warn};

use crate::tables::transactions::{Transaction, TransactionRowsBuilder};

type AnyTxReceipt =
    alloy::serde::WithOtherFields<TransactionReceipt<AnyReceiptEnvelope<rpc::types::Log>>>;

#[derive(Error, Debug)]
pub enum ToRowError {
    #[error("missing field: {0}")]
    Missing(&'static str),
    #[error("overflow in field {0}: {1}")]
    Overflow(&'static str, BoxError),
}

struct BatchingRpcWrapper {
    client: RootProviderWithMetrics,
    batch_size: usize,
    retries: usize,
    limiter: Arc<tokio::sync::Semaphore>,
}

impl BatchingRpcWrapper {
    fn new(
        client: RootProviderWithMetrics,
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
    async fn execute<T: RpcRecv, Params: RpcSend>(
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

        while !remaining_calls.is_empty() {
            let chunk: Vec<_> = remaining_calls
                .drain(..self.batch_size.min(remaining_calls.len()))
                .collect();

            // Acquire semaphore permit for the batch, which will be one request
            let _permit = self.limiter.acquire().await?;

            let batch_result = self.client.batch_request(&chunk).await;

            match batch_result {
                Ok(responses) => {
                    results.extend(responses);
                }
                Err(e) if remaining_attempts > 0 && self.batch_size > 1 => {
                    self.request_batch_individually(&mut results, remaining_attempts, &chunk, e)
                        .await?;
                    remaining_calls.splice(0..0, chunk);
                    remaining_attempts -= 1;
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
        Ok(results)
    }

    /// If a batch fails, try each call individually to isolate the failure for debugging.
    async fn request_batch_individually<T: RpcRecv, Params: RpcSend>(
        &self,
        results: &mut Vec<T>,
        remaining_attempts: usize,
        chunk: &[(&'static str, Params)],
        e: BoxError,
    ) -> Result<(), BoxError> {
        let delay_ms = 500 * (self.retries - remaining_attempts) as u64;
        warn!(
            "Batch failed. Error({:?}) Batch size {}. Retries left: {}, will wait for {}ms before retrying",
            e, self.batch_size, remaining_attempts, delay_ms
        );
        for (method, params) in chunk {
            tracing::info!(
                "Retrying {} with params {:?} after error: {}",
                method,
                params,
                e
            );
            let _permit = self.limiter.acquire().await?;
            let result = self.client.request(method, params).await;
            match result {
                Ok(response) => {
                    results.push(response);
                }
                Err(err) => {
                    error!(
                        "Error executing {} with params {:?}: {}",
                        method, params, err
                    );
                    return Err(err.into());
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        Ok(())
    }
}

#[derive(Clone)]
pub struct JsonRpcClient {
    client: RootProviderWithMetrics,
    network: String,
    provider_name: String,
    limiter: Arc<tokio::sync::Semaphore>,
    batch_size: usize,
    fetch_receipts_per_tx: bool,
    final_blocks_only: bool,
}

impl JsonRpcClient {
    pub fn new(
        url: Url,
        network: String,
        provider_name: String,
        request_limit: u16,
        batch_size: usize,
        rate_limit: Option<NonZeroU32>,
        fetch_receipts_per_tx: bool,
        final_blocks_only: bool,
        meter: Option<&monitoring::telemetry::metrics::Meter>,
    ) -> Result<Self, BoxError> {
        assert!(request_limit >= 1);
        let client = evm::provider::new(url, rate_limit);
        let client = RootProviderWithMetrics::new(client, meter);
        let limiter = tokio::sync::Semaphore::new(request_limit as usize).into();
        Ok(Self {
            client,
            network,
            provider_name,
            limiter,
            batch_size,
            fetch_receipts_per_tx,
            final_blocks_only,
        })
    }

    pub async fn new_ipc(
        path: std::path::PathBuf,
        network: String,
        provider_name: String,
        request_limit: u16,
        batch_size: usize,
        rate_limit: Option<NonZeroU32>,
        fetch_receipts_per_tx: bool,
        final_blocks_only: bool,
        meter: Option<&monitoring::telemetry::metrics::Meter>,
    ) -> Result<Self, BoxError> {
        assert!(request_limit >= 1);
        let client = evm::provider::new_ipc(path, rate_limit)
            .await
            .map(|c| RootProviderWithMetrics::new(c, meter))?;
        let limiter = tokio::sync::Semaphore::new(request_limit as usize).into();
        Ok(Self {
            client,
            network,
            provider_name,
            limiter,
            batch_size,
            fetch_receipts_per_tx,
            final_blocks_only,
        })
    }

    pub async fn new_ws(
        url: Url,
        network: String,
        provider_name: String,
        request_limit: u16,
        batch_size: usize,
        rate_limit: Option<NonZeroU32>,
        fetch_receipts_per_tx: bool,
        final_blocks_only: bool,
        meter: Option<&monitoring::telemetry::metrics::Meter>,
    ) -> Result<Self, BoxError> {
        assert!(request_limit >= 1);
        let client = evm::provider::new_ws(url, rate_limit)
            .await
            .map(|c| RootProviderWithMetrics::new(c, meter))?;
        let limiter = tokio::sync::Semaphore::new(request_limit as usize).into();
        Ok(Self {
            client,
            network,
            provider_name,
            limiter,
            batch_size,
            fetch_receipts_per_tx,
            final_blocks_only,
        })
    }

    pub fn provider_name(&self) -> &str {
        &self.provider_name
    }

    /// Create a stream that fetches blocks from start_block to end_block. One method is called at a time.
    /// This is used when batch_size is set to 1 in the provider config.
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

        let mut last_progress_report = Instant::now();

        stream! {
            'outer: for block_num in start_block..=end_block {
                let elapsed = last_progress_report.elapsed();
                if elapsed >= Duration::from_secs(15) {
                    tracing::info!(start = start_block, end = end_block, block = %block_num, "Progress");
                    last_progress_report = Instant::now();
                }

                let _permit = self.limiter.acquire().await.unwrap();

                let block_num = BlockNumberOrTag::Number(block_num);
                let block = self.client.with_metrics(async |c| {
                    c.get_block_by_number(block_num).full().await
                }).await;
                let block = match block {
                    Ok(Some(block)) => block,
                    Ok(None) => {
                        yield Err(format!("block {} not found", block_num).into());
                        continue;
                    }
                    Err(err) => {
                        yield Err(err.into());
                        continue;
                    }
                };

                if block.transactions.is_empty() {
                    // Avoid sending an RPC request just to get an empty vector.
                    yield rpc_to_rows(block, Vec::new(), &self.network);
                    continue;
                }

                let receipts = if self.fetch_receipts_per_tx {
                    let calls = block
                        .transactions
                        .hashes()
                        .map(|hash| {
                            let client = &self.client;
                            async move {
                                client.with_metrics(|c| async move {
                                    c.get_transaction_receipt(hash).await.map(|r| (hash, r))
                                }).await
                            }
                        });
                    let Ok(receipts) = try_join_all(calls).await else {
                        yield Err(format!("error fetching receipts for block {}", block.header.number).into());
                        continue;
                    };
                    let mut received_receipts = Vec::new();
                    for (hash, receipt) in receipts {
                        match receipt {
                            Some(receipt) => received_receipts.push(receipt),
                            None => {
                                yield Err(format!("missing receipt for transaction: {}", hex::encode(&hash)).into());
                                continue 'outer;
                            }
                        }
                    }
                    received_receipts
                } else {
                    let receipts = self.client
                        .with_metrics(async |c| {
                            c.get_block_receipts(BlockId::Number(block_num)).await
                        })
                        .await
                        .map(|receipts| {
                            let mut receipts = receipts.expect("block already found");
                            receipts.sort_by(|r1, r2| r1.transaction_index.cmp(&r2.transaction_index));
                            receipts
                        });
                    let Ok(receipts) = receipts else {
                        yield Err("error fetching receipts".into());
                        continue;
                    };
                    receipts
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

        let mut blocks_completed = 0;
        let mut txns_completed = 0;

        stream! {
            let stream_start = Instant::now();
            let block_calls: Vec<_> = (start_block..=end_block)
                .map(|block_num| (
                    "eth_getBlockByNumber",
                    (BlockNumberOrTag::Number(block_num), true),
                ))
                .collect::<Vec<_>>()
                .chunks(self.batch_size * 10)
                .map(<[_]>::to_vec)
                .collect();

            for batch_calls in block_calls {
                let start = Instant::now();
                let blocks_result: Result<Vec<AnyRpcBlock>, BoxError> = batching_client.execute(batch_calls).await;
                let blocks = match blocks_result {
                    Ok(blocks) => blocks,
                    Err(err) => {
                        yield Err(err);
                        return;
                    }
                };

                let total_tx_count: usize = blocks.iter().map(|b| b.transactions.len()).sum();

                if total_tx_count == 0 {
                    // No transactions in any block, just yield the block rows
                    for block in blocks.into_iter() {
                        blocks_completed += 1;
                        yield rpc_to_rows(block, Vec::new(), &self.network);
                    }
                } else {
                    let all_receipts_result: Result<Vec<_>, _> = if self.fetch_receipts_per_tx {
                        let receipt_calls: Vec<_> = blocks
                            .iter()
                            .flat_map(|block| {
                                block.transactions.hashes().map(|tx_hash| {
                                    let tx_hash = format!("0x{}",hex::encode(tx_hash));
                                    ("eth_getTransactionReceipt", [tx_hash])
                                })
                            })
                            .collect();
                        batching_client.execute(receipt_calls).await
                    } else {
                        let receipt_calls: Vec<_> = blocks
                            .iter()
                            .map(|block| (
                                "eth_getBlockReceipts",
                                [BlockNumberOrTag::Number(block.header.number)]
                            ))
                            .collect();
                        let receipts_result: Result<Vec<Vec<AnyTxReceipt>>, _> =
                            batching_client.execute(receipt_calls).await;
                        receipts_result.map(|receipts| receipts.into_iter().flatten().collect())
                    };

                    let all_receipts = match all_receipts_result {
                        Ok(receipts) => receipts,
                        Err(err) => {
                            yield Err(err);
                            return;
                        }
                    };

                    if total_tx_count != all_receipts.len() {
                        let err = format!(
                            "mismatched tx and receipt count in batch: {} txs, {} receipts",
                            total_tx_count,
                            all_receipts.len()
                        );
                        yield Err(err.into());
                        return;
                    }

                    let mut all_receipts = all_receipts.into_iter();

                    for block in blocks {
                        let mut block_receipts: Vec<_> =
                            all_receipts.by_ref().take(block.transactions.len()).collect();
                        block_receipts.sort_by(|r1, r2| r1.transaction_index.cmp(&r2.transaction_index));
                        blocks_completed += 1;
                        txns_completed += block.transactions.len();
                        yield rpc_to_rows(block, block_receipts, &self.network);
                    }
                }

                tracing::info!(
                    "Progress {}/{} ({}%) blocks (with {} txns) in {}ms",
                    blocks_completed,
                    end_block - start_block + 1,
                    (start_block as f32 / end_block as f32) * 100.0,
                    txns_completed,
                    start.elapsed().as_millis()
                );
            }
            tracing::info!(
                "Total time to fetch blocks {} to {}: {}ms, processed {} blocks with {} txns",
                start_block,
                end_block,
                stream_start.elapsed().as_millis(),
                blocks_completed,
                txns_completed
            );
        }
    }
}

impl AsRef<alloy::providers::RootProvider<AnyNetwork>> for JsonRpcClient {
    fn as_ref(&self) -> &alloy::providers::RootProvider<AnyNetwork> {
        &self.client.inner
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

    #[instrument(skip(self), err)]
    async fn latest_block(&mut self) -> Result<BlockNum, BoxError> {
        let number = match self.final_blocks_only {
            true => BlockNumberOrTag::Finalized,
            false => BlockNumberOrTag::Latest,
        };
        let _permit = self.limiter.acquire();
        let block = self
            .client
            .with_metrics(async |c| c.get_block_by_number(number).await)
            .await?;
        Ok(block.map(|b| b.header.number).unwrap_or(0))
    }

    fn provider_name(&self) -> &str {
        &self.provider_name
    }
}

#[derive(Debug, Clone)]
struct RootProviderWithMetrics {
    inner: alloy::providers::RootProvider<AnyNetwork>,
    metrics: Option<JsonRpcMetrics>,
}

impl RootProviderWithMetrics {
    fn new(
        inner: alloy::providers::RootProvider<AnyNetwork>,
        meter: Option<&monitoring::telemetry::metrics::Meter>,
    ) -> Self {
        let metrics = meter.map(|meter| JsonRpcMetrics {
            num_rpc_calls_single: monitoring::telemetry::metrics::Counter::new(
                meter,
                "evm_rpc_num_calls_single",
                "Number of single RPC calls made",
            ),
            num_rpc_calls_batch: monitoring::telemetry::metrics::Counter::new(
                meter,
                "evm_rpc_num_calls_batch",
                "Number of batch RPC calls made",
            ),
            num_failed_rpc_calls: monitoring::telemetry::metrics::Counter::new(
                meter,
                "evm_rpc_num_failed_calls",
                "Number of failed RPC calls",
            ),
            rpc_call_latency: monitoring::telemetry::metrics::Histogram::new_f64(
                meter,
                "evm_rpc_call_latency_ms",
                "Latency of RPC calls in milliseconds",
                "ms",
            ),
        });
        Self { inner, metrics }
    }

    /// Use the provider to execute a function, recording metrics if enabled.
    async fn with_metrics<T, E, F, Fut>(&self, func: F) -> Fut::Output
    where
        F: FnOnce(alloy::providers::RootProvider<AnyNetwork>) -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let Some(metrics) = self.metrics.as_ref() else {
            // Execute the function without recording metrics.
            return func(self.inner.clone()).await;
        };

        let start = Instant::now();

        let resp = func(self.inner.clone()).await;

        let duration = start.elapsed().as_millis() as f64;
        metrics.rpc_call_latency.record(duration);
        metrics.num_rpc_calls_single.inc();
        if resp.is_err() {
            metrics.num_failed_rpc_calls.inc();
        }

        resp
    }

    /// Send a single RPC request, recording metrics if enabled.
    async fn request<Params, Resp>(&self, method: &'static str, params: Params) -> BoxResult<Resp>
    where
        Params: RpcSend,
        Resp: RpcRecv,
    {
        let client = self.inner.client();

        let Some(metrics) = self.metrics.as_ref() else {
            // Send request without recording metrics.
            let resp = client.request(method, params).await?;
            return Ok(resp);
        };

        let start = Instant::now();

        let resp = client.request(method, params).await;

        let duration = start.elapsed().as_millis() as f64;
        metrics.rpc_call_latency.record(duration);
        metrics.num_rpc_calls_single.inc();
        if resp.is_err() {
            metrics.num_failed_rpc_calls.inc();
        }

        let resp = resp?;
        Ok(resp)
    }

    /// Send a batch of RPC requests, recording metrics if enabled.
    async fn batch_request<Params, Resp>(
        &self,
        requests: &[(&'static str, Params)],
    ) -> BoxResult<Vec<Resp>>
    where
        Params: RpcSend,
        Resp: RpcRecv,
    {
        let mut batch = BatchRequest::new(self.inner.client());
        let mut waiters = Vec::new();

        for (method, params) in requests.iter() {
            waiters.push(batch.add_call(*method, &params)?);
        }

        let Some(metrics) = self.metrics.as_ref() else {
            // Send batch request without recording metrics.
            batch.send().await?;
            let resp = try_join_all(waiters).await?;

            return Ok(resp);
        };

        let start = Instant::now();

        batch.send().await.inspect_err(|_| {
            let duration = start.elapsed().as_millis() as f64;
            metrics.rpc_call_latency.record(duration);
            metrics.num_rpc_calls_batch.inc();
        })?;

        let resp = try_join_all(waiters).await;

        let duration = start.elapsed().as_millis() as f64;
        metrics.rpc_call_latency.record(duration);
        metrics.num_rpc_calls_batch.inc();
        if resp.is_err() {
            metrics.num_failed_rpc_calls.inc();
        }

        let resp = resp?;
        Ok(resp)
    }
}

#[derive(Debug, Clone)]
struct JsonRpcMetrics {
    num_rpc_calls_single: monitoring::telemetry::metrics::Counter,
    num_rpc_calls_batch: monitoring::telemetry::metrics::Counter,
    num_failed_rpc_calls: monitoring::telemetry::metrics::Counter,
    rpc_call_latency: monitoring::telemetry::metrics::Histogram<f64>,
}

fn rpc_to_rows(
    block: AnyRpcBlock,
    receipts: Vec<AnyTxReceipt>,
    network: &str,
) -> Result<RawDatasetRows, BoxError> {
    if block.transactions.len() != receipts.len() {
        let err = format!(
            "mismatched tx and receipt count for block {}: {} txs, {} receipts",
            block.header.number,
            block.transactions.len(),
            receipts.len()
        );
        return Err(err.into());
    }
    let tx_receipt_pairs = block.transactions.clone().into_transactions().zip(receipts);

    let header = rpc_header_to_row(block.header.clone())?;
    let mut logs = Vec::new();
    let mut transactions = Vec::new();

    for (idx, (tx, mut receipt)) in tx_receipt_pairs.enumerate() {
        if tx.tx_hash() != receipt.transaction_hash {
            let err = format!(
                "mismatched tx and receipt hash for block {}: tx {}, receipt {}",
                header.block_num,
                tx.tx_hash().encode_hex(),
                receipt.transaction_hash.encode_hex()
            );
            return Err(err.into());
        }
        // Move the logs out of the nested structure.
        let receipt_logs = std::mem::take(&mut receipt.inner.inner.inner.receipt.logs);
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
        let mut builder = BlockRowsBuilder::with_capacity_for(&header);
        builder.append(&header);
        builder.build(block.clone())?
    };

    let logs_row = {
        let total_data_size = logs.iter().map(|log| log.data.len()).sum();
        let mut builder = LogRowsBuilder::with_capacity(logs.len(), total_data_size);
        for log in logs {
            builder.append(&log);
        }
        builder.build(block.clone())?
    };

    let transactions_row = {
        let total_input_size: usize = transactions.iter().map(|t| t.input.len()).sum();
        let total_v_size: usize = transactions.iter().map(|tx| tx.v.len()).sum();
        let total_r_size: usize = transactions.iter().map(|tx| tx.r.len()).sum();
        let total_s_size: usize = transactions.iter().map(|tx| tx.s.len()).sum();

        let mut builder = TransactionRowsBuilder::with_capacity(
            transactions.len(),
            total_input_size,
            total_v_size,
            total_r_size,
            total_s_size,
        );
        for tx in transactions {
            builder.append(&tx);
        }
        builder.build(block.clone())?
    };

    Ok(RawDatasetRows::new(vec![
        header_row,
        logs_row,
        transactions_row,
    ]))
}

fn rpc_header_to_row(header: Header<AnyHeader>) -> Result<Block, ToRowError> {
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
        mix_hash: *header.mix_hash.unwrap_or_default(),
        nonce: u64::from(header.nonce.unwrap_or_default()),
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

fn rpc_log_to_row(log: RpcLog, timestamp: Timestamp) -> Result<logs::Log, ToRowError> {
    Ok(logs::Log {
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
    tx: AnyRpcTransaction,
    receipt: AnyTxReceipt,
    tx_index: usize,
) -> Result<Transaction, ToRowError> {
    let sig = match tx.inner.inner.deref() {
        AnyTxEnvelope::Ethereum(envelope) => match envelope {
            EthereumTxEnvelope::Legacy(signed) => signed.signature(),
            EthereumTxEnvelope::Eip2930(signed) => signed.signature(),
            EthereumTxEnvelope::Eip1559(signed) => signed.signature(),
            EthereumTxEnvelope::Eip4844(signed) => signed.signature(),
            EthereumTxEnvelope::Eip7702(signed) => signed.signature(),
        },
        AnyTxEnvelope::Unknown(_) => {
            &alloy::primitives::Signature::from_raw(&[0u8; 65]).expect("invalid raw signature")
        }
    };
    Ok(Transaction {
        block_hash: block.hash,
        block_num: block.block_num,
        timestamp: block.timestamp,
        tx_index: u32::try_from(tx_index)
            .map_err(|e| ToRowError::Overflow("tx_index", e.into()))?,
        tx_hash: tx.inner.tx_hash().0,
        to: tx.to().map(|addr| addr.0.0),
        nonce: tx.nonce(),
        gas_price: TransactionResponse::gas_price(&tx.0.inner)
            .map(i128::try_from)
            .transpose()
            .map_err(|e| ToRowError::Overflow("gas_price", e.into()))?,
        gas_limit: tx.gas_limit(),
        value: i128::try_from(tx.value()).map_err(|e| ToRowError::Overflow("value", e.into()))?,
        input: tx.input().to_vec(),
        v: if sig.v() { vec![1] } else { vec![] },
        r: sig.r().to_be_bytes_vec(),
        s: sig.s().to_be_bytes_vec(),
        receipt_cumulative_gas_used: u64::try_from(receipt.inner.inner.cumulative_gas_used())
            .map_err(|e| ToRowError::Overflow("cumulative_gas_used", e.into()))?,
        r#type: tx.ty().into(),
        max_fee_per_gas: i128::try_from(tx.inner().inner.max_fee_per_gas())
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
        status: receipt.inner.inner.status().into(),
    })
}
