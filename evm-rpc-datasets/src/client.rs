use std::{
    collections::HashMap,
    mem,
    sync::{atomic, Arc},
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
        types::{Block as AlloyBlock, Header, Log as RpcLog, TransactionReceipt},
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
use histogram::Histogram;
use indicatif::ProgressStyle;
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::{error, info, instrument, warn, Span};
use tracing_indicatif::span_ext::IndicatifSpanExt;

use crate::tables::transactions::{Transaction, TransactionRowsBuilder};

#[derive(Error, Debug)]
pub enum ToRowError {
    #[error("missing field: {0}")]
    Missing(&'static str),
    #[error("overflow in field {0}: {1}")]
    Overflow(&'static str, BoxError),
}

#[derive(Debug)]
pub struct Stats {
    pub id: usize,
    pub latency_micros: Histogram,
    pub request_counter: usize,
    pub call_counter: usize,
    pub last_histogram_log: Instant,
}

impl Stats {
    pub fn new(id: usize) -> Self {
        Self {
            id,
            latency_micros: Histogram::new(5, 55).expect("Failed to create histogram"),
            request_counter: 0,
            call_counter: 0,
            last_histogram_log: Instant::now(),
        }
    }
    pub fn log_stats(&mut self) -> Result<(), BoxError> {
        let elapsed = Instant::now().duration_since(self.last_histogram_log);
        let count = self.request_counter;
        let call_count = self.call_counter;
        let avg_qps = if elapsed.as_micros() > 0 {
            count as f64 / (elapsed.as_micros() as f64 / 1_000_000.0)
        } else {
            0.0
        };
        let avg_cps = if elapsed.as_micros() > 0 {
            call_count as f64 / (elapsed.as_micros() as f64 / 1_000_000.0)
        } else {
            0.0
        };
        let mut line = format!(
            "[id={}] avg_qps={:.2} avg_cps={:.2} total_requests={} total_calls={}",
            self.id, avg_qps, avg_cps, count, call_count
        );
        for percentile in [0.5, 0.9, 0.95, 0.99] {
            if let Some(bucket) = self.latency_micros.percentile(percentile)? {
                line.push_str(&format!(
                    " p{}={}-{}µs({})",
                    (percentile * 100.0) as u32,
                    bucket.start(),
                    bucket.end(),
                    bucket.count()
                ));
            } else {
                line.push_str(&format!(" p{}=N/A", (percentile * 100.0) as u32));
            }
        }
        tracing::info!("{}", line);
        self.last_histogram_log = Instant::now();
        self.request_counter = 0;
        self.call_counter = 0;
        Ok(())
    }
}

use std::sync::atomic::{AtomicUsize, Ordering};

static BATCHING_RPC_WRAPPER_SEQ: AtomicUsize = AtomicUsize::new(1);

#[derive(Debug, Clone)]
pub struct BatchingRpcWrapper {
    client: alloy::providers::RootProvider,
    batch_size: usize,
    retries: usize,
    limiter: Arc<tokio::sync::Semaphore>,
    stats: Arc<Mutex<Stats>>,
    pub id: usize,
    url: Url,
    concurrent_requests: Arc<AtomicUsize>,
}

impl BatchingRpcWrapper {
    pub fn new(
        url: Url,
        batch_size: usize,
        retries: usize,
        limiter: Arc<tokio::sync::Semaphore>,
        concurrent_requests: Arc<AtomicUsize>,
    ) -> Self {
        let id = BATCHING_RPC_WRAPPER_SEQ.fetch_add(1, Ordering::Relaxed);
        let stats = Arc::new(Mutex::new(Stats::new(id)));
        let stats_clone = stats.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                ticker.tick().await;
                let mut stats = stats_clone.lock().await;
                if let Err(e) = stats.log_stats() {
                    tracing::warn!("Failed to log stats: {e}");
                }
            }
        });
        let client = alloy::providers::RootProvider::new_http(url.clone());
        Self {
            client,
            batch_size,
            retries,
            limiter,
            stats,
            id,
            url,
            concurrent_requests,
        }
    }

    /// Execute a batch of RPC calls with retries on failure.
    /// calls: &[(&'static str, Params)] - a slice of tuples containing the method name and parameters.
    pub async fn execute<T: RpcRecv, Params: RpcSend>(
        &self,
        calls: &[(&'static str, Params)],
    ) -> Result<Vec<T>, BoxError>
    where
        Params: Clone,
    {
        if calls.is_empty() {
            return Ok(Vec::new());
        }
        let mut results = Vec::new();
        let mut remaining_attempts = self.retries;

        self.concurrent_requests.fetch_add(1, Ordering::SeqCst);

        for chunk in calls.chunks(self.batch_size) {
            // Acquire semaphore permit for the batch, which will be one request
            // let _permit = self.limiter.acquire().await?;
            let mut batch = BatchRequest::new(self.client.client());
            let mut waiters = Vec::new();

            for (method, params) in chunk.iter() {
                waiters.push(batch.add_call(*method, &params)?);
            }
            // Increment call_counter for this batch
            {
                let mut stats = self.stats.lock().await;
                stats.call_counter += chunk.len();
            }

            let stats = self.stats.clone();
            let batch_then_waiters = async move {
                let start = Instant::now();
                batch.send().await?;
                let responses = try_join_all(waiters).await?;
                let mut stats = stats.lock().await;
                stats
                    .latency_micros
                    .increment(start.elapsed().as_micros() as u64)
                    .expect("Failed to record latency in histogram");
                stats.request_counter += 1;
                Span::current().pb_inc(responses.len() as u64);
                Ok::<_, BoxError>(responses)
            };

            match batch_then_waiters.await {
                Ok(responses) => {
                    results.extend(responses);
                }
                Err(e) if remaining_attempts > 0 && self.batch_size > 1 => {
                    // Retry each call in the chunk individually
                    self.request_batch_individually(
                        &mut results,
                        remaining_attempts,
                        &chunk.to_vec(),
                        e,
                    )
                    .await?;
                    remaining_attempts -= 1;
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }

        self.concurrent_requests.fetch_sub(1, Ordering::SeqCst);
        Ok(results)
    }

    /// If a batch fails, try each call individually to isolate the failure for debugging.
    #[tracing::instrument(skip(self, results, chunk, e))]
    async fn request_batch_individually<T: RpcRecv, Params: RpcSend>(
        &self,
        results: &mut Vec<T>,
        remaining_attempts: usize,
        chunk: &Vec<(&'static str, Params)>,
        e: BoxError,
    ) -> Result<(), BoxError> {
        let delay_ms = 500 * (self.retries - remaining_attempts + 1) as u64;
        warn!(
            "Batch failed. Error({:?}) Batch size {}. Retries left: {}, will wait for {}ms before retrying",
            e, self.batch_size, remaining_attempts, delay_ms
        );
        let retry_span = tracing::Span::current();
        retry_span.pb_set_style(&indicatif::ProgressStyle::default_bar()
            .template("Retry {spinner:.yellow} [{elapsed_precise}] [{bar:40.yellow/blue}] {pos}/{len} ({percent_precise}%)")
            .unwrap()
            .progress_chars("#>-")
        );
        retry_span.pb_set_length(chunk.len() as u64);
        for (method, params) in chunk {
            tracing::info!(
                "Retrying {} with params {:?} after error: {}",
                method,
                params,
                e
            );
            let _permit = self.limiter.acquire().await?;
            let result = self.client.client().request(*method, params).await;
            match result {
                Ok(response) => {
                    results.push(response);
                    let mut stats = self.stats.lock().await;
                    stats.request_counter += 1;
                    tracing::Span::current().pb_inc(1);
                }
                Err(err) => {
                    error!(
                        "Error executing {} with params {:?}: {}",
                        method, params, err
                    );
                    tracing::Span::current().pb_set_message("done (error)");
                    return Err(err.into());
                }
            }
        }
        tracing::Span::current().pb_set_message("done");
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        Ok(())
    }
}

#[derive(Clone)]
pub struct JsonRpcClient {
    client: alloy::providers::RootProvider,
    url: Url,
    network: String,
    limiter: Arc<tokio::sync::Semaphore>,
    batch_size: usize,
    concurrent_requests: Arc<atomic::AtomicUsize>,
}

impl JsonRpcClient {
    pub fn new(
        url: Url,
        network: String,
        request_limit: u16,
        batch_size: usize,
    ) -> Result<Self, BoxError> {
        assert!(request_limit >= 1);
        let client = alloy::providers::RootProvider::new_http(url.clone());
        let limiter = tokio::sync::Semaphore::new(request_limit as usize).into();
        let concurrent_requests = Arc::new(atomic::AtomicUsize::new(0 as usize));
        Ok(Self {
            client,
            url,
            network,
            limiter,
            batch_size,
            concurrent_requests,
        })
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
        stream! {
            for block_num in start_block..=end_block {
                let client_permit = self.limiter.acquire().await;
                let block_result = self
                    .client
                    .get_block_by_number(BlockNumberOrTag::Number(block_num))
                    .full()
                    .await;

                let block = match block_result {
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
        let block_calls: Vec<_> = (start_block..=end_block)
            .collect::<Vec<_>>()
            .chunks(self.batch_size * 10)
            .map(<[_]>::to_vec)
            .map(|calls| BlockBatch::new(*calls.first().unwrap(), *calls.last().unwrap(), calls))
            .collect();

        info!(batch_size = self.batch_size);
        let batching_client = BatchingRpcWrapper::new(
            self.url.clone(),
            self.batch_size,
            10,
            self.limiter.clone(),
            self.concurrent_requests.clone(),
        );
        info!(permits = self.limiter.available_permits());
        let network = self.network.clone();
        stream! {
            for batch in block_calls {
                match fetch_block_batch(&batching_client, batch, &network).await {
                    Ok(rows_vec) => {
                        for row in rows_vec {
                            yield Ok(row);
                        }
                    }
                    Err(err) => {
                        yield Err(err);
                        return;
                    }
                }
            }
        }
    }
}

type BlockBatch = Batch<(BlockNumberOrTag, bool)>;

impl BlockBatch {
    pub fn new(start_block: u64, end_block: u64, calls: Vec<u64>) -> Self {
        Self {
            start_block,
            end_block,
            calls: calls
                .into_iter()
                .map(|block_num| {
                    (
                        "eth_getBlockByNumber",
                        (BlockNumberOrTag::Number(block_num), true),
                    )
                })
                .collect(),
        }
    }
}

struct Batch<Params: RpcSend> {
    start_block: u64,
    end_block: u64,
    calls: Vec<(&'static str, Params)>,
}

#[tracing::instrument(skip(batching_client, block_batch, network),
fields(
    id = batching_client.id,
    start_block = block_batch.start_block,
    end_block = block_batch.end_block,
    total_blocks = block_batch.calls.len()
))]
pub async fn fetch_block_batch(
    batching_client: &BatchingRpcWrapper,
    block_batch: BlockBatch,
    network: &str,
) -> Result<Vec<RawDatasetRows>, BoxError> {
    let span = tracing::Span::current();
    span.pb_set_length(block_batch.calls.len() as u64);
    span.pb_set_style(
        &ProgressStyle::default_bar()
            .template(
                "Block batch [{elapsed_precise}] [{bar:40.yellow/blue}] {pos}/{len} ({percent_precise}%) {span_fields}",
            )
            .unwrap()
            .progress_chars("#>-"),
    );
    let blocks: Vec<AlloyBlock> = batching_client.execute(&block_batch.calls).await?;
    let span = tracing::Span::current();
    span.pb_inc(blocks.len() as u64);
    let mut results = Vec::new();

    if !blocks.is_empty() {
        let block_receipts_map = fetch_block_receipts(
            batching_client.clone(),
            &blocks,
        )
        .await?;
        
        // Process blocks with their receipts
        for block in blocks.into_iter() {
            let block_receipts = block_receipts_map
                .get(&block.header.number)
                .cloned()
                .unwrap_or_default();
            results.push(rpc_to_rows(block, block_receipts, network)?);
        }
    }
    span.pb_inc(results.len() as u64);
    Ok(results)
}

#[tracing::instrument(skip(batching_client, blocks), fields(
    total_blocks = blocks.len(),
    total_receipts = blocks.iter().map(|b| b.transactions.len()).sum::<usize>()
))]
async fn fetch_block_receipts(
    batching_client: BatchingRpcWrapper,
    blocks: &[AlloyBlock],
) -> Result<HashMap<u64, Vec<Option<TransactionReceipt>>>, BoxError> {
    let span = Span::current();
    
    // Count total expected receipts across all blocks
    let total_expected_receipts: usize = blocks
        .iter()
        .map(|block| block.transactions.len())
        .sum();
    
    span.pb_set_style(
        &ProgressStyle::default_bar()
            .template("Block Receipts [{elapsed_precise}] [{bar:40.green/blue}] {pos}/{len} receipts ({percent_precise}%) | {msg}")
            .unwrap()
            .progress_chars("#>-"),
    );
    span.pb_set_length(total_expected_receipts as u64);
    span.pb_set_message(&format!("Blocks: 0/{} | Starting...", blocks.len()));

    // Intelligently batch blocks by transaction count to optimize batch sizes
    let target_receipts_per_batch = 1000; // Target ~1000 receipts per batch
    let mut block_receipts_map = HashMap::new();
    let mut processed_receipts = 0usize;
    let mut processed_blocks = 0usize;
    
    // Group blocks into batches by transaction count
    let mut current_batch = Vec::new();
    let mut current_batch_receipt_count = 0;
    
    for block in blocks.iter() {
        let block_tx_count = block.transactions.len();
        
        // If adding this block would exceed our target, process current batch first
        if !current_batch.is_empty() && 
           (current_batch_receipt_count + block_tx_count > target_receipts_per_batch) {
            
            // Process current batch
            let batch_results = process_block_receipt_batch(
                &batching_client,
                &current_batch,
                &mut processed_receipts,
                &mut processed_blocks,
                blocks.len(),
                &span,
            ).await?;
            
            // Add results to map
            for (block_num, receipts) in batch_results {
                block_receipts_map.insert(block_num, receipts);
            }
            
            // Reset batch
            current_batch.clear();
            current_batch_receipt_count = 0;
        }
        
        // Add current block to batch
        current_batch.push(block);
        current_batch_receipt_count += block_tx_count;
    }
    
    // Process final batch if any blocks remain
    if !current_batch.is_empty() {
        let batch_results = process_block_receipt_batch(
            &batching_client,
            &current_batch,
            &mut processed_receipts,
            &mut processed_blocks,
            blocks.len(),
            &span,
        ).await?;
        
        // Add results to map
        for (block_num, receipts) in batch_results {
            block_receipts_map.insert(block_num, receipts);
        }
    }

    span.pb_set_message(&format!(
        "Completed: {}/{} blocks, {} total receipts", 
        processed_blocks, 
        blocks.len(), 
        processed_receipts
    ));
    Ok(block_receipts_map)
}

async fn process_block_receipt_batch(
    batching_client: &BatchingRpcWrapper,
    blocks_in_batch: &[&AlloyBlock],
    processed_receipts: &mut usize,
    processed_blocks: &mut usize,
    total_blocks: usize,
    span: &Span,
) -> Result<Vec<(u64, Vec<Option<TransactionReceipt>>)>, BoxError> {
    // Create eth_getBlockReceipts calls for this batch
    let block_receipt_calls: Vec<_> = blocks_in_batch
        .iter()
        .map(|block| {
            (
                "eth_getBlockReceipts",
                [format!("0x{:x}", block.header.number)],
            )
        })
        .collect();

    // Execute batch of block receipt requests
    let all_block_receipts: Vec<Vec<TransactionReceipt>> = batching_client
        .execute(&block_receipt_calls)
        .await?;

    // Process results and update progress
    let mut batch_results = Vec::new();
    
    for (block, receipts) in blocks_in_batch.iter().zip(all_block_receipts.into_iter()) {
        let block_num = block.header.number;
        let receipts_count = receipts.len();
        
        let receipts_with_options: Vec<Option<TransactionReceipt>> = receipts
            .into_iter()
            .map(Some)
            .collect();
        
        // Update progress counters
        *processed_receipts += receipts_count;
        *processed_blocks += 1;
        
        // Update progress message with detailed information
        span.pb_inc(receipts_count as u64);
        span.pb_set_message(&format!(
            "Blocks: {}/{} | Block {} ({} txns) | Batch: {} blocks", 
            *processed_blocks, 
            total_blocks, 
            block_num, 
            receipts_count,
            blocks_in_batch.len()
        ));
        
        batch_results.push((block_num, receipts_with_options));
    }
    
    Ok(batch_results)
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
