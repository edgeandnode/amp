mod client;
mod diff;
mod rpc;

use std::{
    collections::{BTreeMap, VecDeque},
    ops::RangeInclusive,
    sync::{
        Arc,
        atomic::{self, AtomicU64},
    },
};

use alloy::primitives::BlockNumber;
use anyhow::{Context as _, anyhow};
use arrow_flight::{
    flight_service_client::FlightServiceClient, sql::client::FlightSqlServiceClient,
};
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::Url;
use tokio::sync::Mutex;
use tonic::transport::{ClientTlsConfig, Endpoint};

use crate::client::{Block, Log, fetch_blocks};

#[derive(clap::Parser, Debug)]
#[command(name = "verify", long_about = None)]
pub struct Args {
    #[arg(long, default_value = "http://localhost:1602")]
    pub amp_url: String,
    #[arg(long, default_value = "")]
    pub amp_auth: String,
    #[arg(long, default_value = "edgeandnode/ethereum_mainnet")]
    pub dataset: String,
    #[arg(long, default_value = "0")]
    pub start_block: BlockNumber,
    #[arg(long)]
    pub end_block: BlockNumber,
    #[arg(long, default_value = "1")]
    pub parallelism: usize,
    /// Optional RPC URL for fallback verification when block verification fails.
    /// When provided, failed blocks will be fetched from this RPC endpoint and re-verified.
    #[arg(long)]
    pub rpc_url: Option<Url>,
}

pub async fn run(args: Args) -> anyhow::Result<()> {
    const BATCH_SIZE: u64 = 8192;

    let block_range = args.start_block..=args.end_block;
    let total_blocks = block_range.end() - block_range.start() + 1;

    let batches: VecDeque<RangeInclusive<BlockNumber>> = block_range
        .clone()
        .step_by(BATCH_SIZE as usize)
        .map(|start| start..=u64::min(start + BATCH_SIZE - 1, *block_range.end()))
        .collect();

    let progress_counter = Arc::new(AtomicU64::new(0));
    let progress_bar = ProgressBar::new(total_blocks);
    progress_bar.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} blocks ({percent}%)")
            // SAFETY: Template string is a compile-time constant and cannot fail
            .expect("progress bar template is valid")
            .progress_chars("=> "),
    );

    let work_queue = Arc::new(Mutex::new(batches));

    let mut worker_handles = Vec::new();
    for worker_id in 0..args.parallelism {
        let work_queue = Arc::clone(&work_queue);
        let amp_url = args.amp_url.clone();
        let amp_auth = args.amp_auth.clone();
        let dataset = args.dataset.clone();
        let rpc_url = args.rpc_url.clone();
        let progress_counter = Arc::clone(&progress_counter);

        let handle = tokio::spawn(async move {
            let mut amp = create_amp_client(&amp_url, &amp_auth)
                .await
                .context(anyhow!("worker {worker_id}: failed to create AMP client"))?;

            loop {
                let batch_range = {
                    let mut queue = work_queue.lock().await;
                    match queue.pop_front() {
                        Some(range) => range,
                        None => break,
                    }
                };

                fetch_and_verify_batch(
                    &mut amp,
                    &dataset,
                    &batch_range,
                    &progress_counter,
                    rpc_url.as_ref(),
                )
                .await
                .context(anyhow!(
                    "worker {worker_id} failed to verify range {batch_range:?}"
                ))?;
            }

            Ok::<(), anyhow::Error>(())
        });

        worker_handles.push(handle);
    }

    let progress_monitor = tokio::spawn({
        let progress_counter = Arc::clone(&progress_counter);
        let progress_bar = progress_bar.clone();
        async move {
            loop {
                let current = progress_counter.load(atomic::Ordering::Acquire);
                progress_bar.set_position(current);

                if current >= total_blocks {
                    break;
                }

                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }
    });

    let success_progress_style = ProgressStyle::default_bar()
        .template("[{elapsed_precise}] [{bar:40.green/green}] {pos}/{len} blocks ({percent}%)")
        // SAFETY: Template string is a compile-time constant and cannot fail
        .expect("progress bar template is valid")
        .progress_chars("=> ");
    let error_progress_style = ProgressStyle::default_bar()
        .template("[{elapsed_precise}] [{bar:40.red/red}]")
        // SAFETY: Template string is a compile-time constant and cannot fail
        .expect("progress bar template is valid")
        .progress_chars("=> ");

    for handle in worker_handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                progress_monitor.abort();
                progress_bar.set_style(error_progress_style);
                progress_bar.finish();
                return Err(err);
            }
            Err(join_err) => {
                progress_monitor.abort();
                progress_bar.set_style(error_progress_style);
                progress_bar.finish();
                return Err(anyhow!("worker task panicked: {}", join_err));
            }
        }
    }

    let _ = progress_monitor.await;
    progress_bar.set_style(success_progress_style);
    progress_bar.finish();
    println!("Successfully verified blocks {block_range:?}");

    Ok(())
}

async fn create_amp_client(amp_url: &str, amp_auth: &str) -> anyhow::Result<amp_client::AmpClient> {
    let mut endpoint: Endpoint = amp_url.parse()?;
    if endpoint.uri().scheme_str() == Some("https") {
        endpoint = endpoint.tls_config(ClientTlsConfig::new().with_native_roots())?;
    }
    let channel = endpoint.connect().await?;
    let flight_client =
        FlightServiceClient::new(channel).max_decoding_message_size(16 * 1024 * 1024);
    let flight_sql_client = FlightSqlServiceClient::new_from_inner(flight_client);
    let mut amp = amp_client::AmpClient::from_client(flight_sql_client);
    amp.set_token(amp_auth.to_string());
    Ok(amp)
}

async fn fetch_and_verify_batch(
    amp: &mut amp_client::AmpClient,
    dataset: &str,
    range: &RangeInclusive<BlockNumber>,
    progress_counter: &Arc<AtomicU64>,
    rpc_url: Option<&Url>,
) -> anyhow::Result<()> {
    let blocks = fetch_blocks(amp, dataset, range.clone()).await?;

    for amp_block in blocks {
        let block_number = amp_block.number;
        let verification_result = tokio::task::spawn_blocking(move || {
            let result = verify_block(&amp_block);
            (result, amp_block)
        })
        .await
        .context(anyhow!("blocking task panicked for block {}", block_number))?;

        match verification_result {
            (Ok(_), _) => {}
            (Err(err), _) if rpc_url.is_none() => {
                return Err(err).context(anyhow!("verify block {}", block_number));
            }
            (Err(err), amp_block) => {
                let Some(rpc_url) = rpc_url else {
                    unreachable!("rpc_url is Some due to previous match arm");
                };
                let rpc_verification_message =
                    match fetch_and_verify_rpc_block(rpc_url, block_number).await {
                        Ok(rpc_block) => diff::create_block_diff(&amp_block, &rpc_block),
                        Err(err) => format!("RPC verification also failed: {err:#}"),
                    };
                return Err(err
                    .context(rpc_verification_message)
                    .context(anyhow!("verify block {}", block_number)));
            }
        }

        progress_counter.fetch_add(1, atomic::Ordering::Release);
    }

    Ok(())
}

fn verify_block(block: &Block) -> anyhow::Result<()> {
    let computed_block_hash = alloy::consensus::Header::from(block).hash_slow();
    anyhow::ensure!(computed_block_hash == block.hash);

    verify_transactions_root(block).context("transactions root")?;
    verify_receipts_root(block).context("receipts root (logs)")?;

    Ok(())
}

fn verify_transactions_root(block: &Block) -> anyhow::Result<()> {
    let mut tx_envelopes = Vec::with_capacity(block.transactions.len());
    for tx in &block.transactions {
        tx_envelopes.push(tx.to_tx_envelope()?);
    }

    let transactions_root = alloy::consensus::proofs::calculate_transaction_root(&tx_envelopes);
    anyhow::ensure!(
        transactions_root == block.transactions_root,
        "computed transactions root does not match transactions_root from block header",
    );

    Ok(())
}

fn verify_receipts_root(block: &Block) -> anyhow::Result<()> {
    let mut logs_by_tx: BTreeMap<u32, Vec<&Log>> = Default::default();
    for log in &block.logs {
        logs_by_tx.entry(log.tx_index).or_default().push(log);
    }

    let mut receipts = Vec::with_capacity(block.transactions.len());
    for tx in &block.transactions {
        // Note: tx.gas_used is already cumulative
        let cumulative_gas_used = tx.gas_used;

        let tx_logs = logs_by_tx
            .get(&tx.tx_index)
            .map(|logs| {
                logs.iter()
                    .map(|log| alloy::primitives::Log {
                        address: log.address,
                        data: alloy::primitives::LogData::new_unchecked(
                            log.topics.clone(),
                            log.data.clone(),
                        ),
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        // Pre-Byzantium: state_root is populated, use PostState
        // Post-Byzantium: state_root is null, use Eip658(status)
        let status = if let Some(state_root) = tx.state_root {
            alloy::consensus::Eip658Value::PostState(state_root)
        } else {
            alloy::consensus::Eip658Value::Eip658(tx.status)
        };

        let receipt = alloy::consensus::Receipt {
            status,
            cumulative_gas_used,
            logs: tx_logs,
        };

        let receipt_envelope = match tx.tx_type {
            0 => alloy::consensus::ReceiptEnvelope::Legacy(receipt.with_bloom()),
            1 => alloy::consensus::ReceiptEnvelope::Eip2930(receipt.with_bloom()),
            2 => alloy::consensus::ReceiptEnvelope::Eip1559(receipt.with_bloom()),
            3 => alloy::consensus::ReceiptEnvelope::Eip4844(receipt.with_bloom()),
            4 => alloy::consensus::ReceiptEnvelope::Eip7702(receipt.with_bloom()),
            _ => anyhow::bail!("unsupported transaction type: {}", tx.tx_type),
        };
        receipts.push(receipt_envelope);
    }

    let receipts_root = alloy::consensus::proofs::calculate_receipt_root(&receipts);
    anyhow::ensure!(
        receipts_root == block.receipts_root,
        "computed receipts root does not match receipts_root from block header",
    );

    Ok(())
}

async fn fetch_and_verify_rpc_block(
    rpc_url: &Url,
    block_number: BlockNumber,
) -> anyhow::Result<Block> {
    let block = rpc::fetch_rpc_block(rpc_url, block_number)
        .await
        .context("fetch block from RPC")?;
    verify_block(&block).context("verify block from RPC")?;
    Ok(block)
}

// TODO: verify epochs
// #[expect(dead_code)]
// fn verify_pre_merge_epoch(headers: &[Block]) -> anyhow::Result<()> {
//     let epoch_records: Vec<vee::ExtHeaderRecord> = headers
//         .iter()
//         .map(|header| vee::ExtHeaderRecord {
//             block_number: vee::era_validation::BlockNumber(header.number),
//             block_hash: header.hash,
//             total_difficulty: header.total_difficulty.unwrap_or_default(),
//             full_header: None,
//         })
//         .collect();
//     let epoch = vee::Epoch::try_from(epoch_records).context("validate epoch")?;
//     let validator = vee::EraValidator::default();
//     validator.validate_single_epoch(&epoch)?;
//     Ok(())
//     // TODO: check that epoch blocks are adjacent in canonical chain?
// }
