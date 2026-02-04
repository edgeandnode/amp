//! The program streams blocks from OF1 for a given epoch, fetches the same blocks via JSON-RPC,
//! and compares the results at the [solana_datasets::tables::NonEmptySlot] level.

use std::{path::PathBuf, sync::Arc};

use anyhow::Context;
use clap::Parser;
use futures::StreamExt;
use solana_clock::Slot;
use solana_datasets::{
    ProviderConfig, non_empty_of1_slot, non_empty_rpc_slot, of1_client, rpc_client,
};

const SLOT_MISMATCH_LIMIT: u8 = 10;

#[derive(Parser)]
#[command(name = "solana-compare")]
struct Cli {
    /// Epoch number to process.
    #[arg(long)]
    epoch: u64,

    /// Path to provider config TOML.
    #[arg(long)]
    provider_config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    let provider_toml =
        fs_err::read_to_string(&cli.provider_config).context("reading provider config")?;
    let provider_cfg: ProviderConfig =
        toml::from_str(&provider_toml).context("deserializing provider config")?;

    let slots_per_epoch = solana_clock::DEFAULT_SLOTS_PER_EPOCH;
    let start_slot = cli.epoch * slots_per_epoch;
    let end_slot = (cli.epoch + 1) * slots_per_epoch - 1;

    tracing::info!( epoch = %cli.epoch, start_slot, end_slot, "running OF1 vs RPC comparison");

    let (car_manager_tx, car_manager_rx) = tokio::sync::mpsc::channel(128);

    let car_manager_jh = tokio::task::spawn(of1_client::car_file_manager(
        car_manager_rx,
        provider_cfg.of1_car_directory.clone(),
        provider_cfg.keep_of1_car_files,
        provider_cfg.name.clone(),
        provider_cfg.network.clone(),
        None,
    ));

    let rpc_client = Arc::new(rpc_client::SolanaRpcClient::new(
        provider_cfg.rpc_provider_url,
        provider_cfg.max_rpc_calls_per_second,
        provider_cfg.name.clone(),
        provider_cfg.network.clone(),
    ));

    let get_block_config = rpc_client::rpc_config::RpcBlockConfig {
        encoding: Some(rpc_client::rpc_config::UiTransactionEncoding::Json),
        transaction_details: Some(rpc_client::rpc_config::TransactionDetails::Full),
        max_supported_transaction_version: Some(0),
        rewards: Some(true),
        commitment: Some(rpc_client::rpc_config::CommitmentConfig::finalized()),
    };

    let of1_stream = of1_client::stream(
        start_slot,
        end_slot,
        provider_cfg.of1_car_directory,
        car_manager_tx.clone(),
        rpc_client.clone(),
        get_block_config,
        None,
    );

    let mut expected_slot_num = start_slot;
    let mut mismatched_slots = 0u8;

    {
        let mut of1_stream_pin = std::pin::pin!(of1_stream);
        'blocks: while let Some(of1_result) = of1_stream_pin.next().await {
            let of1_decoded = match of1_result {
                Ok(slot) => slot,
                Err(e) => {
                    tracing::error!(error = %e, "error streaming from OF1");
                    break;
                }
            };

            let slot_num = of1_decoded.slot;

            let slots_match = 'slots_match: {
                if slot_num != expected_slot_num {
                    // If OF1 stream skipped slots, make sure those slots are also missing via RPC.
                    for skipped_slot in expected_slot_num..slot_num {
                        match rpc_client.get_block(slot_num, get_block_config, None).await {
                            Ok(_) => {
                                tracing::warn!(
                                    slot = skipped_slot,
                                    "slot missing in OF1 but present via RPC"
                                );
                                break 'slots_match false;
                            }
                            Err(e) => {
                                if !rpc_client::is_block_missing_err(&e) {
                                    tracing::error!(slot = skipped_slot, error = %e, "error fetching block via RPC");
                                    break 'blocks;
                                }
                            }
                        }
                    }
                }

                let rpc_block = match rpc_client.get_block(slot_num, get_block_config, None).await {
                    Ok(block) => block,
                    Err(e) => {
                        if rpc_client::is_block_missing_err(&e) {
                            tracing::warn!(
                                slot = slot_num,
                                "slot missing via RPC but present in OF1"
                            );
                            break 'slots_match false;
                        } else {
                            tracing::error!(slot = slot_num, error = %e, "error fetching block via RPC");
                            break 'blocks;
                        }
                    }
                };

                let of1_slot = match non_empty_of1_slot(of1_decoded) {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::error!(slot = slot_num, error = %e, "OF1 conversion error");
                        break 'blocks;
                    }
                };

                let rpc_slot = match non_empty_rpc_slot(slot_num, rpc_block) {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::error!(slot = slot_num, error = %e, "RPC conversion error");
                        break 'blocks;
                    }
                };

                slots_match(slot_num, of1_slot, rpc_slot)
            };

            if !slots_match {
                mismatched_slots += 1;

                if mismatched_slots == SLOT_MISMATCH_LIMIT {
                    tracing::warn!("too many mismatched slots, aborting comparison");
                    break 'blocks;
                }
            }

            expected_slot_num = slot_num + 1;
        }
    }

    drop(car_manager_tx);
    if let Err(e) = car_manager_jh.await {
        tracing::error!(error = %e, "car file manager task failed");
    }

    tracing::info!("comparison complete");

    Ok(())
}

/// Compare two `NonEmptySlot`s and log any differences. The comparison is done piecewise for more
/// focused error logging.
fn slots_match(
    slot_num: Slot,
    of1_slot: solana_datasets::tables::NonEmptySlot,
    rpc_slot: solana_datasets::tables::NonEmptySlot,
) -> bool {
    if of1_slot.slot != rpc_slot.slot {
        tracing::warn!(
            slot = %slot_num,
            of1_slot = %of1_slot.slot,
            rpc_slot = %rpc_slot.slot,
            "slot mismatch"
        );
        return false;
    }

    if of1_slot.parent_slot != rpc_slot.parent_slot {
        tracing::warn!(
            slot = %slot_num,
            of1_parent_slot = %of1_slot.parent_slot,
            rpc_parent_slot = %rpc_slot.parent_slot,
            "parent slot mismatch"
        );
        return false;
    }

    if of1_slot.blockhash != rpc_slot.blockhash {
        tracing::warn!(
            slot = %slot_num,
            of1_blockhash = ?of1_slot.blockhash,
            rpc_blockhash = ?rpc_slot.blockhash,
            "blockhash mismatch"
        );
        return false;
    }

    if of1_slot.prev_blockhash != rpc_slot.prev_blockhash {
        tracing::warn!(
            slot = %slot_num,
            of1_prev_blockhash = ?of1_slot.prev_blockhash,
            rpc_prev_blockhash = ?rpc_slot.prev_blockhash,
            "previous blockhash mismatch"
        );
        return false;
    }

    if of1_slot.block_height != rpc_slot.block_height {
        tracing::warn!(
            slot = %slot_num,
            of1_block_height = ?of1_slot.block_height,
            rpc_block_height = ?rpc_slot.block_height,
            "block height mismatch"
        );
        return false;
    }

    if of1_slot.blocktime != rpc_slot.blocktime {
        tracing::warn!(
            slot = %slot_num,
            of1_blocktime = ?of1_slot.blocktime,
            rpc_blocktime = ?rpc_slot.blocktime,
            "blocktime mismatch"
        );
        return false;
    }

    if of1_slot.transactions.len() != rpc_slot.transactions.len() {
        tracing::warn!(
            %slot_num,
            of1_tx_count = %of1_slot.transactions.len(),
            rpc_tx_count = %rpc_slot.transactions.len(),
            "transaction count mismatch"
        );
        return false;
    }

    for (tx_index, (of1_tx, rpc_tx)) in of1_slot
        .transactions
        .iter()
        .zip(rpc_slot.transactions.iter())
        .enumerate()
    {
        if of1_tx != rpc_tx {
            let cmp = pretty_assertions::Comparison::new(&of1_tx, &rpc_tx);
            tracing::warn!(
                %slot_num,
                %tx_index,
                %cmp,
                "transaction mismatch"
            );
            return false;
        }
    }

    if of1_slot.messages.len() != rpc_slot.messages.len() {
        tracing::warn!(
            %slot_num,
            of1_msg_count = %of1_slot.messages.len(),
            rpc_msg_count = %rpc_slot.messages.len(),
            "message count mismatch"
        );
        return false;
    }

    for (msg_index, (of1_msg, rpc_msg)) in of1_slot
        .messages
        .iter()
        .zip(rpc_slot.messages.iter())
        .enumerate()
    {
        if of1_msg != rpc_msg {
            let cmp = pretty_assertions::Comparison::new(&of1_msg, &rpc_msg);
            tracing::warn!(
                %slot_num,
                %msg_index,
                %cmp,
                "message mismatch"
            );
            return false;
        }
    }

    true
}
