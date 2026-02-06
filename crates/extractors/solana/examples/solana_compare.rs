//! The program streams blocks from OF1 for a given epoch, fetches the same blocks via JSON-RPC,
//! and compares the results at the [solana_datasets::tables::NonEmptySlot] level.

use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::Context;
use backon::{ExponentialBuilder, Retryable};
use clap::Parser;
use futures::StreamExt;
use solana_clock::Slot;
use solana_datasets::{
    ProviderConfig, non_empty_of1_slot, non_empty_rpc_slot, of1_client, rpc_client, tables,
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
                    // OF1 stream skipped slots, make sure those slots are also missing via RPC.
                    for skipped_slot in expected_slot_num..slot_num {
                        match get_block_with_retry(&rpc_client, skipped_slot, get_block_config)
                            .await
                        {
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

                let rpc_block = match get_block_with_retry(&rpc_client, slot_num, get_block_config)
                    .await
                {
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
            if slot_num.is_multiple_of(1000) {
                tracing::info!(slot = slot_num, "progress");
            }
        }
    }

    drop(car_manager_tx);
    if let Err(e) = car_manager_jh.await {
        tracing::error!(error = %e, "car file manager task failed");
    }

    tracing::info!("comparison complete");

    Ok(())
}

async fn get_block_with_retry(
    rpc_client: &rpc_client::SolanaRpcClient,
    slot: Slot,
    config: rpc_client::rpc_config::RpcBlockConfig,
) -> rpc_client::client_error::Result<rpc_client::UiConfirmedBlock> {
    (|| async { rpc_client.get_block(slot, config, None).await })
        .retry(ExponentialBuilder::default())
        .sleep(tokio::time::sleep)
        .when(|e| !rpc_client::is_block_missing_err(e))
        .notify(|error: &rpc_client::client_error::Error, delay: Duration| {
            tracing::warn!(
                wait_ms = delay.as_millis(),
                %slot,
                %error,
                "retrying RPC get_block"
            );
        })
        .await
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

    match (of1_slot.blocktime, rpc_slot.blocktime) {
        // TODO(known-mismatch): OF1 blocktimes for early epochs are missing while
        // JSON-RPC blocktimes for the same epochs are present.
        (None, Some(_)) => {}
        // TODO(known-mismatch): OF1 blocktimes for early epochs are `0` while
        // JSON-RPC blocktimes for the same epochs are non-zero.
        (Some(0), Some(rpc_time)) if rpc_time != 0 => {}
        (Some(of1_time), Some(rpc_time)) if of1_time != rpc_time => {
            tracing::warn!(
                slot = %slot_num,
                of1_blocktime = %of1_time,
                rpc_blocktime = %rpc_time,
                "block time mismatch"
            );
            return false;
        }
        _ => {}
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
        match (
            of1_tx.transaction_status_meta.as_ref(),
            rpc_tx.transaction_status_meta.as_ref(),
        ) {
            // TODO(known-mismatch): Some OF1 transactions have the following error:
            //
            // `TransactionError::Instruction(_, InstructionError::Custom(0))`
            //
            // while the same JSON-RPC transactions show no error.
            (Some(of1_tx_meta), Some(rpc_tx_meta)) if of1_tx_meta.status != rpc_tx_meta.status => {
                continue;
            }
            _ => {}
        }

        if of1_tx != rpc_tx {
            // TODO(known-mismatch)
            let known_mismatch = known_tx_ui_amount_mismatch(
                of1_tx.transaction_status_meta.as_ref(),
                rpc_tx.transaction_status_meta.as_ref(),
            ) || known_tx_reward_mismatch(
                of1_tx.transaction_status_meta.as_ref(),
                rpc_tx.transaction_status_meta.as_ref(),
            );

            if !known_mismatch {
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

    if of1_slot.block_rewards.rewards.len() != rpc_slot.block_rewards.rewards.len() {
        // TODO(known-mismatch): some RPC providers could have pruned block rewards
        // for older slots while OF1 has the rewards for the same slots,
        // resulting in a mismatch in block reward count.
        let known_mismatch = rpc_slot.block_rewards.rewards.is_empty();

        if !known_mismatch {
            tracing::warn!(
                %slot_num,
                of1_rewards_count = %of1_slot.block_rewards.rewards.len(),
                rpc_rewards_count = %rpc_slot.block_rewards.rewards.len(),
                "block reward count mismatch"
            );
            return false;
        }
    }

    for (reward_index, (of1_reward, rpc_reward)) in of1_slot
        .block_rewards
        .rewards
        .iter()
        .zip(rpc_slot.block_rewards.rewards.iter())
        .enumerate()
    {
        if of1_reward != rpc_reward {
            let cmp = pretty_assertions::Comparison::new(&of1_reward, &rpc_reward);
            tracing::warn!(
                %slot_num,
                %reward_index,
                %cmp,
                "block reward mismatch"
            );
            return false;
        }
    }

    true
}

/// Checks for a known mismatch (in value, by a small delta) in the `ui_amount`
/// field of token balances in transaction status metadata and returns `true` if
/// found. For any other kind of mismatch (or no mismatch), returns `false`.
fn known_tx_ui_amount_mismatch(
    of1_tx_meta: Option<&tables::transactions::TransactionStatusMeta>,
    rpc_tx_meta: Option<&tables::transactions::TransactionStatusMeta>,
) -> bool {
    fn known_ui_amount_mismatch_in(
        of1_tok_balances: &[tables::transactions::TransactionTokenBalance],
        rpc_tok_balances: &[tables::transactions::TransactionTokenBalance],
    ) -> bool {
        for (of1_balance, rpc_balance) in of1_tok_balances.iter().zip(rpc_tok_balances.iter()) {
            let (Some(of1_amount), Some(rpc_amount)) = (
                of1_balance.ui_token_amount.ui_amount,
                rpc_balance.ui_token_amount.ui_amount,
            ) else {
                continue;
            };

            let delta = (of1_amount - rpc_amount).abs();
            if delta > 0.0 && delta < 0.0001 {
                return true;
            }
        }

        false
    }

    let (Some(of1_tx_meta), Some(rpc_tx_meta)) = (of1_tx_meta, rpc_tx_meta) else {
        return false;
    };

    match (
        of1_tx_meta.pre_token_balances.as_ref(),
        rpc_tx_meta.pre_token_balances.as_ref(),
    ) {
        (Some(of1_balances), Some(rpc_balances))
            if known_ui_amount_mismatch_in(of1_balances, rpc_balances) =>
        {
            return true;
        }
        _ => {}
    }

    match (
        of1_tx_meta.post_token_balances.as_ref(),
        rpc_tx_meta.post_token_balances.as_ref(),
    ) {
        (Some(of1_balances), Some(rpc_balances))
            if known_ui_amount_mismatch_in(of1_balances, rpc_balances) =>
        {
            return true;
        }
        _ => {}
    }

    false
}

/// Checks for a known mismatch where OF1 has block rewards for a transaction but
/// RPC shows no rewards for the same transaction (because some RPC providers
/// prune rewards for older transactions).
fn known_tx_reward_mismatch(
    of1_tx_meta: Option<&tables::transactions::TransactionStatusMeta>,
    rpc_tx_meta: Option<&tables::transactions::TransactionStatusMeta>,
) -> bool {
    let (Some(of1_tx_meta), Some(rpc_tx_meta)) = (of1_tx_meta, rpc_tx_meta) else {
        return false;
    };

    let (Some(of1_rewards), Some(rpc_rewards)) =
        (of1_tx_meta.rewards.as_ref(), rpc_tx_meta.rewards.as_ref())
    else {
        return false;
    };
    rpc_rewards.is_empty() && !of1_rewards.is_empty()
}
