//! Solana extractor that implements the [`BlockStreamer`] trait.
//!
//! The extractor is designed to work in two stages:
//!     1. Download historical data from the Old Faithful archive
//!     2. Stream new data using the Solana JSON-RPC subscription API (not implemented yet)
//!
//! The second stage pulls blocks from the subscription ring buffer, which is populated by the
//! [crate::subscription_task].
//!
//! Learn more about the Old Faithful archive here: <https://docs.old-faithful.net/>.

use std::{
    num::NonZeroU32,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::Context;
use datasets_common::{dataset::BlockNum, network_id::NetworkId};
use datasets_raw::{
    client::{BlockStreamError, BlockStreamer, CleanupError, LatestBlockError},
    rows::Rows,
};
use futures::{Stream, StreamExt, TryStreamExt};
use solana_clock::Slot;
use url::Url;

use crate::{
    metrics, of1_client, rpc_client,
    tables::{self},
};

/// Handles related to the OF1 CAR manager task, stored in the extractor for cleanup.
struct Of1CarManagerHandles {
    tx: tokio::sync::mpsc::Sender<of1_client::CarManagerMessage>,
    jh: tokio::task::JoinHandle<()>,
}

/// A JSON-RPC based Solana extractor that implements the [`BlockStreamer`] trait.
pub struct SolanaExtractor {
    of1_car_manager_handles: Arc<Mutex<Option<Of1CarManagerHandles>>>,
    rpc_client: Arc<rpc_client::SolanaRpcClient>,
    metrics: Option<Arc<metrics::MetricsRegistry>>,
    network: NetworkId,
    provider_name: String,
    of1_car_directory: PathBuf,
    use_archive: crate::UseArchive,
}

impl SolanaExtractor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        rpc_provider_url: Url,
        max_rpc_calls_per_second: Option<NonZeroU32>,
        network: NetworkId,
        provider_name: String,
        of1_car_directory: PathBuf,
        keep_of1_car_files: bool,
        use_archive: crate::UseArchive,
        meter: Option<&monitoring::telemetry::metrics::Meter>,
    ) -> Self {
        assert_eq!(network, "mainnet", "only mainnet is supported");

        let metrics = meter.map(metrics::MetricsRegistry::new).map(Arc::new);

        let rpc_client = rpc_client::SolanaRpcClient::new(
            rpc_provider_url,
            max_rpc_calls_per_second,
            provider_name.clone(),
            network.clone(),
        );

        let (of1_car_manager_tx, of1_car_manager_rx) = tokio::sync::mpsc::channel(128);
        let of1_car_manager_jh = tokio::task::spawn(of1_client::car_file_manager(
            of1_car_manager_rx,
            of1_car_directory.clone(),
            keep_of1_car_files,
            provider_name.clone(),
            network.clone(),
            metrics.clone(),
        ));
        let handles = Of1CarManagerHandles {
            tx: of1_car_manager_tx,
            jh: of1_car_manager_jh,
        };

        Self {
            of1_car_manager_handles: Arc::new(Mutex::new(Some(handles))),
            rpc_client: Arc::new(rpc_client),
            metrics,
            network,
            provider_name,
            of1_car_directory,
            use_archive,
        }
    }

    fn block_stream_impl<T>(
        self,
        start: BlockNum,
        end: BlockNum,
        historical_block_stream: T,
        get_block_config: rpc_client::rpc_config::RpcBlockConfig,
    ) -> impl Stream<Item = Result<Rows, BlockStreamError>>
    where
        T: Stream<Item = Result<of1_client::DecodedSlot, BlockStreamError>>,
    {
        async_stream::stream! {
            // Helper macro to simplify error handling and early returns in the stream.
            macro_rules! ok_or_bail {
                ($expr:expr) => {
                    match $expr {
                        Ok(val) => val,
                        Err(e) => {
                            yield Err(e);
                            return;
                        }
                    }
                };
            }

            // Slots can be skipped, so we'll track the next expected slot for switching to
            // JSON-RPC.
            let mut expected_next_slot = start;
            let requested_range = start..=end;

            // Download historical blocks from the Old Faithful archive.
            futures::pin_mut!(historical_block_stream);
            while let Some(slot) = historical_block_stream.next().await {
                let slot = ok_or_bail!(slot);

                let current_slot = slot.slot;
                if !requested_range.contains(&current_slot) {
                    let e = format!(
                        "historical block stream yielded slot {current_slot} outside of requested range {start}..={end}"
                    );
                    yield Err(e.into());
                    return;
                }

                // Don't emit rows for skipped slots.
                let non_empty_slot = ok_or_bail!(non_empty_of1_slot(slot).map_err(Into::into));
                yield tables::convert_slot_to_db_rows(non_empty_slot, &self.network).map_err(Into::into);

                if current_slot == end {
                    // Reached the end of the requested range.
                    return;
                }

                expected_next_slot = current_slot + 1;
            }

            tracing::debug!(
                next = %expected_next_slot,
                end,
                "exhausted Old Faithful archive, switching to JSON-RPC"
            );

            // Download the remaining blocks via JSON-RPC.
            for slot in expected_next_slot..=end {
                let get_block_resp = self.rpc_client.get_block(
                    slot,
                    get_block_config,
                    self.metrics.clone(),
                ).await;

                match get_block_resp {
                    Ok(block) => {
                        let non_empty_slot = ok_or_bail!(non_empty_rpc_slot(slot, block).map_err(Into::into));
                        yield tables::convert_slot_to_db_rows(non_empty_slot, &self.network).map_err(Into::into);
                    }
                    Err(e) => {
                        // If block is missing (skipped slot), don't emit any rows.
                        if !rpc_client::is_block_missing_err(&e) {
                            yield Err(e.into());
                            return;
                        }
                    }
                };
            }
        }
    }
}

impl Clone for SolanaExtractor {
    fn clone(&self) -> Self {
        assert!(
            self.of1_car_manager_handles.lock().unwrap().is_some(),
            "cannot clone SolanaExtractor after cleanup"
        );

        Self {
            of1_car_manager_handles: self.of1_car_manager_handles.clone(),
            rpc_client: self.rpc_client.clone(),
            metrics: self.metrics.clone(),
            network: self.network.clone(),
            provider_name: self.provider_name.clone(),
            of1_car_directory: self.of1_car_directory.clone(),
            use_archive: self.use_archive,
        }
    }
}

impl BlockStreamer for SolanaExtractor {
    async fn block_stream(
        self,
        start: BlockNum,
        end: BlockNum,
    ) -> impl Stream<Item = Result<Rows, BlockStreamError>> {
        let get_block_config = rpc_client::rpc_config::RpcBlockConfig {
            encoding: Some(rpc_client::rpc_config::UiTransactionEncoding::Json),
            transaction_details: Some(rpc_client::rpc_config::TransactionDetails::Full),
            max_supported_transaction_version: Some(0),
            rewards: Some(false),
            // TODO: Make this configurable.
            commitment: Some(rpc_client::rpc_config::CommitmentConfig::finalized()),
        };

        // Determine archive usage based on configuration
        let use_rpc_only = match self.use_archive {
            crate::UseArchive::Never => {
                tracing::info!("Using RPC-only mode (use_archive = never)");
                true
            }
            crate::UseArchive::Always => {
                tracing::info!("Using archive mode (use_archive = always)");
                false
            }
            crate::UseArchive::Auto => {
                // Auto mode: skip archive for recent slots, use it for historical data
                match self.rpc_client.get_slot(self.metrics.clone()).await {
                    Ok(current_slot) => {
                        let threshold = current_slot.saturating_sub(10_000);
                        let skip_archive = start > threshold;

                        if skip_archive {
                            tracing::info!(
                                start_slot = start,
                                current_slot = current_slot,
                                "Using RPC-only mode (recent slots, use_archive = auto)"
                            );
                        } else {
                            tracing::info!(
                                start_slot = start,
                                current_slot = current_slot,
                                threshold,
                                "Using archive mode (historical slots, use_archive = auto)"
                            );
                        }

                        skip_archive
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "Failed to get current slot for auto mode, falling back to archive"
                        );
                        false
                    }
                }
            }
        };

        let of1_car_manager_tx = {
            let guard = self.of1_car_manager_handles.lock().unwrap();
            guard
                .as_ref()
                .expect("new block streams should not start after cleanup")
                .tx
                .clone()
        };

        let historical_block_stream = if use_rpc_only {
            // Return empty stream to skip Old Faithful entirely
            futures::stream::empty().boxed()
        } else {
            of1_client::stream(
                start,
                end,
                self.of1_car_directory.clone(),
                of1_car_manager_tx,
                self.rpc_client.clone(),
                get_block_config,
                self.metrics.clone(),
            )
            .map_err(Into::into)
            .boxed()
        };

        self.block_stream_impl(start, end, historical_block_stream, get_block_config)
    }

    async fn latest_block(
        &mut self,
        _finalized: bool,
    ) -> Result<Option<BlockNum>, LatestBlockError> {
        let get_block_height_resp = self.rpc_client.get_block_height(self.metrics.clone()).await;

        match get_block_height_resp {
            Ok(block_height) => Ok(Some(block_height)),
            Err(e) if rpc_client::is_block_missing_err(&e) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn wait_for_cleanup(self) -> Result<(), CleanupError> {
        let Self {
            of1_car_manager_handles,
            ..
        } = self;

        let Of1CarManagerHandles { tx, jh } = {
            let mut guard = of1_car_manager_handles.lock().unwrap();
            if let Some(handles) = guard.take() {
                handles
            } else {
                // Cleanup already done.
                return Ok(());
            }
        };

        // Drop the extra sender so that the CAR manager task can exit -- assuming all block
        // streams, which hold clones of the sender, complete before cleanup.
        drop(tx);
        let _ = jh.await;

        Ok(())
    }

    fn provider_name(&self) -> &str {
        &self.provider_name
    }
}

/// Converts [of1_client::DecodedSlot] to [tables::NonEmptySlot]. This conversion can fail if any
/// of the decoded fields do not match the expected format/values.
fn non_empty_of1_slot(slot: of1_client::DecodedSlot) -> anyhow::Result<tables::NonEmptySlot> {
    let of1_client::DecodedSlot {
        slot,
        parent_slot,
        blockhash,
        prev_blockhash,
        block_height,
        blocktime,
        transactions,
        transaction_metas,
        block_rewards,
    } = slot;

    let mut txs = Vec::with_capacity(transactions.len());
    let mut msgs = Vec::with_capacity(transactions.len());

    for (index, (tx, tx_meta)) in transactions.into_iter().zip(transaction_metas).enumerate() {
        let solana_sdk::transaction::VersionedTransaction {
            signatures,
            message,
        } = tx;
        let tx_index = index.try_into().expect("conversion error");

        let tx = tables::transactions::Transaction::from_of1_transaction(
            slot, tx_index, signatures, tx_meta,
        )
        .context("converting of1 transaction")?;
        let message = tables::messages::Message::from_of1_message(slot, tx_index, message);

        txs.push(tx);
        msgs.push(message);
    }

    let block_rewards = tables::block_rewards::BlockRewards::from_of1_rewards(slot, block_rewards)
        .context("converting of1 block rewards")?;

    Ok(tables::NonEmptySlot {
        slot,
        parent_slot,
        blockhash,
        prev_blockhash,
        block_height,
        blocktime: Some(blocktime),
        transactions: txs,
        messages: msgs,
        block_rewards,
    })
}

/// Converts a JSON-RPC confirmed block into a [tables::NonEmptySlot]. This conversion
/// can fail if the JSON-RPC response does not match the expected format in any way.
fn non_empty_rpc_slot(
    slot: Slot,
    confirmed_block: rpc_client::UiConfirmedBlock,
) -> anyhow::Result<tables::NonEmptySlot> {
    // Transactions and block rewards should be present since we requested them when fetching the block.
    let transactions = confirmed_block
        .transactions
        .with_context(|| format!("missing transactions in confirmed block {slot}"))?;
    let block_rewards = confirmed_block
        .rewards
        .with_context(|| format!("missing block rewards in confirmed block {slot}"))?;

    let mut txs = Vec::with_capacity(transactions.len());
    let mut msgs = Vec::with_capacity(transactions.len());

    for (index, tx_with_meta) in transactions.into_iter().enumerate() {
        let tx_index = index.try_into().expect("conversion error");

        let rpc_client::EncodedTransactionWithStatusMeta {
            transaction, meta, ..
        } = tx_with_meta;

        // These should follow the encoding we requested when fetching the block.
        let rpc_client::EncodedTransaction::Json(rpc_client::UiTransaction {
            signatures,
            message,
        }) = transaction
        else {
            anyhow::bail!("expected JSON encoded transaction for slot {slot}, tx index {tx_index}");
        };
        let rpc_client::UiMessage::Raw(msg) = message else {
            anyhow::bail!("expected raw message for slot {slot}, tx index {tx_index}");
        };

        let tx = tables::transactions::Transaction::from_rpc_transaction(
            slot, tx_index, signatures, meta,
        )?;
        let msg = tables::messages::Message::from_rpc_message(slot, tx_index, msg);

        txs.push(tx);
        msgs.push(msg);
    }

    let block_rewards = tables::block_rewards::BlockRewards::from_rpc_rewards(slot, block_rewards);

    Ok(tables::NonEmptySlot {
        slot,
        parent_slot: confirmed_block.parent_slot,
        blockhash: bs58_decode_blockhash(&confirmed_block.blockhash)?,
        prev_blockhash: bs58_decode_blockhash(&confirmed_block.previous_blockhash)?,
        block_height: confirmed_block.block_height,
        blocktime: confirmed_block.block_time,
        transactions: txs,
        messages: msgs,
        block_rewards,
    })
}

fn bs58_decode_blockhash(blockhash_str: &str) -> anyhow::Result<[u8; 32]> {
    bs58::decode(blockhash_str)
        .into_vec()
        .context("base58 decoding blockhash string")
        .and_then(|bytes| {
            bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("block hash should be 32 bytes long"))
        })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use futures::StreamExt;
    use solana_clock::Slot;
    use url::Url;

    use super::SolanaExtractor;
    use crate::{of1_client, rpc_client};

    impl of1_client::DecodedSlot {
        fn dummy(slot: Slot) -> Self {
            Self {
                slot,
                parent_slot: slot.saturating_sub(1),
                blockhash: [0; 32],
                prev_blockhash: [0; 32],
                block_height: None,
                blocktime: 0,
                transactions: Vec::new(),
                transaction_metas: Vec::new(),
                block_rewards: None,
            }
        }
    }

    #[tokio::test]
    async fn historical_blocks_only() {
        let extractor = SolanaExtractor::new(
            Url::parse("https://example.net").unwrap(),
            None,
            "mainnet".parse().expect("valid network id"),
            String::new(),
            PathBuf::new(),
            false,
            crate::UseArchive::Auto,
            None,
        );

        let start = 0;
        let end = 100;

        // Stream the entire range as historical blocks.
        let historical = async_stream::stream! {
            for slot in start..=end {
                yield Ok(of1_client::DecodedSlot::dummy(slot));
            }
        };

        let block_stream = extractor.block_stream_impl(
            start,
            end,
            historical,
            rpc_client::rpc_config::RpcBlockConfig::default(),
        );

        futures::pin_mut!(block_stream);

        let mut expected_block = start;

        while let Some(rows) = block_stream.next().await.transpose().unwrap() {
            assert_eq!(rows.block_num(), expected_block);
            expected_block += 1;
        }

        assert_eq!(expected_block, end + 1);
    }

    #[tokio::test]
    async fn historical_to_json_rpc_transition() {
        let solana_rpc_provider_url: Url = std::env::var("SOLANA_MAINNET_HTTP_URL")
            .expect("missing environment variable")
            .parse()
            .expect("invalid URL");

        let extractor = SolanaExtractor::new(
            solana_rpc_provider_url,
            None,
            "mainnet".parse().expect("valid network id"),
            String::new(),
            PathBuf::new(),
            false,
            crate::UseArchive::Auto,
            None,
        );

        let start = 0;
        let historical_end = 50;
        let end = historical_end + 20;

        // Stream part of the range as historical blocks.
        let historical = async_stream::stream! {
            for slot in start..=historical_end {
                yield Ok(of1_client::DecodedSlot::dummy(slot));
            }
        };

        let block_stream = extractor.block_stream_impl(
            start,
            end,
            historical,
            rpc_client::rpc_config::RpcBlockConfig::default(),
        );

        futures::pin_mut!(block_stream);

        let mut expected_block = start;

        while let Some(rows) = block_stream.next().await.transpose().unwrap() {
            assert_eq!(rows.block_num(), expected_block);
            expected_block += 1;
        }

        assert_eq!(expected_block, end + 1);
    }
}
