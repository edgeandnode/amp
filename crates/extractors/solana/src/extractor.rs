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

use datasets_common::dataset::BlockNum;
use datasets_raw::{
    client::{BlockStreamError, BlockStreamer, CleanupError, LatestBlockError},
    rows::Rows,
};
use futures::{Stream, StreamExt, TryStreamExt};
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
    network: String,
    provider_name: String,
    of1_car_directory: PathBuf,
    use_archive: crate::UseArchive,
}

impl SolanaExtractor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        rpc_provider_url: Url,
        max_rpc_calls_per_second: Option<NonZeroU32>,
        network: String,
        provider_name: String,
        of1_car_directory: PathBuf,
        keep_of1_car_files: bool,
        use_archive: crate::UseArchive,
        meter: Option<&monitoring::telemetry::metrics::Meter>,
    ) -> Self {
        let rpc_client = rpc_client::SolanaRpcClient::new(
            rpc_provider_url,
            max_rpc_calls_per_second,
            provider_name.clone(),
            network.clone(),
        );
        let metrics = meter.map(metrics::MetricsRegistry::new).map(Arc::new);

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
        T: Stream<Item = Result<of1_client::DecodedBlock, BlockStreamError>>,
    {
        async_stream::stream! {
            // Slots can be skipped, so we'll track the next expected slot and in the case of a
            // mismatch, yield empty rows for the skipped slots.
            let mut expected_next_slot = start;

            futures::pin_mut!(historical_block_stream);

            while let Some(block) = historical_block_stream.next().await.transpose()? {
                let current_slot = block.slot;

                debug_assert!(
                    (start..=end).contains(&current_slot),
                    "historical block stream should be limited to requested range"
                );

                if current_slot != expected_next_slot {
                    // NOTE: If `block.slot == end`, we don't yield an empty row for it since
                    // we're about to yield the actual block rows for that slot.
                    let end = std::cmp::min(block.slot, end);

                    // Yield empty rows for skipped slots.
                    for skipped_slot in expected_next_slot..end {
                        yield tables::empty_db_rows(skipped_slot, &self.network).map_err(Into::into);
                    }
                }

                yield tables::convert_of_data_to_db_rows(block, &self.network).map_err(Into::into);

                if current_slot == end {
                    // Reached the end of the requested range.
                    return;
                }

                expected_next_slot = current_slot + 1;
            }

            debug_assert!(expected_next_slot <= end);

            tracing::debug!(
                next = %expected_next_slot,
                end,
                "exhausted Old Faithful archive, switching to JSON-RPC"
            );

            // Download the remaining blocks via JSON-RPC.
            for block_num in expected_next_slot..=end {
                let get_block_resp = self.rpc_client.get_block(
                    block_num,
                    get_block_config,
                    self.metrics.clone(),
                ).await;

                let block = match get_block_resp {
                    Ok(block) => block,
                    Err(e) => {
                        if rpc_client::is_block_missing_err(&e) {
                            yield tables::empty_db_rows(block_num, &self.network).map_err(Into::into);
                        } else {
                            yield Err(e.into());
                        }

                        continue;
                    }
                };

                yield tables::convert_rpc_block_to_db_rows(block_num, block, &self.network).map_err(Into::into);
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use futures::StreamExt;
    use url::Url;

    use super::SolanaExtractor;
    use crate::{of1_client, rpc_client};

    impl of1_client::DecodedBlock {
        fn empty(slot: solana_clock::Slot) -> Self {
            Self {
                slot,
                parent_slot: slot.saturating_sub(1),
                ..Default::default()
            }
        }
    }

    #[tokio::test]
    async fn historical_blocks_only() {
        let extractor = SolanaExtractor::new(
            Url::parse("https://example.net").unwrap(),
            None,
            String::new(),
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
                yield Ok(of1_client::DecodedBlock::empty(slot));
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
            String::new(),
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
                yield Ok(of1_client::DecodedBlock::empty(slot));
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
