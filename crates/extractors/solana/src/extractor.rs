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

use std::{num::NonZeroU32, path::PathBuf, sync::Arc};

use common::{BlockNum, BlockStreamer, BoxResult, RawDatasetRows};
use futures::{Stream, StreamExt};
use url::Url;

use crate::{of1_client, rpc_client, tables};

/// A JSON-RPC based Solana extractor that implements the [`BlockStreamer`] trait.
#[derive(Clone)]
pub struct SolanaExtractor {
    rpc_client: Arc<rpc_client::SolanaRpcClient>,
    network: String,
    provider_name: String,
    of1_car_directory: PathBuf,
}

impl SolanaExtractor {
    pub fn new(
        rpc_provider_url: Url,
        max_rpc_calls_per_second: Option<NonZeroU32>,
        network: String,
        provider_name: String,
        of1_car_directory: PathBuf,
        meter: Option<&monitoring::telemetry::metrics::Meter>,
    ) -> Self {
        let rpc_client = Arc::new(rpc_client::SolanaRpcClient::new(
            rpc_provider_url,
            max_rpc_calls_per_second,
            provider_name.clone(),
            network.clone(),
            meter,
        ));

        Self {
            rpc_client,
            network,
            provider_name,
            of1_car_directory,
        }
    }

    fn block_stream_impl<T>(
        self,
        start: BlockNum,
        end: BlockNum,
        historical_block_stream: T,
        get_block_config: rpc_client::rpc_config::RpcBlockConfig,
    ) -> impl Stream<Item = BoxResult<RawDatasetRows>>
    where
        T: Stream<Item = BoxResult<of1_client::DecodedBlock>>,
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
                        yield tables::empty_db_rows(skipped_slot, &self.network);
                    }
                }

                yield tables::convert_of_data_to_db_rows(block, &self.network);

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
                let get_block_resp = self.rpc_client.get_block(block_num, get_block_config).await;

                let block = match get_block_resp {
                    Ok(block) => block,
                    Err(e) => {
                        if rpc_client::is_block_missing_err(&e) {
                            yield tables::empty_db_rows(block_num, &self.network);
                        } else {
                            yield Err(e.into());
                        }

                        continue;
                    }
                };

                yield tables::convert_rpc_block_to_db_rows(block_num, block, &self.network);
            }
        }
    }
}

impl BlockStreamer for SolanaExtractor {
    async fn block_stream(
        self,
        start: BlockNum,
        end: BlockNum,
    ) -> impl Stream<Item = BoxResult<RawDatasetRows>> {
        let get_block_config = rpc_client::rpc_config::RpcBlockConfig {
            encoding: Some(rpc_client::rpc_config::UiTransactionEncoding::Json),
            transaction_details: Some(rpc_client::rpc_config::TransactionDetails::Full),
            max_supported_transaction_version: Some(0),
            rewards: Some(false),
            // TODO: Make this configurable.
            commitment: Some(rpc_client::rpc_config::CommitmentConfig::finalized()),
        };

        let historical_block_stream = of1_client::stream(
            start,
            end,
            self.of1_car_directory.clone(),
            self.rpc_client.clone(),
            get_block_config,
        );

        self.block_stream_impl(start, end, historical_block_stream, get_block_config)
    }

    async fn latest_block(&mut self, _finalized: bool) -> BoxResult<Option<BlockNum>> {
        let get_block_height_resp = self.rpc_client.get_block_height().await;

        match get_block_height_resp {
            Ok(block_height) => Ok(Some(block_height)),
            Err(e) if rpc_client::is_block_missing_err(&e) => Ok(None),
            Err(e) => Err(e.into()),
        }
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
