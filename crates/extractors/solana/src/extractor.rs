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

use std::path::PathBuf;

use common::{BlockNum, BlockStreamer, BoxResult, RawDatasetRows};
use futures::Stream;
use url::Url;

use crate::{
    of1_client,
    rpc_client::{SolanaRpcClient, is_block_missing_err, rpc_config},
    tables::{self},
};

/// Solana `getBlocks` RPC method has a limit on the number of slots between `start` and `end`.
/// Requesting more than this limit will result in an error.
const SOLANA_RPC_GET_BLOCKS_LIMIT: u64 = 500_000;

/// A JSON-RPC based Solana extractor that implements the [`BlockStreamer`] trait.
#[derive(Clone)]
pub struct SolanaExtractor {
    rpc_client: SolanaRpcClient,
    network: String,
    provider_name: String,
    of1_car_directory: PathBuf,
}

impl SolanaExtractor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        rpc_url: Url,
        network: String,
        provider_name: String,
        of1_car_directory: PathBuf,
        meter: Option<&monitoring::telemetry::metrics::Meter>,
    ) -> Self {
        let rpc_client =
            SolanaRpcClient::new(rpc_url, provider_name.clone(), network.clone(), None, meter);

        Self {
            rpc_client,
            network,
            provider_name,
            of1_car_directory,
        }
    }
}

impl BlockStreamer for SolanaExtractor {
    async fn block_stream(
        self,
        start: BlockNum,
        end: BlockNum,
    ) -> impl Stream<Item = BoxResult<RawDatasetRows>> + Send {
        async_stream::stream! {
            let mut epoch = start / solana_clock::DEFAULT_SLOTS_PER_EPOCH;

            let get_block_config = rpc_config::RpcBlockConfig {
                encoding: Some(rpc_config::UiTransactionEncoding::Json),
                transaction_details: Some(rpc_config::TransactionDetails::Full),
                max_supported_transaction_version: Some(0),
                rewards: Some(false),
                // TODO: Make this configurable.
                commitment: Some(rpc_config::CommitmentConfig::finalized()),
            };

            // Pre-fetch the initial previous block hash via JSON-RPC so that we don't have to
            // (potentially) read multiple Old Faithful CAR files to find it.
            let mut prev_blockhash = if start == 0 {
                [0u8; 32]
            } else {
                let mut parent_slot = start - 1;
                loop {
                    let resp = self.rpc_client.get_block(parent_slot, get_block_config).await;

                    match resp {
                        // Found the parent block, extract its blockhash.
                        Ok(block) => break bs58::decode(block.blockhash)
                            .into_vec()?
                            .try_into()
                            .expect("blockhash is 32 bytes"),
                        // Parent block is missing, try the previous slot.
                        Err(e) if is_block_missing_err(&e) => {
                            if parent_slot == 0 {
                                break [0u8; 32];
                            } else {
                                parent_slot -= 1;
                                continue;
                            }
                        },
                        // Some other error occurred.
                        Err(e) => {
                            yield Err(e.into());
                            return;
                        },
                    }
                }
            };


            // Slots can be skipped, so we'll track the next expected slot and in the case of a
            // mismatch, yield empty rows for the skipped slots.
            let mut expected_next_slot = start;

            // Download historical data via Old Faithful archive CAR files.
            'epochs: loop {
                tracing::debug!(epoch, "processing Old Faithful epoch CAR file");
                let epoch_car_file_path = self.of1_car_directory.join(of1_client::of1_car_filename(epoch));

                if !std::fs::exists(&epoch_car_file_path)? {
                    // This can take a while.
                    if let Err(e) = of1_client::download_of1_car_file(epoch, &self.of1_car_directory).await {
                        if let of1_client::FileDownloadError::Http(404) = e {
                            // No more epoch CAR files available, proceed with JSON-RPC.
                            break 'epochs;
                        } else {
                            yield Err(e.into());
                            return;
                        }
                    };
                }

                let epoch_car_file = tokio::fs::File::open(&epoch_car_file_path).await?;
                let mut node_reader = of1_client::of1_car_parser::node::NodeReader::new(
                    tokio::io::BufReader::new(epoch_car_file)
                );

                while let Some(block) = of1_client::read_entire_block(&mut node_reader, prev_blockhash).await? {
                    prev_blockhash = block.blockhash;

                    if block.slot < start {
                        // Skip blocks before the start of the requested range.
                        continue;
                    }

                    if block.slot != expected_next_slot {
                        // Yield empty rows for skipped slots.
                        let end = std::cmp::min(block.slot, end);

                        // NOTE: If `block.slot == end`, we don't yield an empty row for it since
                        // we're about to yield the actual block rows for that slot.
                        for skipped_slot in expected_next_slot..end {
                            yield tables::empty_db_rows(skipped_slot, &self.network);
                        }
                    }

                    if block.slot > end {
                        // Reached the end of the requested range.
                        return;
                    }

                    expected_next_slot = block.slot + 1;

                    yield tables::convert_of_data_to_db_rows(block, &self.network);
                }

                epoch += 1;
            }

            debug_assert!(expected_next_slot <= end);

            tracing::debug!(
                next = %expected_next_slot,
                end,
                "exhausted Old Faithful archive, switching to JSON-RPC"
            );

            // Download the remaining blocks via JSON-RPC.
            let step: usize = SOLANA_RPC_GET_BLOCKS_LIMIT
                .try_into()
                .expect("conversion error");
            let chunks = (expected_next_slot..=end).step_by(step);

            for chunk_start in chunks {
                let chunk_end = std::cmp::min(chunk_start + SOLANA_RPC_GET_BLOCKS_LIMIT - 1, end);

                for block_num in chunk_start..=chunk_end {
                    let get_block_resp = self.rpc_client.get_block(block_num, get_block_config).await;

                    let block = match get_block_resp {
                        Ok(block) => block,
                        Err(e) => {
                            if is_block_missing_err(&e) {
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

    async fn latest_block(&mut self, _finalized: bool) -> BoxResult<Option<BlockNum>> {
        let get_block_height_resp = self.rpc_client.get_block_height().await;

        match get_block_height_resp {
            Ok(block_height) => Ok(Some(block_height)),
            Err(e) if is_block_missing_err(&e) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn provider_name(&self) -> &str {
        &self.provider_name
    }
}
