//! Canton client implementing BlockStreamer trait.
//!
//! Connects to canton-bridge via Arrow Flight and streams ledger data as AMP Rows.

use std::sync::Arc;

use alloy::primitives::B256;
use arrow::{array::AsArray, datatypes::UInt64Type};
use async_stream::stream;
use canton_bridge::{CantonDescriptor, CantonStreamType};
use datasets_common::{block_range::BlockRange, dataset::BlockNum};
use datasets_raw::{
    client::{BlockStreamError, BlockStreamer, CleanupError, LatestBlockError},
    rows::{Rows, TableRows},
};
use futures::{Stream, StreamExt};
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tracing::{debug, info, warn};

use crate::{dataset::ProviderConfig, tables};

/// Canton client for streaming ledger data via canton-bridge Arrow Flight.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Mutex<arrow_flight::FlightClient>>,
    party_id: String,
    provider_name: String,
    network: String,
}

impl Client {
    /// Create a new Canton client from provider configuration.
    pub async fn new(config: ProviderConfig) -> Result<Self, Error> {
        info!("Connecting to canton-bridge at {}", config.bridge_url);

        let channel = Channel::from_shared(config.bridge_url.clone())
            .map_err(|e| Error::Connection(e.to_string()))?
            .connect()
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;

        // Configure message size limits for large batches
        const MAX_MESSAGE_SIZE: usize = 256 * 1024 * 1024;

        let flight_service_client =
            arrow_flight::flight_service_client::FlightServiceClient::new(channel)
                .max_decoding_message_size(MAX_MESSAGE_SIZE)
                .max_encoding_message_size(MAX_MESSAGE_SIZE);

        let client = arrow_flight::FlightClient::new_from_inner(flight_service_client);

        Ok(Self {
            inner: Arc::new(Mutex::new(client)),
            party_id: config.party_id,
            provider_name: config.name,
            network: config.network,
        })
    }

    /// Get the network name.
    pub fn network(&self) -> &str {
        &self.network
    }

    /// Get tables for the network
    fn tables(&self) -> Vec<datasets_common::dataset::Table> {
        tables::all(&self.network)
    }
}

impl BlockStreamer for Client {
    /// Stream Canton ledger data as AMP Rows.
    ///
    /// Note: Canton uses offsets, not block numbers. The start/end parameters
    /// are interpreted as Canton offsets for compatibility.
    async fn block_stream(
        self,
        start_block: BlockNum,
        end_block: BlockNum,
    ) -> impl Stream<Item = Result<Rows, BlockStreamError>> + Send {
        // Map AMP BlockNum to Canton offset
        let start_offset = start_block as i64;
        let end_offset = end_block as i64;

        let party_id = self.party_id.clone();
        let network = self.network.clone();
        let tables = self.tables();
        let client = self.inner.clone();

        debug!(
            start_offset,
            end_offset, %party_id, "Starting Canton block stream"
        );

        stream! {
            // Create Canton descriptor for transactions stream
            // We stream all three types and combine them
            let descriptor = CantonDescriptor::historical(
                CantonStreamType::Transactions,
                start_offset,
                end_offset,
                party_id.clone(),
            );

            let ticket = descriptor.to_ticket();

            let mut flight_client = client.lock().await;

            // Start the do_get stream
            let mut decoder = match flight_client.do_get(ticket).await {
                Ok(stream) => stream,
                Err(e) => {
                    yield Err(Box::new(std::io::Error::other(format!(
                        "Failed to start stream: {e}"
                    ))) as BlockStreamError);
                    return;
                }
            };

            // Drop lock while streaming
            drop(flight_client);

            // Find the transactions table
            let tx_table = tables.iter()
                .find(|t| t.name().as_str() == "transactions")
                .cloned();

            let Some(tx_table) = tx_table else {
                yield Err(Box::new(std::io::Error::other(
                    "Transactions table not found"
                )) as BlockStreamError);
                return;
            };

            // Process batches (canton-bridge batches already have _block_num)
            while let Some(batch_result) = decoder.next().await {
                match batch_result {
                    Ok(batch) => {
                        if batch.num_rows() == 0 {
                            continue;
                        }

                        // Get block numbers from the batch to determine range
                        let block_nums = batch.column_by_name("_block_num")
                            .expect("_block_num column must exist in canton-schema");
                        let block_nums = block_nums.as_primitive::<UInt64Type>();

                        // Group rows by block number and yield Rows for each
                        let min_block = arrow::compute::min(block_nums).unwrap_or(0);
                        let max_block = arrow::compute::max(block_nums).unwrap_or(0);

                        // For simplicity, if all rows have the same block, yield as one
                        // Otherwise split by block (TODO: optimize this)
                        if min_block == max_block {
                            let range = BlockRange {
                                numbers: min_block..=min_block,
                                network: network.clone(),
                                // Canton doesn't have block hashes, use zero
                                hash: B256::ZERO,
                                prev_hash: None,
                            };
                            match TableRows::new(
                                tx_table.clone(),
                                range,
                                batch.columns().to_vec(),
                            ) {
                                Ok(table_rows) => {
                                    yield Ok(Rows::new(vec![table_rows]));
                                }
                                Err(e) => {
                                    yield Err(Box::new(std::io::Error::other(format!(
                                        "Failed to create TableRows: {e}"
                                    ))) as BlockStreamError);
                                    return;
                                }
                            }
                        } else {
                            // Split batch by block number
                            for block_num in min_block..=max_block {
                                let mask: arrow::array::BooleanArray = block_nums
                                    .iter()
                                    .map(|v| v.map(|n| n == block_num))
                                    .collect();

                                let filtered = match arrow::compute::filter_record_batch(&batch, &mask) {
                                    Ok(b) => b,
                                    Err(e) => {
                                        warn!("Failed to filter batch: {e}");
                                        continue;
                                    }
                                };

                                if filtered.num_rows() == 0 {
                                    continue;
                                }

                                let range = BlockRange {
                                    numbers: block_num..=block_num,
                                    network: network.clone(),
                                    hash: B256::ZERO,
                                    prev_hash: None,
                                };
                                match TableRows::new(
                                    tx_table.clone(),
                                    range,
                                    filtered.columns().to_vec(),
                                ) {
                                    Ok(table_rows) => {
                                        yield Ok(Rows::new(vec![table_rows]));
                                    }
                                    Err(e) => {
                                        yield Err(Box::new(std::io::Error::other(format!(
                                            "Failed to create TableRows: {e}"
                                        ))) as BlockStreamError);
                                        return;
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        yield Err(Box::new(std::io::Error::other(format!(
                            "Stream error: {e}"
                        ))) as BlockStreamError);
                        return;
                    }
                }
            }

            debug!("Canton block stream completed");
        }
    }

    async fn latest_block(
        &mut self,
        _finalized: bool,
    ) -> Result<Option<BlockNum>, LatestBlockError> {
        // Query canton-bridge for info to get current offset
        // Canton has finality, so finalized flag is ignored

        let mut client = self.inner.lock().await;

        // Use list_flights or get_flight_info to determine current offset
        // For now, use a descriptor to query info
        let descriptor =
            CantonDescriptor::live(CantonStreamType::Transactions, self.party_id.clone());
        let flight_desc = descriptor.to_flight_descriptor();

        match client.get_flight_info(flight_desc).await {
            Ok(info) => {
                // The total_records field might indicate current offset
                // This is bridge-specific behavior
                Ok(Some(info.total_records as BlockNum))
            }
            Err(e) => Err(Box::new(std::io::Error::other(format!(
                "Failed to get flight info: {e}"
            ))) as LatestBlockError),
        }
    }

    async fn wait_for_cleanup(self) -> Result<(), CleanupError> {
        // Arrow Flight client doesn't require explicit cleanup
        Ok(())
    }

    fn provider_name(&self) -> &str {
        &self.provider_name
    }
}

/// Errors that can occur with the Canton client.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Connection error to canton-bridge
    #[error("Failed to connect to canton-bridge: {0}")]
    Connection(String),
    /// Stream error
    #[error("Stream error: {0}")]
    Stream(String),
}
