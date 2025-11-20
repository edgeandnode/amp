use common::{BlockNum, BlockStreamer, BoxResult, RawDatasetRows};
use futures::Stream;
use url::Url;

use crate::{
    rpc_client::{SolanaRpcClient, is_block_missing_err, rpc_config},
    tables,
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
}

impl SolanaExtractor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        rpc_url: Url,
        network: String,
        provider_name: String,
        meter: Option<&monitoring::telemetry::metrics::Meter>,
    ) -> Self {
        let rpc_client =
            SolanaRpcClient::new(rpc_url, provider_name.clone(), network.clone(), None, meter);

        Self {
            rpc_client,
            network,
            provider_name,
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
            let get_block_config = rpc_config::RpcBlockConfig {
                encoding: Some(rpc_config::UiTransactionEncoding::Json),
                transaction_details: Some(rpc_config::TransactionDetails::Full),
                max_supported_transaction_version: Some(0),
                rewards: Some(false),
                // TODO: Make this configurable.
                commitment: Some(rpc_config::CommitmentConfig::finalized()),
            };

            let step: usize = SOLANA_RPC_GET_BLOCKS_LIMIT
                .try_into()
                .expect("conversion error");
            let chunks = (start..=end).step_by(step);

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
