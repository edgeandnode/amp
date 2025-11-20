use std::sync::Arc;

use async_stream::stream;
use common::{BlockNum, BlockStreamer, BoxError, RawDatasetRows};
use futures::Stream;

#[derive(Clone)]
pub(crate) enum BlockStreamClient {
    EvmRpc(evm_rpc_datasets::JsonRpcClient),
    Solana {
        extractor: solana_datasets::SolanaExtractor,
        /// NOTE: This handle is not used at the moment, as there is no convenient place to
        /// handle premature subscription cancellation and error recovery outside the task
        /// with the current architecture. It is all done inside the task with a retry loop.
        /// If the design changes at any point in a way that allows managing this task
        /// externally, it would probably lead to more understandable code/task flow.
        #[allow(dead_code)]
        subscription_task: Arc<tokio::task::JoinHandle<()>>,
    },
    EthBeacon(eth_beacon_datasets::BeaconClient),
    Firehose(Box<firehose_datasets::Client>),
}

impl BlockStreamer for BlockStreamClient {
    async fn block_stream(
        self,
        start_block: BlockNum,
        end_block: BlockNum,
    ) -> impl Stream<Item = Result<RawDatasetRows, BoxError>> + Send {
        // Each client returns a different concrete stream type, so we
        // use `stream!` to unify them into a wrapper stream
        stream! {
            match self {
                Self::EvmRpc(client) => {
                    let stream = client.block_stream(start_block, end_block).await;
                    for await item in stream {
                        yield item;
                    }
                }
                Self::Solana {
                    extractor,
                    ..
                } => {
                    let stream = extractor.block_stream(start_block, end_block).await;
                    for await item in stream {
                        yield item;
                    }
                }
                Self::EthBeacon(client) => {
                let stream = client.block_stream(start_block, end_block).await;
                   for await item in stream {
                        yield item;
                    }
                }
                Self::Firehose(client) => {
                    let stream = client.block_stream(start_block, end_block).await;
                    for await item in stream {
                        yield item;
                    }
                }
            }
        }
    }

    async fn latest_block(&mut self, finalized: bool) -> Result<Option<BlockNum>, BoxError> {
        match self {
            Self::EvmRpc(client) => client.latest_block(finalized).await,
            Self::Solana { extractor, .. } => extractor.latest_block(finalized).await,
            Self::EthBeacon(client) => client.latest_block(finalized).await,
            Self::Firehose(client) => client.latest_block(finalized).await,
        }
    }

    fn provider_name(&self) -> &str {
        match self {
            Self::EvmRpc(client) => client.provider_name(),
            Self::Solana { extractor, .. } => extractor.provider_name(),
            Self::EthBeacon(client) => client.provider_name(),
            Self::Firehose(client) => client.provider_name(),
        }
    }
}
