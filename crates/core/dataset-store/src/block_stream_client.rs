use async_stream::stream;
use common::{BlockNum, BlockStreamer, BoxError, RawDatasetRows};
use futures::Stream;

#[derive(Clone)]
pub(crate) enum BlockStreamClient {
    EvmRpc(evm_rpc_datasets::JsonRpcClient),
    EthBeacon(eth_beacon_datasets::BeaconClient),
    Firehose(firehose_datasets::Client),
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
            Self::EthBeacon(client) => client.latest_block(finalized).await,
            Self::Firehose(client) => client.latest_block(finalized).await,
        }
    }

    fn provider_name(&self) -> &str {
        match self {
            Self::EvmRpc(client) => client.provider_name(),
            Self::EthBeacon(client) => client.provider_name(),
            Self::Firehose(client) => client.provider_name(),
        }
    }
}
