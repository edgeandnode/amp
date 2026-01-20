use async_stream::stream;
use common::{BlockNum, BlockStreamer, BoxError, RawDatasetRows};
use futures::Stream;

#[derive(Clone)]
pub(crate) enum BlockStreamClient {
    EvmRpc(evm_rpc_datasets::JsonRpcClient),
    Solana(solana_datasets::SolanaExtractor),
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
                Self::Solana(client) => {
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
            Self::Solana(client) => client.latest_block(finalized).await,
            Self::Firehose(client) => client.latest_block(finalized).await,
        }
    }

    async fn wait_for_cleanup(self) -> Result<(), BoxError> {
        match self {
            Self::EvmRpc(client) => client.wait_for_cleanup().await,
            Self::Solana(client) => client.wait_for_cleanup().await,
            Self::Firehose(client) => client.wait_for_cleanup().await,
        }
    }

    fn provider_name(&self) -> &str {
        match self {
            Self::EvmRpc(client) => client.provider_name(),
            Self::Solana(client) => client.provider_name(),
            Self::Firehose(client) => client.provider_name(),
        }
    }
}
