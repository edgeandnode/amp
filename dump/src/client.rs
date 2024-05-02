use common::BlockStreamer;
use firehose_datasets::client::Client as FirehoseClient;
use substreams_datasets::client::Client as SubstreamsClient;
use tokio::sync::mpsc;

#[derive(Clone)]
pub enum BlockStreamerClient {
    FirehoseClient(FirehoseClient),
    SubstreamsClient(SubstreamsClient),
}

impl BlockStreamer for BlockStreamerClient {
    async fn block_stream(
        self,
        start_block: u64,
        end_block: u64,
        tx: mpsc::Sender<common::DatasetRows>,
    ) -> Result<(), anyhow::Error> {
        match self {
            BlockStreamerClient::FirehoseClient(client) => {
                client.block_stream(start_block, end_block, tx).await
            }
            BlockStreamerClient::SubstreamsClient(client) => {
                client.block_stream(start_block, end_block, tx).await
            }
        }
    }
}
