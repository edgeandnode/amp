use std::str::FromStr as _;
use tokio::sync::mpsc;
use anyhow::anyhow;
use prost::Message as _;
use reqwest::Url;

use pbsubstreams::{
    stream_client::StreamClient,
    Request as StreamRequest,
    response::Message,
};
use tonic::{
    service::interceptor::InterceptedService,
    codec::CompressionEncoding,
    transport::{Uri, Endpoint, Channel},
};
use futures::{
    Stream,
    TryStreamExt as _,
    StreamExt as _,
};

use common::{
    BlockNum,
    BlockStreamer,
    DatasetRows,
};
use super::{
    pb_to_rows::pb_to_rows,
    tables::Tables,
};
use crate::client::{AuthInterceptor, Error, FirehoseProvider};
use crate::proto::sf::substreams::rpc::v2::{self as pbsubstreams, BlockScopedData};
use crate::proto::sf::substreams::v1::Package;

// Cloning is cheap and shares the underlying connection.
#[derive(Clone)]
pub struct SubstreamsClient {
    pub stream_client: StreamClient<InterceptedService<Channel, AuthInterceptor>>,
    pub tables: Tables,
    pub output_module: String,
}

impl Package {
    pub async fn from_url(url: &str) -> Result<Self, Error> {
        let url = Url::parse(url).map_err(|_| Error::AssertFail(anyhow!("failed to parse spkg url")))?;

        // Send a GET request to the URL
        let response = reqwest::get(url).await.map_err(|_| Error::AssertFail(anyhow!("failed to get spkg")))?;
        if !response.status().is_success() {
            return Err(Error::AssertFail(anyhow!("Failed to fetch package from URL")));
        }

        let bytes = response.bytes().await.map_err(|_| Error::AssertFail(anyhow!("failed to read response bytes")))?;
        let decoded = Self::decode(bytes.as_ref())?;

        Ok(decoded)
    }
}

impl SubstreamsClient {
    /// Configure the client from an EVM Firehose endpoint.
    pub async fn new(cfg: FirehoseProvider, manifest: String, output_module: String) -> Result<Self, Error> {
        let FirehoseProvider { url, token } = cfg;
        let stream_client = {
            let uri = Uri::from_str(&url)?;
            let channel = Endpoint::from(uri).connect().await?;
            let auth = AuthInterceptor::new(token)?;
            StreamClient::with_interceptor(channel, auth)
                .accept_compressed(CompressionEncoding::Gzip)
                .max_decoding_message_size(100 * 1024 * 1024) // 100MiB
        };
        let package = Package::from_url(manifest.as_str()).await?;

        let tables = Tables::from_package(package, output_module.clone()).map_err(|_| Error::AssertFail(anyhow!("failed to build tables from spkg")))?;

        Ok(SubstreamsClient { stream_client, tables, output_module })
    }

    /// Both `start` and `stop` are inclusive. Could be abstracted to handle multiple chains, but for
    /// now assumes an EVM Firehose endpoint.
    pub async fn blocks(
        &mut self,
        start: BlockNum,
        stop: BlockNum,
    ) -> Result<impl Stream<Item = Result<BlockScopedData, Error>>, Error> {
        let request = tonic::Request::new(StreamRequest {
            start_block_num: start as i64,
            stop_block_num: stop,

            start_cursor: String::new(),
            final_blocks_only: true,
            modules: self.tables.package.modules.clone(),
            production_mode: true,
            output_module: self.output_module.clone(),
            debug_initial_store_snapshot_for_modules: vec![],
        });

        let raw_stream = self.stream_client.blocks(request).await?.into_inner();
        let block_stream = raw_stream
            .err_into::<Error>()
            .and_then(|response| async move {

                match response.message {
                    Some(Message::BlockScopedData(data)) => {
                        Ok(data)
                    }
                    Some(Message::FatalError(_)) => {
                        return Err(Error::AssertFail(anyhow!("Error streaming")));
                    }
                    _ => {
                        return Ok(BlockScopedData::default());
                    }
                }
            });

        Ok(block_stream)
    }
}


impl BlockStreamer for SubstreamsClient {
    /// Once spawned, will continuously fetch blocks from the Firehose and send them through the channel.
    ///
    /// Terminates with `Ok` when the Firehose stream reaches `end_block`, or the channel receiver goes
    /// away.
    ///
    /// Terminates with `Err` when there is an error estabilishing the stream.
    ///
    /// Errors from the Firehose stream are logged and retried.
    async fn block_stream(
        mut self,
        start_block: u64,
        end_block: u64,
        tx: mpsc::Sender<DatasetRows>,
    ) -> Result<(), anyhow::Error> {

        // Explicitly track the next block in case we need to restart the Firehose stream.
        let mut next_block = start_block;

        // A retry loop for consuming the Firehose.
        'retry: loop {
            let mut stream = match self.blocks(next_block, end_block).await {
                Ok(stream) => Box::pin(stream),

                // If there is an error at the initial connection, we don't retry here as that's
                // unexpected.
                Err(err) => break Err(err.into()),
            };
            while let Some(block) = stream.next().await {
                match block {
                    Ok(block) => {
                        if block.clock.is_none() || block.output.is_none() || block.output.as_ref().unwrap().map_output.is_none(){
                            continue;
                        }
                        let block_num = block.clock.as_ref().unwrap().number;
                        let table_rows = pb_to_rows(block, &self.tables)?;

                        // Send the block and check if the receiver has gone away.
                        if tx.send(table_rows).await.is_err() {
                            break;
                        }

                        next_block = block_num + 1;
                    }
                    Err(err) => {
                        // Log and retry.
                        println!("Error reading substreams stream: {}", err);
                        continue 'retry;
                    }
                }
            }

            // The stream has ended, or the receiver has gone away, either way we hit a natural
            // termination condition.
            break Ok(());
        }
    }
}
