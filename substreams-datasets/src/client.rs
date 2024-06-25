use anyhow::Context as _;
use prost::Message as _;
use std::{str::FromStr as _, time::Duration};
use tokio::sync::mpsc;

use firehose_datasets::{client::AuthInterceptor, Error};

use futures::{Stream, StreamExt as _, TryStreamExt as _};
use tonic::{
    codec::CompressionEncoding,
    service::interceptor::InterceptedService,
    transport::{Channel, Endpoint, Uri},
};

use super::tables::Tables;
use crate::{dataset::extract_def_and_provider, proto::sf::substreams::v1::Package};
use crate::{
    proto::sf::substreams::rpc::v2::{self as pbsubstreams, BlockScopedData},
    transform::transform,
};
use common::{BlockNum, BlockStreamer, BoxError, DatasetRows, Store, Table};
use pbsubstreams::{response::Message, stream_client::StreamClient, Request as StreamRequest};

// Cloning is cheap and shares the underlying connection.
#[derive(Clone)]
pub struct Client {
    pub stream_client: StreamClient<InterceptedService<Channel, AuthInterceptor>>,
    pub tables: Tables,
    pub package: Package,
    pub output_module: String,
    pub network: String,
}

impl Package {
    pub async fn from_url(url: &str) -> Result<Self, Error> {
        let url = reqwest::Url::parse(url)
            .map_err(|_| Error::AssertFail("failed to parse spkg url".into()))?;

        let response = reqwest::get(url)
            .await
            .map_err(|_| Error::AssertFail("failed to get spkg".into()))?;
        if !response.status().is_success() {
            return Err(Error::AssertFail("Failed to fetch package from URL".into()));
        }

        let bytes = response
            .bytes()
            .await
            .map_err(|_| Error::AssertFail("failed to read response bytes".into()))?;
        let decoded = Self::decode(bytes.as_ref())?;

        Ok(decoded)
    }
}

impl Client {
    pub async fn new(raw_config: toml::Value, provider_store: &Store) -> Result<Self, Error> {
        let (def, provider) = extract_def_and_provider(raw_config, provider_store).await?;

        let stream_client = {
            let uri = Uri::from_str(&provider.url)?;
            let channel = Endpoint::from(uri).connect().await?;
            let auth = AuthInterceptor::new(provider.token)?;
            StreamClient::with_interceptor(channel, auth)
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip)
                .max_decoding_message_size(100 * 1024 * 1024) // 100MiB
        };
        let package = Package::from_url(def.manifest.as_str()).await?;
        let network = package.network.clone();

        let tables = Tables::from_package(&package, &def.module)
            .map_err(|_| Error::AssertFail("failed to build tables from spkg".into()))?;

        Ok(Self {
            stream_client,
            package,
            tables,
            output_module: def.module,
            network,
        })
    }

    pub fn tables(&self) -> &Vec<Table> {
        &self.tables.tables
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
            modules: self.package.modules.clone(),
            production_mode: true,
            output_module: self.output_module.clone(),
            debug_initial_store_snapshot_for_modules: vec![],
        });

        let raw_stream = self.stream_client.blocks(request).await?.into_inner();
        let block_stream = raw_stream
            .err_into::<Error>()
            .and_then(|response| async move {
                match response.message {
                    Some(Message::BlockScopedData(data)) => Ok(data),
                    Some(Message::FatalError(_)) => {
                        return Err(Error::AssertFail("Substreams server error".into()));
                    }
                    Some(Message::BlockUndoSignal(_)) => {
                        return Err(Error::AssertFail("Block undo signal received".into()));
                    }
                    // ignore progress, session, snapshot messages
                    _ => {
                        return Ok(BlockScopedData::default());
                    }
                }
            });

        Ok(block_stream)
    }
}

impl BlockStreamer for Client {
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
    ) -> Result<(), BoxError> {
        // Explicitly track the next block in case we need to restart the Firehose stream.
        let mut next_block = start_block;
        const RETRY_BACKOFF: Duration = Duration::from_secs(5);

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
                        if block.clock.is_none()
                            || block.output.is_none()
                            || block.output.as_ref().unwrap().map_output.is_none()
                        {
                            continue;
                        }
                        let block_num = block.clock.as_ref().unwrap().number;
                        let table_rows = transform(block, &self.tables).context(format!(
                            "error converting Blockscope to rows for block {block_num}"
                        ))?;
                        if table_rows.is_empty() {
                            continue;
                        }

                        // Send the block and check if the receiver has gone away.
                        if tx.send(table_rows).await.is_err() {
                            break;
                        }

                        next_block = block_num + 1;
                    }
                    Err(err) => {
                        // Log and retry.
                        log::debug!("error reading substreams stream, retrying in {} seconds, error message: {}", RETRY_BACKOFF.as_secs(), err);
                        tokio::time::sleep(RETRY_BACKOFF).await;
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
