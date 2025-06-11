use std::{str::FromStr, time::Duration};

use async_stream::stream;
use common::{BlockNum, BlockStreamer, BoxError, RawDatasetRows, Table};
use firehose_datasets::{client::AuthInterceptor, Error};
use futures::{Stream, StreamExt as _, TryStreamExt as _};
use pbsubstreams::{response::Message, stream_client::StreamClient, Request as StreamRequest};
use prost::Message as _;
use tonic::{
    codec::CompressionEncoding,
    service::interceptor::InterceptedService,
    transport::{Channel, ClientTlsConfig, Endpoint, Uri},
};

use super::tables::Tables;
use crate::{
    dataset::SubstreamsProvider,
    proto::sf::substreams::{
        rpc::v2::{self as pbsubstreams, BlockScopedData},
        v1::Package,
    },
    transform::transform,
    DatasetDef,
};

/// This client only handles final blocks.
// See also: only-final-blocks
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
    pub async fn new(provider: toml::Value, dataset: &str, network: String) -> Result<Self, Error> {
        let provider: SubstreamsProvider = provider.try_into()?;
        let dataset_def: DatasetDef = toml::from_str(dataset)?;

        let stream_client = {
            let uri = Uri::from_str(&provider.url)?;
            let mut endpoint = Endpoint::from(uri);
            endpoint = endpoint.tls_config(ClientTlsConfig::new().with_native_roots())?;
            let channel = endpoint.connect().await?;
            let auth = AuthInterceptor::new(provider.token)?;
            StreamClient::with_interceptor(channel, auth)
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip)
                .max_decoding_message_size(100 * 1024 * 1024) // 100MiB
        };
        let package = Package::from_url(dataset_def.manifest.as_str()).await?;
        if package.network != network {
            return Err(Error::AssertFail(
                format!(
                    "Package network '{}' does not match requested network '{}'",
                    package.network, network
                )
                .into(),
            ));
        }

        let tables = Tables::from_package(&package, &dataset_def.module)
            .map_err(|_| Error::AssertFail("failed to build tables from spkg".into()))?;

        Ok(Self {
            stream_client,
            package,
            tables,
            output_module: dataset_def.module,
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
        start: i64,
        stop: BlockNum,
    ) -> Result<impl Stream<Item = Result<BlockScopedData, Error>>, Error> {
        let request = tonic::Request::new(StreamRequest {
            start_block_num: start as i64,
            stop_block_num: stop,

            start_cursor: String::new(),
            // See also: only-final-blocks
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
    /// Creates a stream that continuously fetches blocks from Substreams.
    ///
    /// Errors from the Substreams stream are logged and retried automatically.
    async fn block_stream(
        mut self,
        start_block: BlockNum,
        end_block: BlockNum,
    ) -> impl Stream<Item = Result<RawDatasetRows, BoxError>> + Send {
        const RETRY_BACKOFF: Duration = Duration::from_secs(5);

        stream! {
            // Explicitly track the next block in case we need to restart the stream.
            let mut next_block = start_block;

            'retry: loop {
                let mut stream = match self.blocks(next_block as i64, end_block).await {
                    Ok(stream) => std::pin::pin!(stream),
                    // If there is an error at the initial connection, we don't retry here as that's
                    // unexpected.
                    Err(err) => {
                        yield Err(err.into());
                        return;
                    }
                };

                while let Some(block_result) = stream.next().await {
                    match block_result {
                        Ok(block) => {
                            let block_num = match (&block.clock, &block.output) {
                                (Some(clock), Some(output)) if output.map_output.is_some() => {
                                    clock.number
                                }
                                _ => continue,
                            };

                            match transform(block, &self.tables) {
                                Ok(table_rows) if table_rows.is_empty() => continue,
                                Ok(table_rows) => {
                                    yield Ok(table_rows);
                                    next_block = block_num + 1;
                                }
                                Err(err) => {
                                    yield Err(format!(
                                        "error converting Blockscope to rows on block {}: {}",
                                        block_num, err
                                    ).into());
                                    return;
                                }
                            }
                        }
                        Err(err) => {
                            // Log the error and retry after `RETRY_BACKOFF` seconds
                            tracing::debug!(error=%err, "error reading substreams stream, retrying in {} seconds", RETRY_BACKOFF.as_secs());
                            tokio::time::sleep(RETRY_BACKOFF).await;
                            continue 'retry;
                        }
                    }
                }

                // The stream has ended, or the receiver has gone away,
                // either way we hit a natural termination condition
                break;
            }
        }
    }

    async fn latest_block(&mut self, finalized: bool) -> Result<BlockNum, BoxError> {
        // See also: only-final-blocks
        _ = finalized;
        let stream = self.blocks(-1, 0).await?;
        let mut stream = std::pin::pin!(stream);
        let block = stream.next().await;
        Ok(block
            .transpose()?
            .map(|b| b.final_block_height)
            .unwrap_or(0))
    }
}
