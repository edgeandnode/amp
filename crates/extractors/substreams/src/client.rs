use std::{
    str::FromStr,
    time::{Duration, Instant},
};

use async_stream::stream;
use common::{BlockNum, BlockStreamer, BoxError, RawDatasetRows, Table};
use firehose_datasets::{Error, client::AuthInterceptor};
use futures::{Stream, StreamExt as _, TryStreamExt as _};
use monitoring::telemetry;
use pbsubstreams::{Request as StreamRequest, response::Message, stream_client::StreamClient};
use prost::Message as _;
use tonic::{
    codec::CompressionEncoding,
    service::interceptor::InterceptedService,
    transport::{Channel, ClientTlsConfig, Endpoint, Uri},
};

use super::tables::Tables;
use crate::{
    Manifest,
    dataset::ProviderConfig,
    proto::sf::substreams::{
        rpc::v2::{self as pbsubstreams, BlockScopedData},
        v1::Package,
    },
    transform::transform,
};

// Cloning is cheap and shares the underlying connection.
#[derive(Clone)]
pub struct Client {
    stream_client: StreamClient<InterceptedService<Channel, AuthInterceptor>>,
    tables: Tables,
    package: Package,
    output_module: String,
    provider_name: String,
    network: String,
    final_blocks_only: bool,
    metrics: Option<crate::metrics::MetricsRegistry>,
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
    pub async fn new(
        config: ProviderConfig,
        manifest: Manifest,
        final_blocks_only: bool,
        meter: Option<&telemetry::metrics::Meter>,
    ) -> Result<Self, Error> {
        let metrics = meter.map(|m| crate::metrics::MetricsRegistry::new(m));

        let stream_client = {
            let uri = Uri::from_str(&config.url)?;
            let mut endpoint = Endpoint::from(uri);
            endpoint = endpoint.tls_config(ClientTlsConfig::new().with_native_roots())?;
            let channel = endpoint.connect().await?;
            let auth = AuthInterceptor::new(config.token)?;
            StreamClient::with_interceptor(channel, auth)
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip)
                .max_decoding_message_size(100 * 1024 * 1024) // 100MiB
        };
        let package = Package::from_url(manifest.manifest.as_str()).await?;
        if package.network != config.network {
            return Err(Error::AssertFail(
                format!(
                    "Package network '{}' does not match requested network '{}'",
                    package.network, config.network
                )
                .into(),
            ));
        }

        let tables = Tables::from_package(&package, &manifest.module)
            .map_err(|_| Error::AssertFail("failed to build tables from spkg".into()))?;

        Ok(Self {
            stream_client,
            package,
            tables,
            output_module: manifest.module.clone(),
            provider_name: config.name,
            network: config.network,
            final_blocks_only,
            metrics,
        })
    }

    pub fn tables(&self) -> &Vec<Table> {
        &self.tables.tables
    }

    pub fn provider_name(&self) -> &str {
        &self.provider_name
    }

    /// Both `start` and `stop` are inclusive. Could be abstracted to handle multiple chains, but for
    /// now assumes an EVM Firehose endpoint.
    pub async fn blocks(
        &mut self,
        start: i64,
        stop: BlockNum,
    ) -> Result<impl Stream<Item = Result<BlockScopedData, Error>> + use<>, Error> {
        let request = tonic::Request::new(StreamRequest {
            start_block_num: start,
            stop_block_num: stop,

            start_cursor: String::new(),
            final_blocks_only: self.final_blocks_only,
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
            let stream_start = Instant::now();
            let metrics = self.metrics.clone();
            let provider = self.provider_name.clone();
            let network = self.network.clone();
            let module = self.output_module.clone();

            // Explicitly track the next block in case we need to restart the stream.
            let mut next_block = start_block;

            'retry: loop {
                let mut stream = match self.blocks(next_block as i64, end_block).await {
                    Ok(stream) => std::pin::pin!(stream),
                    // If there is an error at the initial connection, we don't retry here as that's
                    // unexpected.
                    Err(err) => {
                        if let Some(ref metrics) = metrics {
                            metrics.record_stream_error(&provider, &network, &module, "connection");
                        }
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
                                Ok(table_rows) => {
                                    if let Some(ref metrics) = metrics {
                                        metrics.record_block_processed(&provider, &network, &module);
                                    }
                                    yield Ok(table_rows);
                                    next_block = block_num + 1;
                                }
                                Err(err) => {
                                    if let Some(ref metrics) = metrics {
                                        metrics.record_stream_error(&provider, &network, &module, "transformation");
                                    }
                                    yield Err(format!(
                                        "error converting Blockscope to rows on block {}: {}",
                                        block_num, err
                                    ).into());
                                    return;
                                }
                            }
                        }
                        Err(err) => {
                            if let Some(ref metrics) = metrics {
                                metrics.record_stream_error(&provider, &network, &module, "stream");
                            }
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

            // Record stream duration
            if let Some(ref metrics) = metrics {
                let duration = stream_start.elapsed().as_millis() as f64;
                metrics.record_stream_duration(duration, &provider, &network, &module);
            }
        }
    }

    async fn latest_block(&mut self) -> Result<Option<BlockNum>, BoxError> {
        let stream = self.blocks(-1, 0).await?;
        let mut stream = std::pin::pin!(stream);
        let block = stream.next().await;
        Ok(block
            .transpose()?
            .map(|b| b.clock.map(|c| c.number).unwrap_or(b.final_block_height)))
    }

    fn provider_name(&self) -> &str {
        &self.provider_name
    }
}
