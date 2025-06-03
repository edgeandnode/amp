use std::{str::FromStr, time::Duration};

use async_stream::stream;
use common::{store::Store, BlockNum, BlockStreamer, BoxError, RawDatasetRows};
use futures::{Stream, StreamExt as _, TryStreamExt as _};
use pbfirehose::{stream_client::StreamClient, ForkStep, Response as StreamResponse};
use prost::Message as _;
use tonic::{
    codec::CompressionEncoding,
    metadata::{Ascii, MetadataValue},
    service::{interceptor::InterceptedService, Interceptor},
    transport::{ClientTlsConfig, Endpoint, Uri},
};

use crate::{
    dataset::{extract_provider, FirehoseProvider},
    evm::{pb_to_rows::protobufs_to_rows, pbethereum},
    proto::sf::firehose::v2 as pbfirehose,
    Error,
};

/// This client only handles final blocks.
// See also: only-final-blocks
// Cloning is cheap and shares the underlying connection.
#[derive(Clone)]
pub struct Client {
    endpoint: Endpoint,
    auth: AuthInterceptor,
    network: String,
}

impl Client {
    /// Configure the client from a Firehose dataset definition.
    pub async fn new(dataset_def: toml::Value, provider_store: &Store) -> Result<Self, Error> {
        let provider = extract_provider(dataset_def, provider_store).await?;

        let FirehoseProvider {
            url,
            token,
            network,
        } = provider;

        let client = {
            let uri = Uri::from_str(&url)?;
            let mut endpoint = Endpoint::from(uri);
            endpoint = endpoint.tls_config(ClientTlsConfig::new().with_native_roots())?;
            let auth = AuthInterceptor::new(token)?;
            Client {
                endpoint,
                auth,
                network,
            }
        };

        // Test connection
        client.connect().await?;

        Ok(client)
    }

    pub fn network(&self) -> String {
        self.network.to_string()
    }

    async fn connect(
        &self,
    ) -> Result<StreamClient<InterceptedService<tonic::transport::Channel, AuthInterceptor>>, Error>
    {
        let channel = self.endpoint.connect().await?;
        Ok(StreamClient::with_interceptor(channel, self.auth.clone())
            .accept_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Gzip)
            .max_decoding_message_size(100 * 1024 * 1024)) // 100MiB
    }

    /// Both `start` and `stop` are inclusive. Could be abstracted to handle multiple chains, but for
    /// now assumes an EVM Firehose endpoint.
    async fn blocks(
        &mut self,
        start: i64,
        stop: BlockNum,
    ) -> Result<impl Stream<Item = Result<pbethereum::Block, Error>>, Error> {
        let request = tonic::Request::new(pbfirehose::Request {
            start_block_num: start as i64,
            stop_block_num: stop,
            // See also: only-final-blocks
            final_blocks_only: true,
            cursor: String::new(),
            transforms: vec![],
        });

        // We intentionally do not store and reuse the connection, but recreate it every time.
        // This is more robust to connection failures.
        let mut client = self.connect().await?;
        let raw_stream = client.blocks(request).await?.into_inner();
        let block_stream = raw_stream
            .err_into::<Error>()
            .and_then(|response| async move {
                let StreamResponse {
                    block,
                    step,
                    cursor: _,
                } = response;
                let step = ForkStep::try_from(step).map_err(|e| Error::AssertFail(e.into()))?;

                // See also: only-final-blocks
                if step != ForkStep::StepFinal {
                    let err = format!("Only STEP_FINAL is expected, found {}", step.as_str_name());
                    return Err(Error::AssertFail(err.into()));
                }
                let Some(block) = block else {
                    return Err(Error::AssertFail("Expected block, found none".into()));
                };

                let ethereum_block = pbethereum::Block::decode(block.value.as_ref())?;
                Ok(ethereum_block)
            });

        Ok(block_stream)
    }
}

#[derive(Clone)]
pub struct AuthInterceptor {
    pub token: Option<MetadataValue<Ascii>>,
}

impl AuthInterceptor {
    pub fn new(token: Option<String>) -> Result<Self, Error> {
        Ok(AuthInterceptor {
            token: token.map_or(Ok(None), |token| {
                let bearer_token = format!("bearer {}", token);
                bearer_token.parse::<MetadataValue<Ascii>>().map(Some)
            })?,
        })
    }
}

impl std::fmt::Debug for AuthInterceptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.token {
            Some(_) => f.write_str("token_redacted"),
            None => f.write_str("no_token_configured"),
        }
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        if let Some(ref t) = self.token {
            req.metadata_mut().insert("authorization", t.clone());
        }

        Ok(req)
    }
}

impl BlockStreamer for Client {
    /// Creates a stream that continuously fetches blocks from the Firehose.
    ///
    /// The stream will yield blocks from `start_block` to `end_block`.
    ///
    /// Errors from the Firehose stream are logged and retried automatically.
    async fn block_stream(
        mut self,
        start_block: u64,
        end_block: u64,
    ) -> impl Stream<Item = Result<RawDatasetRows, BoxError>> + Send {
        const RETRY_BACKOFF: Duration = Duration::from_secs(5);

        stream! {
            // Explicitly track the next block in case we need to restart the Firehose stream.
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
                            let block_num = block.number;

                            match protobufs_to_rows(block, &self.network) {
                                Ok(table_rows) => {
                                    yield Ok(table_rows);
                                    next_block = block_num + 1;
                                }
                                Err(err) => {
                                    yield Err(format!(
                                        "error converting Protobufs to rows on block {}: {}",
                                        block_num,
                                        err
                                    ).into());
                                    return;
                                }
                            }
                        }
                        Err(err) => {
                            // Log the error and retry after `RETRY_BACKOFF` seconds
                            tracing::debug!(error=%err, "error reading firehose stream, retrying in {} seconds", RETRY_BACKOFF.as_secs());
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
        Ok(block.transpose()?.map(|block| block.number).unwrap_or(0))
    }
}
