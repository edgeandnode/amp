use crate::evm::pbethereum;
use crate::proto::sf::firehose::v2 as pbfirehose;

use std::str::FromStr;
use std::time::Duration;

use anyhow::anyhow;
use anyhow::Context as _;
use common::BlockNum;
use common::BlockStreamer;
use common::DatasetRows;
use futures::StreamExt as _;
use futures::{Stream, TryStreamExt as _};
use log::debug;
use pbfirehose::stream_client::StreamClient;
use pbfirehose::ForkStep;
use pbfirehose::Response as StreamResponse;
use prost::Message as _;
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::mpsc;
use tonic::codec::CompressionEncoding;
use tonic::codegen::http::uri::InvalidUri;
use tonic::metadata::errors::InvalidMetadataValue;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::interceptor::InterceptedService;
use tonic::service::Interceptor;
use tonic::transport::Endpoint;
use tonic::transport::Uri;

#[derive(Error, Debug)]
pub enum Error {
    #[error("HTTP/2 connection error: {0}")]
    Connection(#[from] tonic::transport::Error),
    #[error("gRPC call error: {0}")]
    Call(#[from] tonic::Status),
    #[error("ProtocolBuffers decoding error: {0}")]
    PbDecodeError(#[from] prost::DecodeError),
    #[error("Assertion failure: {0}")]
    AssertFail(anyhow::Error),
    #[error("URL parse error: {0}")]
    UriParse(#[from] InvalidUri),
    #[error("Invalid auth token: {0}")]
    Utf8(#[from] InvalidMetadataValue),
}

#[derive(Debug, Deserialize)]
pub struct FirehoseProvider {
    pub url: String,
    pub token: Option<String>,
}

// Cloning is cheap and shares the underlying connection.
#[derive(Clone)]
pub struct Client {
    endpoint: Endpoint,
    auth: AuthInterceptor,
}

impl Client {
    /// Configure the client from an EVM Firehose endpoint.
    pub async fn new(cfg: FirehoseProvider) -> Result<Self, Error> {
        let FirehoseProvider { url, token } = cfg;
        let client = {
            let uri = Uri::from_str(&url)?;
            let endpoint = Endpoint::from(uri);
            let auth = AuthInterceptor::new(token)?;
            Client { endpoint, auth }
        };

        // Test connection
        client.connect().await?;

        Ok(client)
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
    pub async fn blocks(
        &mut self,
        start: BlockNum,
        stop: BlockNum,
    ) -> Result<impl Stream<Item = Result<pbethereum::Block, Error>>, Error> {
        let request = tonic::Request::new(pbfirehose::Request {
            start_block_num: start as i64,
            stop_block_num: stop,

            // We don't handle non-final blocks yet.
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
                let step = ForkStep::try_from(step)?;

                // Assert we have a final block.
                // See also: only-final-blocks
                if step != ForkStep::StepFinal {
                    let err = anyhow!("Only STEP_FINAL is expected, found {}", step.as_str_name());
                    return Err(Error::AssertFail(err));
                }
                let Some(block) = block else {
                    return Err(Error::AssertFail(anyhow!("Expected block, found none")));
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
        use crate::evm::pb_to_rows::protobufs_to_rows;

        const RETRY_BACKOFF: Duration = Duration::from_secs(5);

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
                        let block_num = block.number;

                        let table_rows = protobufs_to_rows(block)
                            .context("error converting Protobufs to rows")?;

                        // Send the block and check if the receiver has gone away.
                        if tx.send(table_rows).await.is_err() {
                            break;
                        }

                        next_block = block_num + 1;
                    }
                    Err(err) => {
                        // Log and retry.
                        debug!("error reading firehose stream, retrying in {} seconds, error message: {}", RETRY_BACKOFF.as_secs(), err);
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
