use crate::evm::pbethereum;
use crate::proto::sf::firehose::v2 as pbfirehose;

use std::str::FromStr;

use anyhow::anyhow;
use common::BlockNum;
use futures::{Stream, TryStreamExt as _};
use pbfirehose::stream_client::StreamClient;
use pbfirehose::ForkStep;
use pbfirehose::Response as StreamResponse;
use prost::Message as _;
use serde::Deserialize;
use thiserror::Error;
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
    #[error("HTTP connection error: {0}")]
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
    stream_client: StreamClient<InterceptedService<tonic::transport::Channel, AuthInterceptor>>,
}

impl Client {
    /// Configure the client from an EVM Firehose endpoint.
    pub async fn new(cfg: FirehoseProvider) -> Result<Self, Error> {
        let FirehoseProvider { url, token } = cfg;
        let stream_client = {
            let uri = Uri::from_str(&url)?;
            let channel = Endpoint::from(uri).connect().await?;
            let auth = AuthInterceptor::new(token)?;
            StreamClient::with_interceptor(channel, auth)
                .accept_compressed(CompressionEncoding::Gzip)
                .max_decoding_message_size(100 * 1024 * 1024) // 100MiB
        };

        Ok(Client { stream_client })
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

        let raw_stream = self.stream_client.blocks(request).await?.into_inner();
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
struct AuthInterceptor {
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
