use crate::substreams::client::SubstreamsClient;
use crate::evm::client::EvmClient;


use common::BlockStreamer;
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::mpsc;
use tonic::codegen::http::uri::InvalidUri;
use tonic::metadata::errors::InvalidMetadataValue;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::Interceptor;

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

#[derive(Clone)]
pub enum BlockStreamerClient {
    EvmClient(EvmClient),
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
            BlockStreamerClient::EvmClient(client) => client.block_stream(start_block, end_block, tx).await,
            BlockStreamerClient::SubstreamsClient(substreams_client) => substreams_client.block_stream(start_block, end_block, tx).await,
        }
    }
}
