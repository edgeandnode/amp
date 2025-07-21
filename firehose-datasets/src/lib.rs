//! EVM block data is sufficiently complicated that there may be multiple encoding flavors, each with
//! multiple versions. There is no universal encoding, and we're not going to try to enforce one.
//! Each extraction layer can have its own data format. This `firehose` crate defines Firehose
//! data formats and provides a client to fetch them from a Firehose gRPC endpoint.

pub mod client;
pub mod evm;

pub use client::Client;
use common::{BoxError, store::StoreError};
pub use dataset::DATASET_KIND;
use thiserror::Error;
use tonic::{codegen::http::uri::InvalidUri, metadata::errors::InvalidMetadataValue};

#[derive(Error, Debug)]
pub enum Error {
    #[error("HTTP/2 connection error: {0}")]
    Connection(#[from] tonic::transport::Error),
    #[error("gRPC call error: {0}")]
    Call(#[from] tonic::Status),
    #[error("ProtocolBuffers decoding error: {0}")]
    PbDecodeError(#[from] prost::DecodeError),
    #[error("Assertion failure: {0}")]
    AssertFail(BoxError),
    #[error("URL parse error: {0}")]
    UriParse(#[from] InvalidUri),
    #[error("invalid auth token: {0}")]
    Utf8(#[from] InvalidMetadataValue),
    #[error("store error: {0}")]
    StoreError(#[from] StoreError),
    #[error("TOML parse error: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),
}

pub mod dataset;
mod proto;
