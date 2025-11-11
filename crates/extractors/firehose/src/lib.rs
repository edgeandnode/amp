//! EVM block data is sufficiently complicated that there may be multiple encoding flavors, each with
//! multiple versions. There is no universal encoding, and we're not going to try to enforce one.
//! Each extraction layer can have its own data format. This `firehose` crate defines Firehose
//! data formats and provides a client to fetch them from a Firehose gRPC endpoint.
use common::{BoxError, store::StoreError};
use tonic::{codegen::http::uri::InvalidUri, metadata::errors::InvalidMetadataValue};

pub mod client;
pub mod dataset;
mod dataset_kind;
pub mod evm;
pub mod metrics;
#[expect(clippy::doc_overindented_list_items)]
#[expect(clippy::enum_variant_names)]
mod proto;

pub use client::Client;

pub use self::dataset_kind::{FirehoseDatasetKind, FirehoseDatasetKindError};

/// Errors that can occur when working with Firehose data sources.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// HTTP/2 connection error occurred while connecting to Firehose endpoint
    #[error("HTTP/2 connection error: {0}")]
    Connection(#[source] tonic::transport::Error),
    /// gRPC call error occurred during communication with Firehose
    #[error("gRPC call error: {0}")]
    Call(#[source] tonic::Status),
    /// Protocol Buffers decoding error occurred while parsing Firehose data
    #[error("ProtocolBuffers decoding error: {0}")]
    PbDecodeError(#[source] prost::DecodeError),
    /// Internal assertion failure
    #[error("Assertion failure: {0}")]
    AssertFail(BoxError),
    /// URI parsing error occurred while parsing Firehose endpoint URL
    #[error("URL parse error: {0}")]
    UriParse(#[source] InvalidUri),
    /// Invalid authentication token metadata value
    #[error("invalid auth token: {0}")]
    Utf8(#[source] InvalidMetadataValue),
    /// Store error occurred during data storage operations
    #[error("store error: {0}")]
    StoreError(#[source] StoreError),
}
