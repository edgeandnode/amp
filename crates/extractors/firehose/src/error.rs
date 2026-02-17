use tonic::{codegen::http::uri::InvalidUri, metadata::errors::InvalidMetadataValue};

/// Errors that can occur when working with Firehose data sources.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// HTTP/2 connection error occurred while connecting to Firehose endpoint
    #[error("HTTP/2 connection error")]
    Connection(#[source] tonic::transport::Error),
    /// gRPC call error occurred during communication with Firehose
    #[error("gRPC call error")]
    Call(#[source] tonic::Status),
    /// Protocol Buffers decoding error occurred while parsing Firehose data
    #[error("ProtocolBuffers decoding error")]
    PbDecodeError(#[source] prost::DecodeError),
    /// Internal assertion failure
    #[error("Assertion failure")]
    AssertFail(String),
    /// URI parsing error occurred while parsing Firehose endpoint URL
    #[error("URL parse error")]
    UriParse(#[source] InvalidUri),
    /// Invalid authentication token metadata value
    #[error("invalid auth token")]
    Utf8(#[source] InvalidMetadataValue),
}
