//! Firehose provider types, configuration, and extraction client.
//!
//! This crate provides the complete Firehose source implementation: the provider
//! kind identifier, configuration, gRPC transport construction, and the
//! block-streaming extraction client.

use amp_providers_common::provider_name::ProviderName;

use crate::{config::FirehoseProviderConfig, error::ClientError};

pub mod client;
pub mod config;
pub mod error;
pub mod evm;
pub mod kind;
pub mod metrics;
#[expect(clippy::enum_variant_names)]
mod proto;
pub mod tables;

pub use self::client::Client;

/// Create a Firehose block-streaming client from provider configuration.
pub async fn client(
    name: ProviderName,
    config: FirehoseProviderConfig,
    meter: Option<&monitoring::telemetry::metrics::Meter>,
) -> Result<Client, ClientError> {
    Client::new(name, config, meter).await
}
