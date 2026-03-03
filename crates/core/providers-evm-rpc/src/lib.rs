//! EVM RPC provider types, configuration, and extraction client.
//!
//! This crate provides the complete EVM RPC source implementation: the provider
//! kind identifier, configuration, HTTP/WS/IPC transport construction, and the
//! block-streaming extraction client. These types are used throughout the system
//! to represent and connect to JSON-RPC blockchain data sources.

use std::path::PathBuf;

use amp_providers_common::provider_name::ProviderName;

use crate::{config::EvmRpcProviderConfig, error::ClientError, provider::Auth};

pub mod client;
pub mod config;
pub mod error;
pub mod kind;
pub mod metrics;
pub mod provider;
pub mod tables;

pub use self::client::Client;

/// Create an EVM RPC block-streaming client from provider configuration.
pub async fn client(
    name: ProviderName,
    config: EvmRpcProviderConfig,
    meter: Option<&monitoring::telemetry::metrics::Meter>,
) -> Result<Client, ClientError> {
    let url = config.url.into_inner();
    let auth = config.auth_token.map(|token| match config.auth_header {
        Some(header) => Auth::CustomHeader {
            name: header,
            value: token,
        },
        None => Auth::Bearer(token),
    });

    let request_limit = u16::max(1, config.concurrent_request_limit.unwrap_or(1024));
    let client = match url.scheme() {
        "ipc" => {
            let path = url.path();
            Client::new_ipc(
                PathBuf::from(path),
                config.network,
                name,
                request_limit,
                config.rpc_batch_size,
                config.rate_limit_per_minute,
                config.fetch_receipts_per_tx,
                meter,
            )
            .await?
        }
        "ws" | "wss" => {
            Client::new_ws(
                url,
                config.network,
                name,
                request_limit,
                config.rpc_batch_size,
                config.rate_limit_per_minute,
                config.fetch_receipts_per_tx,
                auth,
                meter,
            )
            .await?
        }
        _ => Client::new(
            url,
            config.network,
            name,
            request_limit,
            config.rpc_batch_size,
            config.rate_limit_per_minute,
            config.fetch_receipts_per_tx,
            config.timeout,
            auth,
            meter,
        )?,
    };

    Ok(client)
}
