//! Solana provider types, configuration, and extraction client.
//!
//! This crate provides the complete Solana source implementation: the provider
//! kind identifier, configuration, RPC transport, Old Faithful archive streaming,
//! and the block-streaming extraction client.
//!
//! ### Important note - Slot vs. Block Number
//!
//! In Solana, each produced block gets placed in a "slot", which is a specific time interval
//! during which a validator can propose a block. However, not every slot results in a produced
//! block; some slots may be skipped due to various reasons such as network issues or validator
//! performance. Therefore, the slot number does not always correspond directly to a block number.
//!
//! Since [`datasets_raw::client::BlockStreamer`] and related infrastructure generally operate
//! on the concept of block numbers, the implementation treats Solana slots as block numbers.
//! Skipped slots do not produce any rows, resulting in gaps in the block number sequence. Chain
//! integrity is maintained through hash-based validation where each block's parent_hash must
//! match the previous block's hash.

use amp_providers_common::provider_name::ProviderName;
use solana_client::rpc_config::CommitmentConfig;

use crate::{config::AuthHeaderName, error::ClientError};

pub mod client;
pub mod config;
pub mod error;
pub mod kind;
pub mod metrics;
pub mod of1_client;
pub mod rpc_client;
pub mod tables;

pub use self::client::{
    Client, TRUNCATED_LOG_MESSAGES_MARKER, non_empty_of1_slot, non_empty_rpc_slot,
};

/// Create a Solana block-streaming client from provider configuration.
pub fn client(
    name: ProviderName,
    config: config::SolanaProviderConfig,
    meter: Option<&monitoring::telemetry::metrics::Meter>,
) -> Result<Client, ClientError> {
    if config.network != "mainnet" {
        return Err(ClientError::UnsupportedNetwork {
            network: config.network,
        });
    }

    match config.rpc_provider_info.url.scheme() {
        "http" | "https" => {}
        scheme => {
            return Err(ClientError::UnsupportedUrlScheme {
                scheme: scheme.to_string(),
            });
        }
    }
    match config
        .fallback_rpc_provider_info
        .as_ref()
        .map(|info| info.url.scheme())
    {
        Some("http") | Some("https") | None => {}
        Some(scheme) => {
            return Err(ClientError::UnsupportedUrlScheme {
                scheme: scheme.to_string(),
            });
        }
    }

    let commitment = commitment_config(config.commitment);

    // Resolve authentication configuration
    let main_rpc_provider_auth = config.rpc_provider_info.auth_token.map(|token| {
        let token = token.into_inner();
        let header = config
            .rpc_provider_info
            .auth_header
            .map(AuthHeaderName::into_inner);
        rpc_client::Auth::new(token, header)
    });

    let main_rpc_connection_info = rpc_client::RpcProviderConnectionInfo {
        url: config.rpc_provider_info.url,
        auth: main_rpc_provider_auth,
    };
    let fallback_rpc_connection_info = config.fallback_rpc_provider_info.map(|info| {
        let auth = info.auth_token.map(|token| {
            let token = token.into_inner();
            let header = info.auth_header.map(AuthHeaderName::into_inner);
            rpc_client::Auth::new(token, header)
        });
        rpc_client::RpcProviderConnectionInfo {
            url: info.url,
            auth,
        }
    });

    let client = Client::new(
        main_rpc_connection_info,
        fallback_rpc_connection_info,
        config.max_rpc_calls_per_second,
        config.network,
        name,
        config.use_archive,
        commitment,
        meter,
    );

    Ok(client)
}

/// Convert a [`config::CommitmentLevel`] config value into a Solana [`CommitmentConfig`].
pub fn commitment_config(level: config::CommitmentLevel) -> CommitmentConfig {
    match level {
        config::CommitmentLevel::Processed => CommitmentConfig::processed(),
        config::CommitmentLevel::Confirmed => CommitmentConfig::confirmed(),
        config::CommitmentLevel::Finalized => CommitmentConfig::finalized(),
    }
}
