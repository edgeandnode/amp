//! EVM RPC provider creation.

use crate::config::{EvmRpcProviderConfig, ParseConfigError, ProviderConfig};

/// Type alias for an EVM RPC provider that works with any network.
///
/// This is an alloy `RootProvider` configured with `AnyNetwork`, allowing it to work
/// with any EVM-compatible blockchain network. The provider is constructed by
/// `create` based on the URL scheme in the provider configuration.
pub type EvmRpcProvider = alloy::providers::RootProvider<alloy::network::AnyNetwork>;

/// Create an EVM RPC provider from configuration.
///
/// This function parses the provider configuration and constructs an EVM RPC provider.
/// It automatically detects whether to use IPC or HTTP/HTTPS based on the URL scheme
/// in the provider configuration.
pub async fn create(config: ProviderConfig) -> Result<EvmRpcProvider, CreateEvmRpcClientError> {
    use evm_rpc_datasets::provider as evm_provider;

    let provider_name = config.name.clone();
    let typed_config = config
        .try_into_config::<EvmRpcProviderConfig>()
        .map_err(|source| CreateEvmRpcClientError::ConfigParse {
            name: provider_name,
            source,
        })?;

    // Construct the provider based on URL scheme
    let provider = if typed_config.url.scheme() == "ipc" {
        evm_provider::new_ipc(typed_config.url.path(), typed_config.rate_limit_per_minute)
            .await
            .map_err(CreateEvmRpcClientError::IpcConnection)?
    } else {
        evm_provider::new(typed_config.url, typed_config.rate_limit_per_minute)
    };

    Ok(provider)
}

/// Errors that occur when creating an EVM RPC client.
#[derive(Debug, thiserror::Error)]
pub enum CreateEvmRpcClientError {
    /// Failed to parse the provider configuration into the expected type.
    ///
    /// This occurs when a provider was found for the network but its configuration
    /// could not be deserialized into `EvmRpcProviderConfig`.
    ///
    /// Possible causes:
    /// - Missing required `url` field in provider configuration
    /// - Invalid URL format in the `url` field
    /// - Invalid value type for `rate_limit_per_minute` (must be positive integer)
    #[error("failed to parse provider configuration for '{name}'")]
    ConfigParse {
        name: String,
        #[source]
        source: ParseConfigError,
    },

    /// Failed to establish IPC connection.
    ///
    /// This occurs when using an IPC URL scheme and the connection to the provider socket
    /// fails due to socket unavailability, permission issues, or other IPC-specific problems.
    #[error("failed to establish IPC connection")]
    IpcConnection(#[source] evm_rpc_datasets::error::ProviderError),
}
