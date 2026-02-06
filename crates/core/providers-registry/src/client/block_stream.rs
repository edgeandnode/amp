//! Client creation for raw dataset providers.

use async_stream::stream;
use datasets_common::{block_num::BlockNum, network_id::NetworkId};
use datasets_raw::{
    client::{BlockStreamError, BlockStreamer, CleanupError, LatestBlockError},
    rows::Rows,
};
use evm_rpc_datasets::EvmRpcDatasetKind;
use firehose_datasets::FirehoseDatasetKind;
use futures::Stream;
use monitoring::telemetry::metrics::Meter;
use solana_datasets::SolanaDatasetKind;

use crate::config::{ParseConfigError, ProviderConfig};

/// Creates a block stream client from provider configuration. Supports EVM RPC, Solana, and Firehose providers.
pub async fn create(
    config: ProviderConfig,
    meter: Option<&Meter>,
) -> Result<BlockStreamClient, CreateClientError> {
    let provider_name = config.name.clone();

    if config.kind == EvmRpcDatasetKind {
        let typed_config =
            config
                .try_into_config()
                .map_err(|err| CreateClientError::ConfigParse {
                    name: provider_name.clone(),
                    source: err,
                })?;
        evm_rpc_datasets::client(typed_config, meter)
            .await
            .map(BlockStreamClient::EvmRpc)
            .map_err(|err| CreateClientError::ProviderClient {
                name: provider_name,
                source: ProviderClientError::EvmRpc(err),
            })
    } else if config.kind == SolanaDatasetKind {
        let typed_config =
            config
                .try_into_config()
                .map_err(|err| CreateClientError::ConfigParse {
                    name: provider_name.clone(),
                    source: err,
                })?;
        solana_datasets::extractor(typed_config, meter)
            .map(BlockStreamClient::Solana)
            .map_err(|err| CreateClientError::ProviderClient {
                name: provider_name,
                source: ProviderClientError::Solana(err),
            })
    } else if config.kind == FirehoseDatasetKind {
        let typed_config =
            config
                .try_into_config()
                .map_err(|err| CreateClientError::ConfigParse {
                    name: provider_name.clone(),
                    source: err,
                })?;
        firehose_datasets::Client::new(typed_config, meter)
            .await
            .map(|c| BlockStreamClient::Firehose(Box::new(c)))
            .map_err(|err| CreateClientError::ProviderClient {
                name: provider_name,
                source: ProviderClientError::Firehose(err),
            })
    } else {
        Err(CreateClientError::UnsupportedKind {
            kind: config.kind.to_string(),
        })
    }
}

/// Unified client type for all raw data providers.
#[derive(Clone)]
pub enum BlockStreamClient {
    EvmRpc(evm_rpc_datasets::JsonRpcClient),
    Solana(solana_datasets::SolanaExtractor),
    Firehose(Box<firehose_datasets::Client>),
}

impl BlockStreamer for BlockStreamClient {
    async fn block_stream(
        self,
        start_block: BlockNum,
        end_block: BlockNum,
    ) -> impl Stream<Item = Result<Rows, BlockStreamError>> + Send {
        // Each client returns a different concrete stream type, so we
        // use `stream!` to unify them into a wrapper stream
        stream! {
            match self {
                Self::EvmRpc(client) => {
                    let stream = client.block_stream(start_block, end_block).await;
                    for await item in stream {
                        yield item;
                    }
                }
                Self::Solana(client) => {
                    let stream = client.block_stream(start_block, end_block).await;
                    for await item in stream {
                        yield item;
                    }
                }
                Self::Firehose(client) => {
                    let stream = client.block_stream(start_block, end_block).await;
                    for await item in stream {
                        yield item;
                    }
                }
            }
        }
    }

    async fn latest_block(
        &mut self,
        finalized: bool,
    ) -> Result<Option<BlockNum>, LatestBlockError> {
        match self {
            Self::EvmRpc(client) => client.latest_block(finalized).await,
            Self::Solana(client) => client.latest_block(finalized).await,
            Self::Firehose(client) => client.latest_block(finalized).await,
        }
    }

    async fn wait_for_cleanup(self) -> Result<(), CleanupError> {
        match self {
            Self::EvmRpc(client) => client.wait_for_cleanup().await,
            Self::Solana(client) => client.wait_for_cleanup().await,
            Self::Firehose(client) => client.wait_for_cleanup().await,
        }
    }

    fn provider_name(&self) -> &str {
        match self {
            Self::EvmRpc(client) => client.provider_name(),
            Self::Solana(client) => client.provider_name(),
            Self::Firehose(client) => client.provider_name(),
        }
    }
}

/// Errors that can occur when creating a client for a raw dataset provider.
#[derive(Debug, thiserror::Error)]
pub enum CreateClientError {
    /// Failed to parse provider configuration.
    ///
    /// This occurs when the provider configuration cannot be deserialized into the
    /// expected type for the dataset kind (EvmRpc, Solana, or Firehose).
    #[error("failed to parse provider config for '{name}'")]
    ConfigParse {
        name: String,
        #[source]
        source: ParseConfigError,
    },

    /// Failed to create provider client.
    ///
    /// This wraps provider-specific errors (EVM RPC, Solana, or Firehose) that occur
    /// during client initialization.
    #[error("failed to create client for provider '{name}'")]
    ProviderClient {
        name: String,
        #[source]
        source: ProviderClientError,
    },

    /// No provider found for the given kind and network.
    ///
    /// This occurs when:
    /// - No provider is configured for the specific kind-network pair
    /// - All providers for this kind-network are disabled
    /// - Provider configuration files are missing or invalid
    #[error("no provider found for kind '{kind}' and network '{network}'")]
    ProviderNotFound { kind: String, network: NetworkId },

    /// Unsupported provider kind.
    ///
    /// This occurs when the provider kind does not match any supported raw dataset type.
    #[error("unsupported provider kind: {kind}")]
    UnsupportedKind { kind: String },
}

/// Provider-specific client creation errors.
///
/// This enum groups errors from different provider types (EVM RPC, Solana, Firehose)
/// that can occur during client initialization.
#[derive(Debug, thiserror::Error)]
pub enum ProviderClientError {
    /// Failed to create EVM RPC client.
    ///
    /// This occurs during initialization of the EVM RPC client, which may fail due to
    /// invalid RPC URLs, connection issues, or authentication failures.
    #[error("failed to create EVM RPC client")]
    EvmRpc(#[source] evm_rpc_datasets::error::ProviderError),

    /// Failed to create Solana extractor.
    ///
    /// This occurs during initialization of the Solana extractor, which may fail due to
    /// invalid or unsupported URL schemes.
    #[error("failed to create Solana extractor")]
    Solana(#[source] solana_datasets::error::ExtractorError),

    /// Failed to create Firehose client.
    ///
    /// This occurs during initialization of the Firehose client, which may fail due to
    /// invalid gRPC endpoints, connection issues, or authentication failures.
    #[error("failed to create Firehose client")]
    Firehose(#[source] firehose_datasets::Error),
}
