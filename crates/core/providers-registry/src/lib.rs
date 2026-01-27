//! Provider configuration management for dataset stores.
//!
//! This module provides functionality to manage external data source provider configurations
//! (such as EVM RPC endpoints, Firehose endpoints, etc.) that datasets connect to.
//! Provider configurations are defined as TOML files.
//!
//! ## Provider Configuration Structure
//!
//! All provider configurations must define at least:
//! - `name`: A unique identifier for the provider configuration
//! - `kind`: The type of provider (e.g., "evm-rpc", "firehose")
//! - `network`: The blockchain network (e.g., "mainnet", "goerli", "polygon")
//!
//! Additional fields depend on the provider type.
use std::{collections::BTreeMap, ops::Deref};

use datasets_common::dataset_kind_str::DatasetKindStr;
use monitoring::{logging, telemetry::metrics::Meter};
use object_store::ObjectStore;
use rand::seq::SliceRandom as _;

mod client;
mod config;
mod env_subs;
mod store;

pub use self::{
    client::{
        block_stream::{BlockStreamClient, CreateClientError},
        evm_rpc::{CreateEvmRpcClientError, EvmRpcProvider},
    },
    config::{ParseConfigError, ProviderConfig},
    store::{ConfigDeleteError, ConfigStoreError, ProviderConfigsStore},
};

/// Manages provider configurations with caching
///
/// ## Object Store Agnostic Design
///
/// The store logic is agnostic to rate-limiting and location prefixing. API users must provide
/// the appropriate object store implementation to the struct constructor:
///
/// - **Rate Limiting**: Use `object_store::limit::LimitStore` to wrap the underlying store
/// - **Path Prefixing**: Use `object_store::prefix::PrefixStore` or equivalent for path isolation
/// - **Composition**: These can be combined as needed (e.g., `LimitStore<PrefixStore<LocalFileSystem>>`)
///
/// ## Caching
///
/// Caching is handled by the underlying [`ProviderConfigsStore`]. See its documentation for
/// details on the lazy-loaded, write-through caching strategy.
#[derive(Debug, Clone)]
pub struct ProvidersRegistry<S: ObjectStore = std::sync::Arc<dyn ObjectStore>> {
    store: ProviderConfigsStore<S>,
}

impl<S> ProvidersRegistry<S>
where
    S: ObjectStore + Clone,
{
    /// Creates a new registry wrapping the given provider configs store.
    pub fn new(store: ProviderConfigsStore<S>) -> Self {
        Self { store }
    }

    /// Get all provider configurations, using cache if available
    ///
    /// Returns a read guard that dereferences to the cached `BTreeMap<String, ProviderConfig>`.
    /// This provides efficient access to all provider configurations without cloning.
    ///
    /// # Deadlock Warning
    ///
    /// The returned guard holds a read lock on the internal cache. Holding this guard
    /// for extended periods can cause deadlocks with operations that require write access
    /// (such as `register` and `delete`). Extract the needed data immediately and drop
    /// the guard as soon as possible.
    #[must_use]
    pub async fn get_all(&self) -> impl Deref<Target = BTreeMap<String, ProviderConfig>> + '_ {
        self.store.get_all().await
    }

    /// Get a provider configuration by `name`, using cache if available. Returns None if not found.
    pub async fn get_by_name(&self, name: &str) -> Option<ProviderConfig> {
        self.store.get(name).await
    }

    /// Register a provider configuration in both cache and store
    ///
    /// If a provider configuration with the same name already exists, it will be overwritten.
    pub async fn register(&self, provider: ProviderConfig) -> Result<(), RegisterError> {
        let name = provider.name.clone();

        self.store
            .store(&name, provider)
            .await
            .map_err(RegisterError)
    }

    /// Delete a provider configuration by name from both the store and cache
    ///
    /// This operation is idempotent: deleting a non-existent provider succeeds silently.
    ///
    /// # Cache Management
    /// - If file not found: removes stale cache entry and returns `Ok(())`
    /// - If other store errors: preserves cache (file may still exist) and propagates error
    /// - If deletion succeeds: removes from both store and cache
    pub async fn delete(&self, name: &str) -> Result<(), DeleteError> {
        self.store.delete(name).await.map_err(DeleteError)
    }
}

impl<S> ProvidersRegistry<S>
where
    S: ObjectStore + Clone,
{
    /// Find a provider and create a block stream client for the given kind and network.
    ///
    /// This method combines provider lookup and client creation in one operation.
    /// Providers are tried in random order to distribute load.
    pub async fn create_block_stream_client(
        &self,
        kind: impl Into<DatasetKindStr>,
        network: &str,
        meter: Option<&Meter>,
    ) -> Result<BlockStreamClient, CreateClientError> {
        let kind = kind.into();
        let config = self
            .find_provider(kind.clone(), network)
            .await
            .ok_or_else(|| CreateClientError::ProviderNotFound {
                kind: kind.to_string(),
                network: network.to_string(),
            })?;
        client::block_stream::create(config, meter).await
    }

    /// Find a provider by kind and network, applying environment variable substitution.
    ///
    /// Providers are tried in random order to distribute load.
    pub async fn find_provider(
        &self,
        kind: impl Into<DatasetKindStr>,
        network: &str,
    ) -> Option<ProviderConfig> {
        let kind = kind.into();

        let mut matching_providers = self
            .get_all()
            .await
            .values()
            .filter(|prov| prov.kind == kind && prov.network == network)
            .cloned()
            .collect::<Vec<_>>();

        if matching_providers.is_empty() {
            return None;
        }

        matching_providers.shuffle(&mut rand::rng());

        'try_find_provider: for mut provider in matching_providers {
            for (_key, value) in provider.rest.iter_mut() {
                if let Err(err) = env_subs::substitute_env_vars(value) {
                    tracing::warn!(
                        provider_name = %provider.name,
                        provider_kind = %kind,
                        provider_network = %network,
                        error = %err,
                        error_source = logging::error_source(&err),
                        "environment variable substitution failed for provider, trying next"
                    );
                    continue 'try_find_provider;
                }
            }

            tracing::debug!(
                provider_kind = %kind,
                provider_network = %network,
                "successfully selected provider with environment substitution"
            );

            return Some(provider);
        }

        None
    }

    /// Create an EVM RPC client for the given network.
    ///
    /// This method combines provider lookup, configuration parsing, and provider construction
    /// into a single operation. It automatically detects whether to use IPC or HTTP/HTTPS
    /// based on the URL scheme in the provider configuration.
    ///
    /// Returns `None` if no provider found for the network, or an error if a provider was
    /// found but configuration parsing or provider construction failed.
    pub async fn create_evm_rpc_client(
        &self,
        network: &str,
    ) -> Result<Option<EvmRpcProvider>, CreateEvmRpcClientError> {
        let Some(config) = self
            .find_provider(evm_rpc_datasets::EvmRpcDatasetKind, network)
            .await
        else {
            return Ok(None);
        };

        client::evm_rpc::create(config).await.map(Some)
    }
}

/// Error that can occur when registering a provider configuration.
#[derive(Debug, thiserror::Error)]
#[error("failed to store provider configuration")]
pub struct RegisterError(#[source] ConfigStoreError);

/// Error that can occur when deleting a provider configuration.
#[derive(Debug, thiserror::Error)]
#[error("Failed to delete provider configuration")]
pub struct DeleteError(#[source] pub ConfigDeleteError);

#[cfg(test)]
mod tests {
    mod cache;
    mod crud;
}
