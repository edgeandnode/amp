//! EthCall UDF cache for EVM RPC datasets.
//!
//! This module provides the `EthCallUdfsCache` struct which manages creation and caching
//! of `eth_call` scalar UDFs for EVM RPC datasets through the providers registry.

use std::sync::Arc;

use amp_providers_registry::ProvidersRegistry;
use datafusion::{
    common::HashMap,
    logical_expr::{ScalarUDF, async_udf::AsyncScalarUDF},
};
use datasets_common::{
    dataset::Dataset as _, dataset_kind_str::DatasetKindStr, hash_reference::HashReference,
    network_id::NetworkId,
};
use datasets_raw::dataset::Dataset as RawDataset;
use evm_rpc_datasets::EvmRpcDatasetKind;
use parking_lot::RwLock;

use super::udf::EthCall;

/// Manages creation and caching of `eth_call` scalar UDFs for EVM RPC datasets.
///
/// Orchestrates UDF creation through the providers registry with in-memory caching.
#[derive(Clone)]
pub struct EthCallUdfsCache {
    registry: ProvidersRegistry,
    cache: Arc<RwLock<HashMap<HashReference, ScalarUDF>>>,
}

impl std::fmt::Debug for EthCallUdfsCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EthCallUdfsCache").finish_non_exhaustive()
    }
}

impl EthCallUdfsCache {
    /// Creates a new EthCall UDFs cache.
    pub fn new(registry: ProvidersRegistry) -> Self {
        Self {
            registry,
            cache: Default::default(),
        }
    }

    /// Returns a reference to the underlying providers registry.
    pub fn providers_registry(&self) -> &ProvidersRegistry {
        &self.registry
    }

    /// Returns cached eth_call scalar UDF, otherwise loads the UDF and caches it.
    ///
    /// The function will be named `<sql_schema_name>.eth_call`.
    ///
    /// # Panics
    ///
    /// Panics if an EVM RPC dataset has no tables. This is a structural invariant
    /// guaranteed by the dataset construction process.
    pub async fn eth_call_for_dataset(
        &self,
        sql_schema_name: &str,
        dataset: &dyn datasets_common::dataset::Dataset,
    ) -> Result<Option<ScalarUDF>, EthCallForDatasetError> {
        let Some(raw) = dataset.downcast_ref::<RawDataset>() else {
            return Ok(None);
        };

        if raw.kind() != EvmRpcDatasetKind {
            return Ok(None);
        }

        // Check if we already have the provider cached.
        if let Some(udf) = self.cache.read().get(dataset.reference()) {
            return Ok(Some(udf.clone()));
        }

        // Load the provider from the dataset definition.
        let network = raw.network();

        let provider = match self.registry.create_evm_rpc_client(network).await {
            Ok(Some(provider)) => provider,
            Ok(None) => {
                tracing::warn!(
                    provider_kind = %EvmRpcDatasetKind,
                    provider_network = %network,
                    "no provider found for requested kind-network configuration"
                );
                return Err(EthCallForDatasetError::ProviderNotFound {
                    dataset_kind: EvmRpcDatasetKind.into(),
                    network: network.clone(),
                });
            }
            Err(err) => {
                return Err(EthCallForDatasetError::ProviderCreation(err));
            }
        };

        let udf = AsyncScalarUDF::new(Arc::new(EthCall::new(sql_schema_name, provider)))
            .into_scalar_udf();

        // Cache the EthCall UDF
        self.cache
            .write()
            .insert(dataset.reference().clone(), udf.clone());

        Ok(Some(udf))
    }
}

/// Errors that occur when creating eth_call user-defined functions for EVM RPC datasets.
#[derive(Debug, thiserror::Error)]
pub enum EthCallForDatasetError {
    /// No provider configuration found for the dataset kind and network combination.
    #[error("No provider found for dataset kind '{dataset_kind}' and network '{network}'")]
    ProviderNotFound {
        dataset_kind: DatasetKindStr,
        network: NetworkId,
    },

    /// Failed to create the EVM RPC provider.
    #[error("Failed to create EVM RPC provider")]
    ProviderCreation(#[source] amp_providers_registry::CreateEvmRpcClientError),
}
