use std::sync::Arc;

use amp_datasets_registry::DatasetsRegistry;
use amp_providers_registry::ProvidersRegistry;
use common::{
    catalog::dataset_access::{
        EthCallForDatasetError as DatasetAccessEthCallForDatasetError,
        GetDatasetError as DatasetAccessGetDatasetError,
        ResolveRevisionError as DatasetAccessResolveRevisionError,
    },
    evm::udfs::EthCall,
};
use datafusion::{
    common::HashMap,
    logical_expr::{ScalarUDF, async_udf::AsyncScalarUDF},
};
use datasets_common::{
    hash::Hash, hash_reference::HashReference, manifest::Manifest as CommonManifest,
    reference::Reference,
};
use datasets_derived::{DerivedDatasetKind, Manifest as DerivedManifest};
use datasets_raw::{
    client::{BlockStreamer, BlockStreamerExt as _},
    manifest::RawDatasetManifest,
};
use evm_rpc_datasets::{Dataset as EvmRpcDataset, Manifest as EvmRpcManifest};
use firehose_datasets::dataset::Manifest as FirehoseManifest;
use monitoring::telemetry::metrics::Meter;
use parking_lot::RwLock;
use solana_datasets::Manifest as SolanaManifest;
use tracing::instrument;

mod dataset_kind;
mod error;

pub use amp_datasets_registry::error::ResolveRevisionError;

pub use self::error::{
    EthCallForDatasetError, GetClientError, GetDatasetError, GetDerivedManifestError,
};
use crate::dataset_kind::{DatasetKind, UnsupportedKindError};

#[derive(Clone)]
pub struct DatasetStore {
    // Datasets registry for managing dataset revisions and tags.
    datasets_registry: DatasetsRegistry,
    // Provider registry for managing provider configurations and caching.
    providers_registry: ProvidersRegistry,
    // Cache maps HashReference to eth_call UDF.
    eth_call_cache: Arc<RwLock<HashMap<HashReference, ScalarUDF>>>,
    // This cache maps HashReference to the dataset definition.
    dataset_cache: Arc<RwLock<HashMap<HashReference, Arc<dyn datasets_common::dataset::Dataset>>>>,
}

impl DatasetStore {
    pub fn new(datasets_registry: DatasetsRegistry, providers_registry: ProvidersRegistry) -> Self {
        Self {
            datasets_registry,
            providers_registry,
            eth_call_cache: Default::default(),
            dataset_cache: Default::default(),
        }
    }
}

// Dataset versioning API
impl DatasetStore {
    /// Resolve a reference to a hash reference.
    ///
    /// See [`DatasetsRegistry::resolve_revision`] for full details.
    pub async fn resolve_revision(
        &self,
        reference: impl AsRef<Reference>,
    ) -> Result<Option<HashReference>, ResolveRevisionError> {
        self.datasets_registry.resolve_revision(reference).await
    }
}

// Dataset loading API
impl DatasetStore {
    /// Retrieves a dataset by hash reference, with in-memory caching.
    ///
    /// This method accepts a `HashReference` (a reference that has already been resolved
    /// to a concrete manifest hash) and loads the dataset:
    ///
    /// 1. Check the in-memory cache using the manifest hash as the key
    /// 2. On cache miss, retrieve the manifest path from metadata DB using the hash
    /// 3. Retrieve the manifest content from the manifest store using the path
    /// 4. Parse the common manifest structure to identify the dataset kind
    /// 5. Parse the kind-specific manifest and create the typed dataset instance
    /// 6. Cache the dataset and return
    ///
    /// To resolve a `Reference` (which may contain symbolic revisions like "latest" or "dev")
    /// into a `HashReference`, use `resolve_revision()` first.
    ///
    /// Returns `None` if the dataset cannot be found in the manifest store.
    #[instrument(skip(self), err)]
    pub async fn get_dataset(
        &self,
        reference: &HashReference,
    ) -> Result<Arc<dyn datasets_common::dataset::Dataset>, GetDatasetError> {
        let hash = reference.hash();

        // Check cache using HashReference as the key
        if let Some(dataset) = self.dataset_cache.read().get(reference).cloned() {
            tracing::trace!(dataset = %format!("{reference:#}"), "Cache hit, returning cached dataset");
            tracing::debug!(
                dataset = %format!("{reference:#}"),
                "Dataset loaded successfully"
            );
            return Ok(dataset);
        }

        tracing::debug!(dataset = %format!("{reference:#}"), "Cache miss, loading from store");

        // Load the manifest content using the path
        let Some(manifest_content) =
            self.datasets_registry
                .get_manifest(hash)
                .await
                .map_err(|source| GetDatasetError::LoadManifestContent {
                    reference: reference.clone(),
                    source,
                })?
        else {
            return Err(GetDatasetError::DatasetNotFound(reference.clone()));
        };

        let manifest = manifest_content
            .try_into_manifest::<CommonManifest>()
            .map_err(|source| GetDatasetError::ParseManifestForKind {
                reference: reference.clone(),
                source,
            })?;

        let kind = manifest
            .kind
            .as_str()
            .parse()
            .map_err(
                |err: UnsupportedKindError| GetDatasetError::UnsupportedKind {
                    reference: reference.clone(),
                    kind: err.kind,
                },
            )?;

        let dataset: Arc<dyn datasets_common::dataset::Dataset> = match kind {
            DatasetKind::EvmRpc => {
                let manifest = manifest_content
                    .try_into_manifest::<EvmRpcManifest>()
                    .map_err(|source| GetDatasetError::ParseManifest {
                        reference: reference.clone(),
                        kind: kind.into(),
                        source,
                    })?;
                Arc::new(evm_rpc_datasets::dataset(reference.clone(), manifest))
            }
            DatasetKind::Solana => {
                let manifest = manifest_content
                    .try_into_manifest::<SolanaManifest>()
                    .map_err(|source| GetDatasetError::ParseManifest {
                        reference: reference.clone(),
                        kind: kind.into(),
                        source,
                    })?;
                Arc::new(solana_datasets::dataset(reference.clone(), manifest))
            }
            DatasetKind::Firehose => {
                let manifest = manifest_content
                    .try_into_manifest::<FirehoseManifest>()
                    .map_err(|source| GetDatasetError::ParseManifest {
                        reference: reference.clone(),
                        kind: kind.into(),
                        source,
                    })?;
                Arc::new(firehose_datasets::evm::dataset(reference.clone(), manifest))
            }
            DatasetKind::Derived => {
                let manifest = manifest_content
                    .try_into_manifest::<DerivedManifest>()
                    .map_err(|source| GetDatasetError::ParseManifest {
                        reference: reference.clone(),
                        kind: kind.into(),
                        source,
                    })?;
                common::datasets_derived::dataset(reference.clone(), manifest)
                    .map(Arc::new)
                    .map_err(|source| GetDatasetError::CreateDerivedDataset {
                        reference: reference.clone(),
                        source,
                    })?
            }
        };

        // Cache the dataset
        self.dataset_cache
            .write()
            .insert(reference.clone(), dataset.clone());

        tracing::debug!(
            dataset = %format!("{reference:#}"),
            "Dataset loaded successfully"
        );

        Ok(dataset)
    }

    #[tracing::instrument(skip(self), err)]
    pub async fn get_derived_manifest(
        &self,
        hash: &Hash,
    ) -> Result<DerivedManifest, GetDerivedManifestError> {
        // Load the manifest content
        let Some(manifest_content) = self
            .datasets_registry
            .get_manifest(hash)
            .await
            .map_err(GetDerivedManifestError::LoadManifestContent)?
        else {
            return Err(GetDerivedManifestError::ManifestNotFound(hash.clone()));
        };

        let manifest = manifest_content
            .try_into_manifest::<DerivedManifest>()
            .map_err(GetDerivedManifestError::ManifestParseError)?;

        Ok(manifest)
    }

    #[tracing::instrument(skip(self), err)]
    pub async fn get_client(
        &self,
        hash: &Hash,
        meter: Option<&Meter>,
    ) -> Result<impl BlockStreamer, GetClientError> {
        // Load the manifest content using the path
        let Some(manifest_content) = self
            .datasets_registry
            .get_manifest(hash)
            .await
            .map_err(GetClientError::LoadManifestContent)?
        else {
            return Err(GetClientError::ManifestNotFound(hash.clone()));
        };

        let manifest = manifest_content
            .try_into_manifest::<RawDatasetManifest>()
            .map_err(GetClientError::RawManifestParseError)?;

        // Check it's a raw dataset kind (not derived)
        if manifest.kind == DerivedDatasetKind {
            return Err(GetClientError::UnsupportedKind {
                kind: manifest.kind.to_string(),
            });
        }

        let kind = manifest.kind;
        let network = manifest.network;

        let client = self
            .providers_registry
            .create_block_stream_client(kind, &network, meter)
            .await
            .map_err(GetClientError::ClientCreation)?;

        Ok(client.with_retry())
    }

    /// Returns cached eth_call scalar UDF, otherwise loads the UDF and caches it.
    ///
    /// The function will be named `<sql_table_ref_schema>.eth_call`.
    async fn eth_call_for_dataset(
        &self,
        sql_table_ref_schema: &str,
        dataset: &dyn datasets_common::dataset::Dataset,
    ) -> Result<Option<ScalarUDF>, EthCallForDatasetError> {
        if !dataset.is::<EvmRpcDataset>() {
            return Ok(None);
        }

        // Check if we already have the provider cached.
        if let Some(udf) = self.eth_call_cache.read().get(dataset.reference()) {
            return Ok(Some(udf.clone()));
        }

        // Load the provider from the dataset definition.
        let network = dataset
            .tables()
            .first()
            .expect("EVM RPC Datasets MUST have tables")
            .network();

        let provider = match self.providers_registry.create_evm_rpc_client(network).await {
            Ok(Some(provider)) => provider,
            Ok(None) => {
                tracing::warn!(
                    provider_kind = %DatasetKind::EvmRpc,
                    provider_network = %network,
                    "no providers available for the requested kind-network configuration"
                );
                return Err(EthCallForDatasetError::ProviderNotFound {
                    dataset_kind: DatasetKind::EvmRpc.into(),
                    network: network.clone(),
                });
            }
            Err(err) => {
                return Err(EthCallForDatasetError::ProviderCreation(err));
            }
        };

        let udf = AsyncScalarUDF::new(Arc::new(EthCall::new(sql_table_ref_schema, provider)))
            .into_scalar_udf();

        // Cache the EthCall UDF
        self.eth_call_cache
            .write()
            .insert(dataset.reference().clone(), udf.clone());

        Ok(Some(udf))
    }
}

// Implement DatasetAccess trait for DatasetStore
impl common::catalog::dataset_access::DatasetAccess for DatasetStore {
    async fn resolve_revision(
        &self,
        reference: impl AsRef<Reference> + Send,
    ) -> Result<Option<HashReference>, DatasetAccessResolveRevisionError> {
        let reference_value = reference.as_ref();
        self.resolve_revision(reference_value)
            .await
            .map_err(Into::into)
    }

    async fn get_dataset(
        &self,
        reference: &HashReference,
    ) -> Result<Arc<dyn datasets_common::dataset::Dataset>, DatasetAccessGetDatasetError> {
        self.get_dataset(reference).await.map_err(Into::into)
    }

    async fn eth_call_for_dataset(
        &self,
        sql_table_ref_schema: &str,
        dataset: &dyn datasets_common::dataset::Dataset,
    ) -> Result<Option<ScalarUDF>, DatasetAccessEthCallForDatasetError> {
        self.eth_call_for_dataset(sql_table_ref_schema, dataset)
            .await
            .map_err(Into::into)
    }
}
