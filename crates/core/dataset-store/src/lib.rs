use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
    sync::Arc,
};

use amp_datasets_registry::{DatasetsRegistry, error::ResolveRevisionError};
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
use datasets_raw::client::{BlockStreamer, BlockStreamerExt};
use evm_rpc_datasets::{EvmRpcDatasetKind, Manifest as EvmRpcManifest};
use firehose_datasets::dataset::Manifest as FirehoseManifest;
use monitoring::telemetry::metrics::Meter;
use parking_lot::RwLock;
use solana_datasets::Manifest as SolanaManifest;
use tracing::instrument;

mod dataset_kind;
mod error;

pub use self::error::{
    EthCallForDatasetError, GetAllDatasetsError, GetClientError, GetDatasetError,
    GetDerivedManifestError,
};
use crate::{
    dataset_kind::{DatasetKind, UnsupportedKindError},
    error::DatasetDependencyError,
};

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
            tracing::trace!(dataset = %reference.short_display(), "Cache hit, returning cached dataset");
            tracing::debug!(
                dataset = %reference.short_display(),
                "Dataset loaded successfully"
            );
            return Ok(dataset);
        }

        tracing::debug!(dataset = %reference.short_display(), "Cache miss, loading from store");

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

        let kind = manifest.kind.parse().map_err(|err: UnsupportedKindError| {
            GetDatasetError::UnsupportedKind {
                reference: reference.clone(),
                kind: err.kind,
            }
        })?;

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
                datasets_derived::dataset(reference.clone(), manifest)
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
            dataset = %reference.short_display(),
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
            .try_into_manifest::<CommonManifest>()
            .map_err(GetDerivedManifestError::ManifestParseError)?;

        let kind = DatasetKind::from_str(&manifest.kind).map_err(|err: UnsupportedKindError| {
            GetDerivedManifestError::UnsupportedKind { kind: err.kind }
        })?;

        if kind != DatasetKind::Derived {
            return Err(GetDerivedManifestError::UnsupportedKind {
                kind: kind.to_string(),
            });
        }

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
            .try_into_manifest::<CommonManifest>()
            .map_err(GetClientError::CommonManifestParseError)?;

        let kind: DatasetKind = manifest.kind.parse().map_err(|err: UnsupportedKindError| {
            GetClientError::UnsupportedKind { kind: err.kind }
        })?;
        if !kind.is_raw() {
            return Err(GetClientError::UnsupportedKind {
                kind: manifest.kind,
            });
        }

        let Some(network) = manifest.network else {
            tracing::warn!(
                dataset_kind = %kind,
                "dataset is missing required 'network' field for raw dataset kind"
            );
            return Err(GetClientError::MissingNetwork);
        };

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
        if dataset.kind() != EvmRpcDatasetKind {
            return Ok(None);
        }

        // Check if we already have the provider cached.
        if let Some(udf) = self.eth_call_cache.read().get(dataset.reference()) {
            return Ok(Some(udf.clone()));
        }

        // Load the provider from the dataset definition.
        let Some(network) = dataset.network() else {
            tracing::warn!(
                dataset = %dataset.reference().short_display(),
                "dataset is missing required 'network' field for evm-rpc kind"
            );
            return Err(EthCallForDatasetError::MissingNetwork {
                reference: dataset.reference().clone(),
            });
        };

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

/// Return the input datasets and their dataset dependencies. The output set is ordered such that
/// each dataset comes after all datasets it depends on.
pub async fn dataset_and_dependencies(
    store: &DatasetStore,
    dataset: Reference,
) -> Result<Vec<Reference>, DatasetDependencyError> {
    let mut datasets = vec![dataset];
    let mut deps: BTreeMap<Reference, Vec<Reference>> = Default::default();
    while let Some(dataset_ref) = datasets.pop() {
        // Resolve the reference to a hash reference first
        let hash_ref = store
            .resolve_revision(&dataset_ref)
            .await
            .map_err(DatasetDependencyError::ResolveRevision)?
            .ok_or_else(|| DatasetDependencyError::DatasetNotFound(dataset_ref.clone()))?;
        let dataset = store
            .get_dataset(&hash_ref)
            .await
            .map_err(DatasetDependencyError::GetDataset)?;

        if dataset.kind() != DerivedDatasetKind {
            deps.insert(dataset_ref, vec![]);
            continue;
        }

        let refs: Vec<Reference> = dataset
            .dependencies()
            .values()
            .map(|dep| dep.to_reference())
            .collect();
        let mut untracked_refs = refs
            .iter()
            .filter(|r| deps.keys().all(|d| d != *r))
            .cloned()
            .collect();
        datasets.append(&mut untracked_refs);
        deps.insert(dataset_ref, refs);
    }

    dependency_sort(deps).map_err(DatasetDependencyError::CycleDetected)
}

/// Given a map of values to their dependencies, return a set where each value is ordered after
/// all of its dependencies. An error is returned if a cycle is detected.
fn dependency_sort(
    deps: BTreeMap<Reference, Vec<Reference>>,
) -> Result<Vec<Reference>, datasets_derived::deps::DfsError<Reference>> {
    let nodes: BTreeSet<&Reference> = deps
        .iter()
        .flat_map(|(ds, deps)| std::iter::once(ds).chain(deps))
        .collect();
    let mut ordered: Vec<Reference> = Default::default();
    let mut visited: BTreeSet<&Reference> = Default::default();
    let mut visited_cycle: BTreeSet<&Reference> = Default::default();
    for node in nodes {
        if !visited.contains(node) {
            datasets_derived::deps::dfs(
                node,
                &deps,
                &mut ordered,
                &mut visited,
                &mut visited_cycle,
            )?;
        }
    }
    Ok(ordered)
}

#[cfg(test)]
mod tests {
    use datasets_common::revision::Revision;

    #[test]
    fn dependency_sort_order() {
        #[expect(clippy::type_complexity)]
        let cases: &[(&[(&str, &[&str])], Option<&[&str]>)] = &[
            (&[("a", &["b"]), ("b", &["a"])], None),
            (&[("a", &["b"])], Some(&["b", "a"])),
            (&[("a", &["b", "c"])], Some(&["b", "c", "a"])),
            (&[("a", &["b"]), ("c", &[])], Some(&["b", "a", "c"])),
            (&[("a", &["b"]), ("c", &["b"])], Some(&["b", "a", "c"])),
            (
                &[("a", &["b", "c"]), ("b", &["d"]), ("c", &["d"])],
                Some(&["d", "b", "c", "a"]),
            ),
            (
                &[("a", &["b", "c"]), ("b", &["c", "d"])],
                Some(&["c", "d", "b", "a"]),
            ),
        ];
        let name_to_ref = |name: &str| {
            datasets_common::reference::Reference::new(
                "_".parse().unwrap(),
                name.parse().unwrap(),
                Revision::Dev,
            )
        };
        for (input, expected) in cases {
            let deps = input
                .iter()
                .map(|(k, v)| (name_to_ref(k), v.iter().map(|n| name_to_ref(n)).collect()))
                .collect();
            let expected: Option<Vec<datasets_common::reference::Reference>> =
                expected.map(|n| n.iter().map(|n| name_to_ref(n)).collect());
            let result = super::dependency_sort(deps);
            match expected {
                Some(expected) => assert_eq!(*expected, result.unwrap()),
                None => assert!(result.is_err()),
            }
        }
    }
}
