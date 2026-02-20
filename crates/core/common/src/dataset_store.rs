//! Dataset store for managing dataset revisions, manifests, and providers.
//!
//! This module provides the `DatasetStore` struct which orchestrates dataset loading
//! and provider management through the datasets and providers registries.

use std::sync::Arc;

pub use amp_datasets_registry::error::ResolveRevisionError;
use amp_datasets_registry::{
    DatasetsRegistry,
    manifests::{ManifestContent, ManifestParseError},
};
use amp_providers_registry::ProvidersRegistry;
use datafusion::{
    common::HashMap,
    logical_expr::{ScalarUDF, async_udf::AsyncScalarUDF},
};
use datasets_common::{
    dataset_kind_str::DatasetKindStr, hash::Hash, hash_reference::HashReference,
    manifest::Manifest as CommonManifest, network_id::NetworkId, reference::Reference,
};
use datasets_derived::{DerivedDatasetKind, Manifest as DerivedManifest};
use datasets_raw::{
    client::{BlockStreamer, BlockStreamerExt as _},
    manifest::RawDatasetManifest,
};
use evm_rpc_datasets::{Dataset as EvmRpcDataset, EvmRpcDatasetKind, Manifest as EvmRpcManifest};
use firehose_datasets::{FirehoseDatasetKind, Manifest as FirehoseManifest};
use monitoring::telemetry::metrics::Meter;
use parking_lot::RwLock;
use solana_datasets::{Manifest as SolanaManifest, SolanaDatasetKind};

use crate::evm::udfs::EthCall;

/// Manages dataset loading, caching, and provider access.
///
/// Orchestrates dataset retrieval through the datasets and providers registries,
/// with in-memory caching for datasets and eth_call UDFs.
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
    /// Creates a new dataset store with in-memory caching for datasets and eth_call UDFs.
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
    /// Retrieves a dataset by hash reference with in-memory caching. Use `resolve_revision()` to convert symbolic references to hash references first.
    #[tracing::instrument(skip(self), err)]
    pub async fn get_dataset(
        &self,
        reference: &HashReference,
    ) -> Result<Arc<dyn datasets_common::dataset::Dataset>, GetDatasetError> {
        let hash = reference.hash();

        // Check cache using HashReference as the key
        if let Some(dataset) = self.dataset_cache.read().get(reference).cloned() {
            tracing::trace!(dataset = %format!("{reference:#}"), "cache hit, returned cached dataset");
            tracing::debug!(
                dataset = %format!("{reference:#}"),
                "Dataset loaded successfully"
            );
            return Ok(dataset);
        }

        tracing::debug!(dataset = %format!("{reference:#}"), "cache miss, loading dataset from store");

        // Load the manifest content using the path
        let Some(manifest_content) =
            self.datasets_registry
                .get_manifest(hash)
                .await
                .map_err(|source| GetDatasetError::LoadManifestContent {
                    reference: reference.clone(),
                    source: Box::new(source),
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

        let dataset = create_dataset_from_manifest(&manifest.kind, reference, manifest_content)?;

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

    /// Retrieves a derived dataset manifest by hash without creating a dataset instance or caching.
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

    /// Creates a block streaming client for a raw dataset with automatic retry on failures.
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
    ///
    /// # Panics
    ///
    /// Panics if an EVM RPC dataset has no tables. This is a structural invariant
    /// guaranteed by the dataset construction process.
    pub async fn eth_call_for_dataset(
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
        // SAFETY: EVM RPC datasets always have at least one table, guaranteed by dataset construction.
        let network = dataset
            .tables()
            .first()
            .expect("EVM RPC Datasets MUST have tables")
            .network();

        let provider = match self.providers_registry.create_evm_rpc_client(network).await {
            Ok(Some(provider)) => provider,
            Ok(None) => {
                tracing::warn!(
                    provider_kind = %evm_rpc_datasets::EvmRpcDatasetKind,
                    provider_network = %network,
                    "no provider found for requested kind-network configuration"
                );
                return Err(EthCallForDatasetError::ProviderNotFound {
                    dataset_kind: evm_rpc_datasets::EvmRpcDatasetKind.into(),
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

/// Errors that occur when retrieving and loading datasets by hash reference.
#[derive(Debug, thiserror::Error)]
pub enum GetDatasetError {
    /// Dataset not found.
    ///
    /// This occurs when the manifest hash in the hash reference does not exist in the
    /// manifest store, or when the manifest file cannot be retrieved.
    #[error("Dataset '{0}' not found")]
    DatasetNotFound(HashReference),

    /// Failed to load manifest content from object store
    ///
    /// This occurs when the object store operation to fetch the manifest file fails,
    /// which could be due to network issues, permissions, or storage backend problems.
    #[error("Failed to load manifest content from object store for dataset '{reference}'")]
    LoadManifestContent {
        reference: HashReference,
        #[source]
        source: Box<amp_datasets_registry::error::GetManifestError>,
    },

    /// Failed to parse manifest to extract kind field
    ///
    /// This occurs when parsing the manifest file to extract the `kind` field.
    /// The manifest may contain invalid JSON/TOML syntax or be missing the kind field.
    #[error("Failed to parse manifest to extract kind field for dataset '{reference}'")]
    ParseManifestForKind {
        reference: HashReference,
        #[source]
        source: ManifestParseError,
    },

    /// Dataset kind is not supported
    ///
    /// This occurs when the `kind` field in the manifest contains a value that
    /// doesn't match any supported dataset type (evm-rpc, firehose, derived).
    #[error("Unsupported dataset kind '{kind}' for dataset '{reference}'")]
    UnsupportedKind {
        reference: HashReference,
        kind: String,
    },

    /// Failed to parse kind-specific manifest
    ///
    /// This occurs when parsing the kind-specific manifest (EvmRpc, EthBeacon, Firehose, or Derived).
    /// The manifest structure may not match the expected schema for the dataset kind.
    #[error("Failed to parse {kind} manifest for dataset '{reference}'")]
    ParseManifest {
        reference: HashReference,
        kind: DatasetKindStr,
        #[source]
        source: ManifestParseError,
    },

    /// Failed to create derived dataset instance
    ///
    /// This occurs when creating a derived dataset from its manifest, which may fail due to:
    /// - Invalid SQL queries in the dataset definition
    /// - Dependency resolution issues (referenced datasets not found)
    /// - Logical errors in the dataset definition
    #[error("Failed to create derived dataset for '{reference}'")]
    CreateDerivedDataset {
        reference: HashReference,
        #[source]
        source: Box<crate::datasets_derived::DatasetError>,
    },
}

/// Errors that occur when retrieving derived dataset manifests by hash.
#[derive(Debug, thiserror::Error)]
pub enum GetDerivedManifestError {
    /// Failed to load manifest content from object store
    ///
    /// This occurs when the object store operation to fetch the manifest file fails,
    /// which could be due to network issues, permissions, or storage backend problems.
    #[error("Failed to load manifest content from object store")]
    LoadManifestContent(#[source] amp_datasets_registry::error::GetManifestError),

    /// Failed to parse the manifest file content.
    ///
    /// This occurs when:
    /// - The manifest file contains invalid JSON or TOML syntax
    /// - The manifest structure doesn't match the expected derived dataset schema
    /// - The dataset kind is not 'manifest' (for derived datasets)
    /// - Required fields are missing or have incorrect types
    #[error("Failed to parse manifest")]
    ManifestParseError(#[source] ManifestParseError),

    /// Manifest not found in the manifest store.
    ///
    /// This occurs when the manifest hash does not correspond to any manifest
    /// in the manifest store.
    #[error("Manifest {0} not found in the manifest store")]
    ManifestNotFound(Hash),
}

/// Errors that occur when creating block streaming clients for raw datasets.
#[derive(Debug, thiserror::Error)]
pub enum GetClientError {
    /// Failed to load manifest content from object store
    ///
    /// This occurs when the object store operation to fetch the manifest file fails,
    /// which could be due to network issues, permissions, or storage backend problems.
    #[error("Failed to load manifest content from object store")]
    LoadManifestContent(#[source] amp_datasets_registry::error::GetManifestError),

    /// Failed to parse the raw manifest file content.
    ///
    /// This occurs when:
    /// - The manifest file contains invalid JSON or TOML syntax
    /// - The manifest is missing the required 'network' field
    /// - The manifest structure doesn't match the expected raw dataset schema
    #[error("Failed to parse manifest for raw dataset")]
    RawManifestParseError(#[source] ManifestParseError),

    /// The dataset kind is not a raw dataset type.
    ///
    /// This occurs when trying to get a client for a dataset that is not a raw data source.
    /// Only raw dataset kinds (evm-rpc, firehose) can have clients retrieved.
    /// SQL and Derived datasets cannot have clients as they are views over other datasets.
    #[error(
        "Dataset has unsupported kind '{kind}' for client retrieval (expected raw dataset: evm-rpc or firehose)"
    )]
    UnsupportedKind { kind: String },

    /// Failed to create client for the dataset.
    ///
    /// This occurs during client creation for the provider, which may fail due to:
    /// - No provider found for the kind-network combination
    /// - Invalid provider configuration
    /// - Client initialization failures (connection issues, invalid URLs, etc.)
    #[error("Failed to create client")]
    ClientCreation(#[source] amp_providers_registry::CreateClientError),

    /// Manifest not found in the manifest store.
    ///
    /// This occurs when the manifest hash does not correspond to any manifest
    /// in the manifest store.
    #[error("Manifest {0} not found in the manifest store")]
    ManifestNotFound(Hash),
}

/// Errors that occur when creating eth_call user-defined functions for EVM RPC datasets.
#[derive(Debug, thiserror::Error)]
pub enum EthCallForDatasetError {
    /// No provider configuration found for the dataset kind and network combination.
    ///
    /// This occurs when:
    /// - No provider is configured for the specific kind-network pair
    /// - All providers for this kind-network are disabled or failed environment variable substitution
    /// - Provider configuration files are missing or invalid
    #[error("No provider found for dataset kind '{dataset_kind}' and network '{network}'")]
    ProviderNotFound {
        dataset_kind: DatasetKindStr,
        network: NetworkId,
    },

    /// Failed to create the EVM RPC provider.
    ///
    /// This occurs when provider configuration parsing fails or when
    /// establishing an IPC connection fails.
    #[error("Failed to create EVM RPC provider")]
    ProviderCreation(#[source] amp_providers_registry::CreateEvmRpcClientError),
}

/// Parses manifest content according to the dataset kind and creates the appropriate dataset implementation.
fn create_dataset_from_manifest(
    kind: &DatasetKindStr,
    reference: &HashReference,
    manifest_content: ManifestContent,
) -> Result<Arc<dyn datasets_common::dataset::Dataset>, GetDatasetError> {
    let dataset: Arc<dyn datasets_common::dataset::Dataset> = match kind.as_str() {
        s if s == EvmRpcDatasetKind => {
            let manifest = manifest_content
                .try_into_manifest::<EvmRpcManifest>()
                .map_err(|source| GetDatasetError::ParseManifest {
                    reference: reference.clone(),
                    kind: evm_rpc_datasets::EvmRpcDatasetKind.into(),
                    source,
                })?;
            Arc::new(evm_rpc_datasets::dataset(reference.clone(), manifest))
        }
        s if s == SolanaDatasetKind => {
            let manifest = manifest_content
                .try_into_manifest::<SolanaManifest>()
                .map_err(|source| GetDatasetError::ParseManifest {
                    reference: reference.clone(),
                    kind: solana_datasets::SolanaDatasetKind.into(),
                    source,
                })?;
            Arc::new(solana_datasets::dataset(reference.clone(), manifest))
        }
        s if s == FirehoseDatasetKind => {
            let manifest = manifest_content
                .try_into_manifest::<FirehoseManifest>()
                .map_err(|source| GetDatasetError::ParseManifest {
                    reference: reference.clone(),
                    kind: firehose_datasets::FirehoseDatasetKind.into(),
                    source,
                })?;
            Arc::new(firehose_datasets::dataset(reference.clone(), manifest))
        }
        s if s == DerivedDatasetKind => {
            let manifest = manifest_content
                .try_into_manifest::<DerivedManifest>()
                .map_err(|source| GetDatasetError::ParseManifest {
                    reference: reference.clone(),
                    kind: DerivedDatasetKind.into(),
                    source,
                })?;
            crate::datasets_derived::dataset(reference.clone(), manifest)
                .map(Arc::new)
                .map_err(|source| GetDatasetError::CreateDerivedDataset {
                    reference: reference.clone(),
                    source: Box::new(source),
                })?
        }
        _ => {
            return Err(GetDatasetError::UnsupportedKind {
                reference: reference.clone(),
                kind: kind.to_string(),
            });
        }
    };
    Ok(dataset)
}
