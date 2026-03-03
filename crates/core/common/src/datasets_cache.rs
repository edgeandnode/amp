//! Dataset cache for managing dataset revisions, manifests, and providers.
//!
//! This module provides the `DatasetsCache` struct which orchestrates dataset loading
//! through the datasets registry with in-memory caching.
// TODO: Move to providers-registry once the derived dataset constructor is decoupled from common

use std::sync::Arc;

pub use amp_datasets_registry::error::ResolveRevisionError;
use amp_datasets_registry::{
    DatasetsRegistry,
    manifests::{ManifestContent, ManifestParseError},
};
use datafusion::common::HashMap;
use datasets_common::{
    dataset_kind_str::DatasetKindStr, hash::Hash, hash_reference::HashReference,
    manifest::Manifest as CommonManifest, reference::Reference,
};
use datasets_derived::{DerivedDatasetKind, Manifest as DerivedManifest};
use evm_rpc_datasets::{EvmRpcDatasetKind, Manifest as EvmRpcManifest};
use firehose_datasets::{FirehoseDatasetKind, Manifest as FirehoseManifest};
use parking_lot::RwLock;
use solana_datasets::{Manifest as SolanaManifest, SolanaDatasetKind};

/// Manages dataset loading and caching.
///
/// Orchestrates dataset retrieval through the datasets registry
/// with in-memory caching for loaded datasets.
#[derive(Clone)]
pub struct DatasetsCache {
    registry: DatasetsRegistry,
    cache: Arc<RwLock<HashMap<HashReference, Arc<dyn datasets_common::dataset::Dataset>>>>,
}

impl std::fmt::Debug for DatasetsCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatasetsCache").finish_non_exhaustive()
    }
}

impl DatasetsCache {
    /// Creates a new datasets cache with in-memory caching.
    pub fn new(registry: DatasetsRegistry) -> Self {
        Self {
            registry,
            cache: Default::default(),
        }
    }
}

// Dataset versioning API
impl DatasetsCache {
    /// Resolve a reference to a hash reference.
    ///
    /// See [`DatasetsRegistry::resolve_revision`] for full details.
    pub async fn resolve_revision(
        &self,
        reference: impl AsRef<Reference>,
    ) -> Result<Option<HashReference>, ResolveRevisionError> {
        self.registry.resolve_revision(reference).await
    }
}

// Dataset loading API
impl DatasetsCache {
    /// Retrieves a dataset by hash reference with in-memory caching. Use `resolve_revision()` to convert symbolic references to hash references first.
    #[tracing::instrument(skip(self), err)]
    pub async fn get_dataset(
        &self,
        reference: &HashReference,
    ) -> Result<Arc<dyn datasets_common::dataset::Dataset>, GetDatasetError> {
        let hash = reference.hash();

        // Check cache using HashReference as the key
        if let Some(dataset) = self.cache.read().get(reference).cloned() {
            tracing::trace!(dataset = %format!("{reference:#}"), "cache hit, returned cached dataset");
            tracing::debug!(
                dataset = %format!("{reference:#}"),
                "Dataset loaded successfully"
            );
            return Ok(dataset);
        }

        tracing::debug!(dataset = %format!("{reference:#}"), "cache miss, loading dataset from store");

        // Load the manifest content using the path
        let Some(manifest_content) = self.registry.get_manifest(hash).await.map_err(|source| {
            GetDatasetError::LoadManifestContent {
                reference: reference.clone(),
                source: Box::new(source),
            }
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
        self.cache
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
            .registry
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
}

/// Errors that occur when retrieving and loading datasets by hash reference.
#[derive(Debug, thiserror::Error)]
pub enum GetDatasetError {
    /// Dataset not found.
    #[error("Dataset '{0}' not found")]
    DatasetNotFound(HashReference),

    /// Failed to load manifest content from object store
    #[error("Failed to load manifest content from object store for dataset '{reference}'")]
    LoadManifestContent {
        reference: HashReference,
        #[source]
        source: Box<amp_datasets_registry::error::GetManifestError>,
    },

    /// Failed to parse manifest to extract kind field
    #[error("Failed to parse manifest to extract kind field for dataset '{reference}'")]
    ParseManifestForKind {
        reference: HashReference,
        #[source]
        source: ManifestParseError,
    },

    /// Dataset kind is not supported
    #[error("Unsupported dataset kind '{kind}' for dataset '{reference}'")]
    UnsupportedKind {
        reference: HashReference,
        kind: String,
    },

    /// Failed to parse kind-specific manifest
    #[error("Failed to parse {kind} manifest for dataset '{reference}'")]
    ParseManifest {
        reference: HashReference,
        kind: DatasetKindStr,
        #[source]
        source: ManifestParseError,
    },

    /// Failed to create derived dataset instance
    #[error("Failed to create derived dataset for '{reference}'")]
    CreateDerivedDataset {
        reference: HashReference,
        #[source]
        source: Box<datasets_derived::dataset::DatasetError>,
    },
}

impl crate::retryable::RetryableErrorExt for GetDatasetError {
    fn is_retryable(&self) -> bool {
        use amp_datasets_registry::retryable::RetryableErrorExt as _;
        match self {
            Self::DatasetNotFound(_) => false,
            Self::LoadManifestContent { source, .. } => source.is_retryable(),
            Self::ParseManifestForKind { .. } => false,
            Self::UnsupportedKind { .. } => false,
            Self::ParseManifest { .. } => false,
            Self::CreateDerivedDataset { .. } => false,
        }
    }
}

/// Errors that occur when retrieving derived dataset manifests by hash.
#[derive(Debug, thiserror::Error)]
pub enum GetDerivedManifestError {
    /// Failed to load manifest content from object store
    #[error("Failed to load manifest content from object store")]
    LoadManifestContent(#[source] amp_datasets_registry::error::GetManifestError),

    /// Failed to parse the manifest file content.
    #[error("Failed to parse manifest")]
    ManifestParseError(#[source] ManifestParseError),

    /// Manifest not found in the manifest store.
    #[error("Manifest {0} not found in the manifest store")]
    ManifestNotFound(Hash),
}

impl crate::retryable::RetryableErrorExt for GetDerivedManifestError {
    fn is_retryable(&self) -> bool {
        use amp_datasets_registry::retryable::RetryableErrorExt;
        match self {
            Self::LoadManifestContent(err) => err.is_retryable(),
            Self::ManifestParseError(_) => false,
            Self::ManifestNotFound(_) => false,
        }
    }
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
                    kind: EvmRpcDatasetKind.into(),
                    source,
                })?;
            Arc::new(evm_rpc_datasets::dataset(reference.clone(), manifest))
        }
        s if s == SolanaDatasetKind => {
            let manifest = manifest_content
                .try_into_manifest::<SolanaManifest>()
                .map_err(|source| GetDatasetError::ParseManifest {
                    reference: reference.clone(),
                    kind: SolanaDatasetKind.into(),
                    source,
                })?;
            Arc::new(solana_datasets::dataset(reference.clone(), manifest))
        }
        s if s == FirehoseDatasetKind => {
            let manifest = manifest_content
                .try_into_manifest::<FirehoseManifest>()
                .map_err(|source| GetDatasetError::ParseManifest {
                    reference: reference.clone(),
                    kind: FirehoseDatasetKind.into(),
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
            datasets_derived::dataset::dataset(reference.clone(), manifest)
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
