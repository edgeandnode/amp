use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    num::NonZeroU32,
    str::FromStr,
    sync::Arc,
};

use common::{
    BlockStreamer, BlockStreamerExt, BoxError, Dataset, LogicalCatalog, PlanningContext,
    catalog::physical::{Catalog, PhysicalTable},
    evm::{self, udfs::EthCall},
    manifest::derived,
    query_context::QueryEnv,
};
use datafusion::{
    common::HashMap,
    logical_expr::{ScalarUDF, async_udf::AsyncScalarUDF},
    sql::{TableReference, parser, resolve::resolve_table_references},
};
use datasets_common::{
    hash::Hash, manifest::Manifest as CommonManifest, name::Name, namespace::Namespace,
    partial_reference::PartialReference, reference::Reference, revision::Revision,
    version::Version,
};
use datasets_derived::{DerivedDatasetKind, Manifest as DerivedManifest};
use eth_beacon_datasets::{
    Manifest as EthBeaconManifest, ProviderConfig as EthBeaconProviderConfig,
};
use evm_rpc_datasets::{
    EvmRpcDatasetKind, Manifest as EvmRpcManifest, ProviderConfig as EvmRpcProviderConfig,
};
use firehose_datasets::dataset::{
    Manifest as FirehoseManifest, ProviderConfig as FirehoseProviderConfig,
};
use js_runtime::isolate_pool::IsolatePool;
use metadata_db::{DatasetVersion, MetadataDb};
use parking_lot::RwLock;
use rand::seq::SliceRandom as _;
use tracing::instrument;
use url::Url;

mod block_stream_client;
mod dataset_kind;
mod env_substitute;
mod error;
pub mod manifests;
pub mod providers;
mod sql_visitors;

use self::{
    block_stream_client::BlockStreamClient,
    manifests::{DatasetManifestsStore, ManifestContent, ManifestPath},
    providers::{ProviderConfig, ProviderConfigsStore},
};
pub use self::{
    dataset_kind::{DatasetKind, UnsupportedKindError},
    error::{
        CatalogForSqlError, DeleteManifestError, DeleteVersionTagError, EthCallForDatasetError,
        ExtractDatasetFromFunctionNamesError, ExtractDatasetFromTableRefsError,
        GetAllDatasetsError, GetClientError, GetDatasetError, GetDerivedManifestError,
        GetLogicalCatalogError, GetManifestError, GetPhysicalCatalogError, LinkManifestError,
        ListAllDatasetsError, ListDatasetsUsingManifestError, ListOrphanedManifestsError,
        ListVersionTagsError, PlanningCtxForSqlError, RegisterManifestError, ResolveRevisionError,
        SetVersionTagError, UnlinkDatasetManifestsError,
    },
    manifests::{ManifestParseError, StoreError},
};

#[derive(Clone)]
pub struct DatasetStore {
    metadata_db: MetadataDb,
    // Provider store for managing provider configurations and caching.
    provider_configs_store: ProviderConfigsStore,
    // Store for dataset definitions (manifests).
    dataset_manifests_store: DatasetManifestsStore,
    // Cache maps dataset name to eth_call UDF.
    eth_call_cache: Arc<RwLock<HashMap<String, ScalarUDF>>>,
    // This cache maps dataset name to the dataset definition.
    dataset_cache: Arc<RwLock<HashMap<datasets_common::hash::Hash, Arc<Dataset>>>>,
}

impl DatasetStore {
    pub fn new(
        metadata_db: MetadataDb,
        provider_configs_store: ProviderConfigsStore,
        dataset_manifests_store: DatasetManifestsStore,
    ) -> Arc<Self> {
        Arc::new(Self {
            metadata_db,
            provider_configs_store,
            dataset_manifests_store,
            eth_call_cache: Default::default(),
            dataset_cache: Default::default(),
        })
    }
}

// Provider configuration  management APIs
impl DatasetStore {
    /// Load provider configurations from disk and initialize the cache.
    ///
    /// Overwrites existing cache contents. Invalid configuration files are logged and skipped.
    pub async fn load_providers_into_cache(&self) {
        self.provider_configs_store.load_into_cache().await
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
    /// (such as `register_provider` and `delete_provider`). Extract the needed data
    /// immediately and drop the guard as soon as possible.
    #[must_use]
    pub async fn get_all_providers(
        &self,
    ) -> impl std::ops::Deref<Target = BTreeMap<String, ProviderConfig>> + '_ {
        self.provider_configs_store.get_all().await
    }

    /// Get a provider configuration by name, using cache if available
    pub async fn get_provider_by_name(&self, name: &str) -> Option<ProviderConfig> {
        self.provider_configs_store.get_by_name(name).await
    }

    /// Register a new provider configuration in both cache and store
    ///
    /// If a provider configuration with the same name already exists, returns a conflict error.
    pub async fn register_provider(
        &self,
        provider: ProviderConfig,
    ) -> Result<(), providers::RegisterError> {
        self.provider_configs_store.register(provider).await
    }

    /// Delete a provider configuration by name from both the store and cache
    ///
    /// This operation fails if the provider configuration file does not exist in the store,
    /// ensuring consistent delete behavior across different object store implementations.
    ///
    /// # Cache Management
    /// - If file not found: removes stale cache entry and returns `DeleteError::NotFound`
    /// - If other store errors: preserves cache (file may still exist) and propagates error
    /// - If deletion succeeds: removes from both store and cache
    pub async fn delete_provider(&self, name: &str) -> Result<(), providers::DeleteError> {
        self.provider_configs_store.delete(name).await
    }
}

// Manifest management APIs
impl DatasetStore {
    /// Store a manifest in both object store and metadata database without linking to datasets
    ///
    /// Does NOT create version tags or link to datasets. Idempotent (content-addressable storage
    /// + `ON CONFLICT DO NOTHING`). If DB registration fails after object store write, retry is safe.
    pub async fn register_manifest(
        &self,
        hash: &Hash,
        content: String,
    ) -> Result<(), RegisterManifestError> {
        let path = self
            .dataset_manifests_store
            .store(hash, content)
            .await
            .map_err(RegisterManifestError::ManifestStorage)?;

        metadata_db::manifests::register(&self.metadata_db, hash, path)
            .await
            .map_err(RegisterManifestError::MetadataRegistration)?;

        Ok(())
    }

    /// Retrieve a manifest by its content hash
    ///
    /// Resolves hash to file path via metadata database, then fetches content from object store.
    /// Returns `None` if manifest not found in DB or object store.
    pub async fn get_manifest(
        &self,
        hash: &Hash,
    ) -> Result<Option<ManifestContent>, GetManifestError> {
        let Some(path) = metadata_db::manifests::get_path(&self.metadata_db, hash)
            .await
            .map_err(GetManifestError::MetadataDbQueryPath)?
            .map(ManifestPath::from)
        else {
            return Ok(None);
        };

        let content = self
            .dataset_manifests_store
            .get(path)
            .await
            .map_err(GetManifestError::ObjectStoreError)?;

        Ok(content)
    }

    /// Delete a manifest from both metadata database and object store
    ///
    /// Uses transaction with `SELECT FOR UPDATE` to check links before deletion, preventing
    /// concurrent link creation. Returns `ManifestLinked` error if linked to any datasets.
    /// Idempotent (returns `Ok(())` if not found). Deletes from object store before commit.
    pub async fn delete_manifest(&self, hash: &Hash) -> Result<(), DeleteManifestError> {
        // Begin transaction for atomic check-and-delete
        let mut tx = self
            .metadata_db
            .begin_txn()
            .await
            .map_err(DeleteManifestError::TransactionBegin)?;

        // Check if manifest has remaining links (with row-level locking)
        // This prevents concurrent processes from linking the manifest until we commit
        let links = metadata_db::manifests::count_dataset_links_and_lock(&mut tx, hash)
            .await
            .map_err(DeleteManifestError::MetadataDbCheckLinks)?;

        if links > 0 {
            // No need to rollback explicitly - transaction drops on return
            return Err(DeleteManifestError::ManifestLinked);
        }

        // Delete from metadata database (CASCADE deletes links/tags)
        // Lock is still held, so no concurrent links can be created
        let Some(path) = metadata_db::manifests::delete(&mut tx, hash)
            .await
            .map_err(DeleteManifestError::MetadataDbDelete)?
            .map(Into::into)
        else {
            // Treat not found as success (idempotency)
            tracing::debug!(
                manifest_hash = %hash,
                "Manifest not found in metadata database (already deleted or never existed)"
            );
            return Ok(());
        };

        // Delete manifest file from object store BEFORE committing transaction
        // If this fails, transaction will roll back and manifest remains in DB
        self.dataset_manifests_store
            .delete(path)
            .await
            .map_err(DeleteManifestError::ObjectStoreError)?;

        tracing::debug!(
            manifest_hash = %hash,
            "Manifest deleted from object store"
        );

        // Commit transaction - releases locks
        // If this fails, object store file is gone but DB still has manifest
        // Retry is safe: DB delete will find nothing (idempotent), object store delete tolerates NotFound
        tx.commit()
            .await
            .map_err(DeleteManifestError::TransactionCommit)?;

        tracing::debug!(manifest_hash = %hash, "Manifest deletion completed successfully");
        Ok(())
    }

    /// List all datasets that use a specific manifest
    ///
    /// Returns all dataset tags (namespace, name, version) that reference the given
    /// manifest hash. This is useful for discovering which datasets and versions
    /// are using a particular manifest.
    ///
    /// System-managed tags ("latest" and "dev") are excluded from the results.
    ///
    /// Returns `None` if the manifest doesn't exist.
    /// Returns `Some(vec![])` (empty vector) if the manifest exists but no datasets use it.
    pub async fn list_manifest_linked_datasets(
        &self,
        manifest_hash: &Hash,
    ) -> Result<Option<Vec<DatasetTag>>, ListDatasetsUsingManifestError> {
        // Check if manifest exists
        let manifest_path = metadata_db::manifests::get_path(&self.metadata_db, manifest_hash)
            .await
            .map_err(ListDatasetsUsingManifestError::MetadataDbQueryPath)?;

        if manifest_path.is_none() {
            return Ok(None);
        }

        // Query all tags using this manifest
        let tags = metadata_db::datasets::list_tags_by_hash(&self.metadata_db, manifest_hash)
            .await
            .map_err(ListDatasetsUsingManifestError::MetadataDbListTags)?
            .into_iter()
            .map(DatasetTag::from)
            .collect();

        Ok(Some(tags))
    }

    /// List all orphaned manifests (manifests with no dataset links)
    ///
    /// Returns manifest hashes for all manifests that exist in storage but are not
    /// linked to any datasets. These manifests can be safely deleted.
    pub async fn list_orphaned_manifests(&self) -> Result<Vec<Hash>, ListOrphanedManifestsError> {
        metadata_db::manifests::list_orphaned(&self.metadata_db)
            .await
            .map(|hashes| hashes.into_iter().map(Into::into).collect())
            .map_err(ListOrphanedManifestsError)
    }
}

// Dataset versioning API
impl DatasetStore {
    /// Link an existing manifest to a dataset
    ///
    /// This method assumes the manifest already exists in the system and only performs the linking
    /// operation. It does NOT store or register the manifest - use `register_manifest` first if
    /// you need to store a new manifest.
    ///
    /// ## Operations performed (in transaction)
    /// 1. Link manifest to dataset (idempotent)
    /// 2. Update "dev" tag to point to this manifest (idempotent)
    pub async fn link_manifest(
        &self,
        namespace: &Namespace,
        name: &Name,
        manifest_hash: &Hash,
    ) -> Result<(), LinkManifestError> {
        // Use transaction to ensure both operations succeed atomically
        let mut tx = self
            .metadata_db
            .begin_txn()
            .await
            .map_err(LinkManifestError::TransactionBegin)?;

        // Link manifest to dataset (idempotent)
        // Foreign key constraint will reject if manifest doesn't exist
        if let Err(err) =
            metadata_db::datasets::link_manifest_to_dataset(&mut tx, namespace, name, manifest_hash)
                .await
        {
            return Err(if err.is_foreign_key_violation() {
                LinkManifestError::ManifestNotFound(manifest_hash.clone())
            } else {
                LinkManifestError::LinkManifestToDataset(err)
            });
        }

        // Automatically update dev tag to point to the linked manifest
        metadata_db::datasets::set_dev_tag(&mut tx, namespace, name, manifest_hash)
            .await
            .map_err(LinkManifestError::SetDevTag)?;

        tx.commit()
            .await
            .map_err(LinkManifestError::TransactionCommit)?;

        Ok(())
    }

    /// Set a semantic version tag for a dataset manifest
    ///
    /// Creates or updates version tag, then automatically updates "latest" tag if this version is higher.
    /// Uses transaction with `SELECT FOR UPDATE` on "latest" row to prevent concurrent tag updates
    /// from causing stale writes. Idempotent.
    pub async fn set_dataset_version_tag(
        &self,
        namespace: &Namespace,
        name: &Name,
        version: &Version,
        manifest_hash: &Hash,
    ) -> Result<(), SetVersionTagError> {
        // Use transaction to ensure atomic version tag + latest tag update
        let mut tx = self
            .metadata_db
            .begin_txn()
            .await
            .map_err(SetVersionTagError::MetadataDb)?;

        metadata_db::datasets::register_version_tag(
            &mut tx,
            namespace,
            name,
            version,
            manifest_hash,
        )
        .await
        .map_err(SetVersionTagError::MetadataDb)?;

        // Lock the "latest" row to prevent concurrent modifications (SELECT FOR UPDATE)
        let current_latest =
            metadata_db::datasets::get_latest_tag_and_lock(&mut tx, namespace, name)
                .await
                .map_err(SetVersionTagError::UpdateLatestTag)?
                .map(|tag| -> Version { tag.version.into() });

        // Lock held until commit, so current_latest cannot change during this transaction.
        // Update "latest" tag only if new version is higher (or no latest exists).
        if let Some(ref current_latest) = current_latest
            && version <= current_latest
        {
            // Version is not higher than current latest, no need to update latest tag
            tx.commit()
                .await
                .map_err(SetVersionTagError::TransactionCommit)?;
            return Ok(());
        }

        metadata_db::datasets::set_latest_tag(&mut tx, namespace, name, manifest_hash)
            .await
            .map_err(SetVersionTagError::UpdateLatestTag)?;

        // Commit transaction - all operations succeed or all are rolled back
        tx.commit()
            .await
            .map_err(SetVersionTagError::TransactionCommit)?;

        Ok(())
    }

    /// Resolves the "latest" tag to its manifest hash
    ///
    /// Returns the manifest hash that the "latest" tag currently points to, or None if no
    /// "latest" tag exists for this dataset.
    pub async fn resolve_latest_version_hash(
        &self,
        namespace: &Namespace,
        name: &Name,
    ) -> Result<Option<Hash>, ResolveRevisionError> {
        let hash = metadata_db::datasets::get_latest_tag_hash(&self.metadata_db, namespace, name)
            .await
            .map_err(ResolveRevisionError)?
            .map(Into::into);
        Ok(hash)
    }

    /// Resolves the "dev" tag to its manifest hash
    ///
    /// Returns the manifest hash that the "dev" tag currently points to, or None if no
    /// "dev" tag exists for this dataset.
    pub async fn resolve_dev_version_hash(
        &self,
        namespace: &Namespace,
        name: &Name,
    ) -> Result<Option<Hash>, ResolveRevisionError> {
        let hash = metadata_db::datasets::get_dev_tag_hash(&self.metadata_db, namespace, name)
            .await
            .map_err(ResolveRevisionError)?
            .map(Into::into);
        Ok(hash)
    }

    /// Resolves a semantic version tag to its manifest hash
    ///
    /// Returns the manifest hash that the specified version tag points to, or None if the
    /// version tag doesn't exist for this dataset.
    pub async fn resolve_version_hash(
        &self,
        namespace: &Namespace,
        name: &Name,
        version: &Version,
    ) -> Result<Option<Hash>, ResolveRevisionError> {
        let hash = metadata_db::datasets::get_version_tag_hash(
            &self.metadata_db,
            namespace,
            name,
            version,
        )
        .await
        .map_err(ResolveRevisionError)?
        .map(Into::into);
        Ok(hash)
    }

    /// Resolves a revision to a manifest hash for a given dataset.
    ///
    /// This is the primary resolution method used by Admin API handlers to convert
    /// revision references (version tags, hashes, or special tags) into concrete manifest hashes.
    ///
    /// For more specific use cases, consider using the dedicated methods:
    /// - `resolve_version_hash` for semantic versions
    /// - `resolve_latest_version_hash` for the "latest" tag
    /// - `resolve_dev_version_hash` for the "dev" tag
    pub async fn resolve_dataset_revision(
        &self,
        namespace: &Namespace,
        name: &Name,
        revision: &Revision,
    ) -> Result<Option<Hash>, ResolveRevisionError> {
        match revision {
            Revision::Hash(hash) => {
                // Hash is already concrete, just verify it exists in manifest storage
                let path = metadata_db::manifests::get_path(&self.metadata_db, hash)
                    .await
                    .map_err(ResolveRevisionError)?;
                Ok(path.map(|_| hash.clone()))
            }
            Revision::Version(version) => self.resolve_version_hash(namespace, name, version).await,
            Revision::Latest => self.resolve_latest_version_hash(namespace, name).await,
            Revision::Dev => self.resolve_dev_version_hash(namespace, name).await,
        }
    }

    /// List all version tags for a dataset
    ///
    /// Returns all semantic version tags for the dataset with their metadata,
    /// sorted in descending order (newest version first).
    ///
    /// Returns an empty list if the dataset has no version tags.
    pub async fn list_dataset_version_tags(
        &self,
        namespace: &Namespace,
        name: &Name,
    ) -> Result<Vec<metadata_db::DatasetTag>, ListVersionTagsError> {
        metadata_db::datasets::list_version_tags(&self.metadata_db, namespace, name)
            .await
            .map_err(ListVersionTagsError)
    }

    /// List all datasets across all namespaces
    ///
    /// Returns all dataset tags from the metadata database.
    /// Each tag represents a dataset-version combination.
    ///
    /// Returns an empty list if no datasets are registered.
    pub async fn list_all_datasets(
        &self,
    ) -> Result<Vec<metadata_db::DatasetTag>, ListAllDatasetsError> {
        metadata_db::datasets::list_all(&self.metadata_db)
            .await
            .map_err(ListAllDatasetsError)
    }

    /// Unlink all manifests from a dataset
    ///
    /// Removes all dataset-manifest associations for the dataset and returns the set of
    /// unlinked manifest hashes. This will also cascade delete all version tags due to
    /// foreign key constraints.
    ///
    /// The caller is responsible for checking if any of the returned manifests became
    /// orphaned (no remaining references) and deleting them if needed.
    ///
    /// This operation is idempotent - returns an empty set if dataset doesn't exist.
    pub async fn unlink_dataset_manifests(
        &self,
        namespace: &Namespace,
        name: &Name,
    ) -> Result<BTreeSet<Hash>, UnlinkDatasetManifestsError> {
        // Delete all dataset_manifests links
        // This will cascade delete all tags due to foreign key constraint
        let unlinked_hashes =
            metadata_db::datasets::unlink_manifests(&self.metadata_db, namespace, name)
                .await
                .map_err(UnlinkDatasetManifestsError)?
                .into_iter()
                .map(Into::into)
                .collect::<BTreeSet<Hash>>();

        tracing::debug!(
            namespace=%namespace,
            name=%name,
            unlinked_count=%unlinked_hashes.len(),
            "Unlinked dataset manifests"
        );

        Ok(unlinked_hashes)
    }

    /// Delete a version tag for a dataset
    ///
    /// Removes the specified semantic version tag from the dataset.
    /// This operation is idempotent - returns `Ok(())` if the version doesn't exist.
    ///
    /// Note: This only deletes the version tag, not the manifest itself.
    /// Manifests are content-addressable and may be referenced by other versions.
    pub async fn delete_dataset_version_tag(
        &self,
        namespace: &Namespace,
        name: &Name,
        version: &Version,
    ) -> Result<(), DeleteVersionTagError> {
        metadata_db::datasets::delete_version_tag(&self.metadata_db, namespace, name, version)
            .await
            .map_err(DeleteVersionTagError)
    }
}

// Dataset loading API
impl DatasetStore {
    /// Retrieves a dataset by name and optional version, with in-memory caching.
    ///
    /// This method resolves the dataset's manifest hash from the namespace+name+version tuple
    /// and delegates to `get_by_hash` for loading and caching:
    ///
    /// 1. Validate the dataset name and resolves the version (uses latest if not provided)
    /// 2. Resolve the manifest hash for the namespace+name+version tuple
    /// 3. Delegate to `get_by_hash` which handles caching and loading
    ///
    /// Returns `None` if the dataset cannot be found in the metadata database or manifest store.
    pub async fn get_dataset(
        &self,
        reference: impl Into<PartialReference>,
    ) -> Result<Arc<Dataset>, GetDatasetError> {
        let reference = reference.into();
        let namespace = reference.namespace_or_global();
        let name = reference.name();
        let revision = reference.revision_or_latest();

        let manifest_hash = match revision {
            Revision::Hash(hash) => hash.clone(),
            Revision::Version(_) | Revision::Latest | Revision::Dev => {
                match metadata_db::datasets::get_version_tag(
                    &self.metadata_db,
                    &namespace,
                    name,
                    DatasetVersion::from_owned_unchecked(revision.to_string()),
                )
                .await
                {
                    Ok(Some(dataset_tag)) => dataset_tag.hash.into(),
                    Ok(None) => return Err(GetDatasetError::DatasetNotFound(reference.clone())),
                    Err(source) => {
                        return Err(GetDatasetError::ManifestRetrievalError {
                            namespace: namespace.to_string(),
                            name: name.to_string(),
                            version: Some(revision.to_string()),
                            source: Box::new(source),
                        });
                    }
                }
            }
        };

        match self.get_by_hash(&manifest_hash).await {
            Ok(Some(dataset)) => {
                tracing::debug!(
                    dataset_namespace = %namespace,
                    dataset_name = %name,
                    dataset_version = %revision,
                    "Dataset loaded successfully"
                );
                Ok(dataset)
            }
            Ok(None) => Err(GetDatasetError::DatasetNotFound(reference.clone())),
            Err(e) => Err(GetDatasetError::ManifestRetrievalError {
                namespace: namespace.to_string(),
                name: name.to_string(),
                version: Some(revision.to_string()),
                source: e,
            }),
        }
    }

    /// Loads a dataset by its manifest hash, with in-memory caching.
    ///
    /// This retrieves the manifest content from the object store using the hash,
    /// parses it to determine the dataset kind, and creates the appropriate dataset instance:
    ///
    /// 1. Check the in-memory cache using the manifest hash as the key
    /// 2. On cache miss, retrieve the manifest path from metadata DB using the hash
    /// 3. Retrieve the manifest content from the manifest store using the path
    /// 4. Parse the common manifest structure to identify the dataset kind
    /// 5. Parse the kind-specific manifest (EvmRpc, EthBeacon, Firehose, or Derived)
    /// 6. Create the typed dataset instance, cache it, and return
    ///
    /// Returns `None` if the manifest cannot be found in the store, or an error if parsing or
    /// [`Dataset`] instance creation fails.
    #[instrument(skip(self), err)]
    pub async fn get_by_hash(
        &self,
        manifest_hash: &Hash,
    ) -> Result<Option<Arc<Dataset>>, BoxError> {
        // Check cache using manifest hash as the key
        if let Some(dataset) = self.dataset_cache.read().get(manifest_hash).cloned() {
            tracing::trace!(
                manifest_hash = %manifest_hash,
                "Cache hit, returning cached dataset"
            );
            return Ok(Some(dataset));
        }

        tracing::debug!(
            manifest_hash = %manifest_hash,
            "Cache miss, loading from store"
        );

        // Get the manifest path from metadata database
        let Some(path) = metadata_db::manifests::get_path(&self.metadata_db, manifest_hash)
            .await?
            .map(ManifestPath::from)
        else {
            return Ok(None);
        };

        // Load the manifest content using the path
        let Some(manifest_content) = self.dataset_manifests_store.get(path).await? else {
            return Ok(None);
        };

        let manifest = manifest_content.try_into_manifest::<CommonManifest>()?;

        let kind: DatasetKind = manifest.kind.parse()?;

        let dataset = match kind {
            DatasetKind::EvmRpc => {
                let manifest = manifest_content.try_into_manifest::<EvmRpcManifest>()?;
                evm_rpc_datasets::dataset(manifest_hash.clone(), manifest)
            }
            DatasetKind::EthBeacon => {
                let manifest = manifest_content.try_into_manifest::<EthBeaconManifest>()?;
                eth_beacon_datasets::dataset(manifest_hash.clone(), manifest)
            }
            DatasetKind::Firehose => {
                let manifest = manifest_content.try_into_manifest::<FirehoseManifest>()?;
                firehose_datasets::evm::dataset(manifest_hash.clone(), manifest)
            }
            DatasetKind::Derived => {
                let manifest = manifest_content.try_into_manifest::<DerivedManifest>()?;
                derived::dataset(manifest_hash.clone(), manifest)?
            }
        };

        let dataset = Arc::new(dataset);

        // Cache the dataset
        self.dataset_cache
            .write()
            .insert(manifest_hash.clone(), dataset.clone());

        tracing::debug!(
            manifest_hash = %manifest_hash,
            "Dataset loaded (and cached) successfully"
        );

        Ok(Some(dataset))
    }

    #[tracing::instrument(skip(self), err)]
    pub async fn get_derived_manifest(
        &self,
        manifest_hash: &datasets_common::hash::Hash,
    ) -> Result<DerivedManifest, GetDerivedManifestError> {
        // Get the manifest path from metadata database
        let Some(path) = metadata_db::manifests::get_path(&self.metadata_db, manifest_hash)
            .await
            .map_err(|source| GetDerivedManifestError::ManifestRetrievalError {
                source: Box::new(source),
            })?
            .map(ManifestPath::from)
        else {
            return Err(GetDerivedManifestError::ManifestNotRegistered(
                manifest_hash.clone(),
            ));
        };

        // Load the manifest content using the path
        let Some(dataset_src) = self
            .dataset_manifests_store
            .get(path)
            .await
            .map_err(|err| GetDerivedManifestError::ManifestRetrievalError {
                source: Box::new(err),
            })?
        else {
            return Err(GetDerivedManifestError::ManifestNotFound(
                manifest_hash.clone(),
            ));
        };

        let manifest = dataset_src
            .try_into_manifest::<CommonManifest>()
            .map_err(|err| GetDerivedManifestError::ManifestParseError { source: err })?;

        let kind = DatasetKind::from_str(&manifest.kind).map_err(|err: UnsupportedKindError| {
            GetDerivedManifestError::UnsupportedKind { kind: err.kind }
        })?;

        if kind != DatasetKind::Derived {
            return Err(GetDerivedManifestError::UnsupportedKind {
                kind: kind.to_string(),
            });
        }

        let manifest = dataset_src
            .try_into_manifest::<DerivedManifest>()
            .map_err(|err| GetDerivedManifestError::ManifestParseError { source: err })?;

        Ok(manifest)
    }

    #[tracing::instrument(skip(self), err)]
    pub async fn get_client(
        &self,
        manifest_hash: &datasets_common::hash::Hash,
        meter: Option<&monitoring::telemetry::metrics::Meter>,
    ) -> Result<impl BlockStreamer, GetClientError> {
        // Get the manifest path from metadata database
        let Some(path) = metadata_db::manifests::get_path(&self.metadata_db, manifest_hash)
            .await
            .map_err(|source| GetClientError::ManifestRetrievalError {
                source: Box::new(source),
            })?
            .map(ManifestPath::from)
        else {
            return Err(GetClientError::ManifestNotRegistered(manifest_hash.clone()));
        };

        // Load the manifest content using the path
        let Some(manifest_content) =
            self.dataset_manifests_store
                .get(path)
                .await
                .map_err(|err| GetClientError::ManifestRetrievalError {
                    source: Box::new(err),
                })?
        else {
            return Err(GetClientError::ManifestNotFound(manifest_hash.clone()));
        };

        let manifest = manifest_content
            .try_into_manifest::<CommonManifest>()
            .map_err(|err| GetClientError::CommonManifestParseError { source: err })?;

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

        let Some(config) = self.find_provider(kind, network.clone()).await else {
            tracing::warn!(
                provider_kind = %manifest.kind,
                provider_network = %network,
                "no providers available for the requested kind-network configuration"
            );

            return Err(GetClientError::ProviderNotFound {
                dataset_kind: kind,
                network,
            });
        };

        let provider_name = config.name.clone();
        let client = match kind {
            DatasetKind::EvmRpc => {
                let config = config
                    .try_into_config::<EvmRpcProviderConfig>()
                    .map_err(|err| GetClientError::ProviderConfigParseError {
                        name: provider_name.clone(),
                        source: err,
                    })?;
                evm_rpc_datasets::client(config, meter)
                    .await
                    .map(BlockStreamClient::EvmRpc)
                    .map_err(|err| GetClientError::EvmRpcClientError {
                        name: provider_name.clone(),
                        source: err,
                    })?
            }
            DatasetKind::EthBeacon => {
                let config = config
                    .try_into_config::<EthBeaconProviderConfig>()
                    .map_err(|err| GetClientError::ProviderConfigParseError {
                        name: provider_name.clone(),
                        source: err,
                    })?;
                BlockStreamClient::EthBeacon(eth_beacon_datasets::client(config))
            }
            DatasetKind::Firehose => {
                let config = config
                    .try_into_config::<FirehoseProviderConfig>()
                    .map_err(|err| GetClientError::ProviderConfigParseError {
                        name: provider_name.clone(),
                        source: err,
                    })?;
                firehose_datasets::Client::new(config, meter)
                    .await
                    .map(|c| BlockStreamClient::Firehose(Box::new(c)))
                    .map_err(|err| GetClientError::FirehoseClientError {
                        name: provider_name.clone(),
                        source: err,
                    })?
            }
            _ => {
                unreachable!("non-raw dataset kinds are filtered out earlier");
            }
        };

        Ok(client.with_retry())
    }

    async fn find_provider(&self, kind: DatasetKind, network: String) -> Option<ProviderConfig> {
        // Collect matching provider configurations into a vector for shuffling
        let mut matching_providers = self
            .provider_configs_store
            .get_all()
            .await
            .values()
            .filter(|prov| prov.kind == kind && prov.network == network)
            .cloned()
            .collect::<Vec<_>>();

        if matching_providers.is_empty() {
            return None;
        }

        // Try each provider in random order until we find one with successful env substitution
        matching_providers.shuffle(&mut rand::rng());

        'try_find_provider: for mut provider in matching_providers {
            // Apply environment variable substitution to the `rest` table values
            for (_key, value) in provider.rest.iter_mut() {
                if let Err(err) = env_substitute::substitute_env_vars(value) {
                    tracing::warn!(
                        provider_name = %provider.name,
                        provider_kind = %kind,
                        provider_network = %network,
                        error = %err,
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

        // If we get here, no suitable providers were found
        None
    }

    /// Creates a `QueryContext` for a SQL query, this will infer and load any dependencies. The
    /// procedure is:
    ///
    /// 1. Collect table references in the query.
    /// 2. Assume that in `foo.bar`, `foo` is a dataset name.
    /// 3. Look up the dataset names in the configured dataset store.
    /// 4. Collect the datasets into a catalog.
    pub async fn catalog_for_sql(
        &self,
        query: &parser::Statement,
        env: QueryEnv,
    ) -> Result<Catalog, CatalogForSqlError> {
        let (tables, _) = resolve_table_references(query, true)
            .map_err(|err| CatalogForSqlError::TableReferenceResolution { source: err.into() })?;
        let function_names = sql_visitors::all_function_names(query)
            .map_err(|err| CatalogForSqlError::FunctionNameExtraction { source: err })?;

        self.get_physical_catalog(tables, function_names, &env)
            .await
            .map_err(CatalogForSqlError::GetPhysicalCatalog)
    }

    /// Looks up the datasets for the given table references and gets them into a catalog.
    pub async fn get_physical_catalog(
        &self,
        table_refs: impl IntoIterator<Item = TableReference>,
        function_names: impl IntoIterator<Item = String>,
        env: &QueryEnv,
    ) -> Result<Catalog, GetPhysicalCatalogError> {
        let logical_catalog = self
            .get_logical_catalog(table_refs, function_names, &env.isolate_pool)
            .await
            .map_err(GetPhysicalCatalogError::GetLogicalCatalog)?;

        let mut tables = Vec::new();
        for table in &logical_catalog.tables {
            let physical_table = PhysicalTable::get_active(table, self.metadata_db.clone())
                .await
                .map_err(|err| GetPhysicalCatalogError::PhysicalTableRetrieval {
                    table: table.to_string(),
                    source: err,
                })?
                .ok_or(GetPhysicalCatalogError::TableNotSynced {
                    table: table.to_string(),
                })?;
            tables.push(physical_table.into());
        }
        Ok(Catalog::new(tables, logical_catalog))
    }

    /// Similar to `catalog_for_sql`, but only for planning and not execution. This does not require a
    /// physical location to exist for the dataset views.
    pub async fn planning_ctx_for_sql(
        &self,
        query: &parser::Statement,
    ) -> Result<PlanningContext, PlanningCtxForSqlError> {
        let (tables, _) = resolve_table_references(query, true).map_err(|err| {
            PlanningCtxForSqlError::TableReferenceResolution { source: err.into() }
        })?;
        let function_names = sql_visitors::all_function_names(query)
            .map_err(|err| PlanningCtxForSqlError::FunctionNameExtraction { source: err })?;
        let resolved_tables = self
            .get_logical_catalog(tables, function_names, &IsolatePool::dummy())
            .await?;
        Ok(PlanningContext::new(resolved_tables))
    }

    /// Looks up the datasets for the given table references and creates resolved tables. Create
    /// UDFs specific to the referenced datasets.
    async fn get_logical_catalog(
        &self,
        table_refs: impl IntoIterator<Item = TableReference>,
        function_names: impl IntoIterator<Item = String>,
        isolate_pool: &IsolatePool,
    ) -> Result<LogicalCatalog, GetLogicalCatalogError> {
        let table_refs = table_refs.into_iter().collect::<Vec<_>>();
        let function_names = function_names.into_iter().collect::<Vec<_>>();

        let datasets = {
            let mut datasets = BTreeSet::new();

            let table_refs_datasets = dataset_versions_from_table_refs(table_refs.iter())
                .map_err(GetLogicalCatalogError::ExtractDatasetFromTableRefs)?;
            tracing::debug!(
                table_refs_datasets = ?table_refs_datasets,
                "Extracted datasets from table references"
            );
            datasets.extend(table_refs_datasets);

            let function_datasets =
                dataset_versions_from_function_names(function_names.iter().map(AsRef::as_ref))
                    .map_err(GetLogicalCatalogError::ExtractDatasetFromFunctionNames)?;
            tracing::debug!(
                function_datasets = ?function_datasets,
                "Extracted datasets from function names"
            );
            datasets.extend(function_datasets);

            tracing::debug!(
                total_datasets = ?datasets,
                "Combined datasets from table references and function names"
            );
            datasets
        };

        let mut resolved_tables = Vec::new();
        let mut udfs = Vec::new();
        for partial_ref in datasets {
            let dataset = self
                .get_dataset(partial_ref.clone())
                .await
                .map_err(GetLogicalCatalogError::GetDataset)?;

            if dataset.kind == EvmRpcDatasetKind {
                let udf = self
                    .eth_call_for_dataset(&partial_ref.to_string(), &dataset)
                    .await
                    .map_err(|err| GetLogicalCatalogError::EthCallUdfCreation {
                        dataset: partial_ref.name.to_string(),
                        source: err,
                    })?;
                if let Some(udf) = udf {
                    udfs.push(udf);
                }
            }

            // Add JS UDFs
            for udf in dataset.functions(partial_ref.name.to_string(), isolate_pool.clone()) {
                udfs.push(udf.into());
            }

            for table in &dataset.tables {
                // Only include tables that are actually referenced in the query
                let is_referenced = table_refs.iter().any(|table_ref| {
                    match (table_ref.schema(), table_ref.table()) {
                        (Some(schema), table_name) => {
                            // Reconstruct the schema name from partial reference
                            let schema_name = partial_ref.to_string();
                            schema == schema_name && table_name == table.name()
                        }
                        _ => false, // Unqualified table
                    }
                });

                if is_referenced {
                    // Use partial reference string representation as schema name
                    let schema_name = partial_ref.to_string();
                    let table_ref =
                        TableReference::partial(schema_name.clone(), table.name().to_string());
                    let resolved_table =
                        common::ResolvedTable::new(table.clone(), dataset.clone(), table_ref);
                    resolved_tables.push(resolved_table);
                }
            }
        }

        Ok(LogicalCatalog {
            tables: resolved_tables,
            udfs,
        })
    }

    /// Returns cached eth_call scalar UDF, otherwise loads the UDF and caches it.
    ///
    /// The function will be named `<catalog_schema>.<eth_call>`.
    async fn eth_call_for_dataset(
        &self,
        catalog_schema: &str,
        dataset: &Dataset,
    ) -> Result<Option<ScalarUDF>, EthCallForDatasetError> {
        if dataset.kind != EvmRpcDatasetKind {
            return Ok(None);
        }

        // Check if we already have the provider cached.
        let cache_key = dataset.manifest_hash().as_str();
        if let Some(udf) = self.eth_call_cache.read().get(cache_key) {
            return Ok(Some(udf.clone()));
        }

        // Load the provider from the dataset definition.
        let Some(network) = &dataset.network else {
            tracing::warn!(
                manifest_hash = %dataset.manifest_hash(),
                "dataset is missing required 'network' field for evm-rpc kind"
            );
            return Err(EthCallForDatasetError::MissingNetwork {
                manifest_hash: dataset.manifest_hash().to_string(),
            });
        };

        let Some(config) = self
            .find_provider(DatasetKind::EvmRpc, network.clone())
            .await
        else {
            tracing::warn!(
                provider_kind = %DatasetKind::EvmRpc,
                provider_network = %network,
                "no providers available for the requested kind-network configuration"
            );
            return Err(EthCallForDatasetError::ProviderNotFound {
                dataset_kind: DatasetKind::EvmRpc,
                network: network.clone(),
            });
        };

        // Internal struct for extracting specific provider config fields.
        #[derive(serde::Deserialize)]
        struct EvmRpcProviderConfig {
            url: Url,
            rate_limit_per_minute: Option<NonZeroU32>,
        }

        let provider = config
            .try_into_config::<EvmRpcProviderConfig>()
            .map_err(EthCallForDatasetError::ProviderConfigParse)?;

        // Cache the provider.
        let provider = if provider.url.scheme() == "ipc" {
            evm::provider::new_ipc(provider.url.path(), provider.rate_limit_per_minute)
                .await
                .map_err(EthCallForDatasetError::IpcConnection)?
        } else {
            evm::provider::new(provider.url, provider.rate_limit_per_minute)
        };

        let udf =
            AsyncScalarUDF::new(Arc::new(EthCall::new(catalog_schema, provider))).into_scalar_udf();

        // Cache the EthCall UDF
        self.eth_call_cache
            .write()
            .insert(cache_key.to_string(), udf.clone());

        Ok(Some(udf))
    }
}

/// Extracts dataset names and versions from table references in SQL queries.
///
/// This function processes table references from SQL queries and extracts the dataset names
/// and optional versions from the schema portion of qualified table names.
///
/// # Table Reference Format
/// - All tables must be qualified with namespace and dataset name: `namespace/dataset_name.table_name`
/// - Versioned datasets: `namespace/dataset_name@version.table_name` (e.g., `_/eth_rpc@1.0.0.blocks`)
/// - Unversioned datasets: `namespace/dataset_name.table_name` (e.g., `_/eth_rpc.blocks`)
/// - Catalog-qualified tables are not supported
///
/// # Returns
/// A set of unique partial references extracted from the table references.
fn dataset_versions_from_table_refs<'a>(
    table_refs: impl Iterator<Item = &'a TableReference>,
) -> Result<BTreeSet<PartialReference>, ExtractDatasetFromTableRefsError> {
    let mut datasets = BTreeSet::new();

    for table_ref in table_refs {
        if table_ref.catalog().is_some() {
            return Err(ExtractDatasetFromTableRefsError::CatalogQualifiedTable {
                table: table_ref.table().to_string(),
            });
        }

        let Some(catalog_schema) = table_ref.schema() else {
            return Err(ExtractDatasetFromTableRefsError::UnqualifiedTable {
                table: table_ref.table().to_string(),
            });
        };

        // Parse using PartialReference to handle namespace/name@version format
        let partial_ref = catalog_schema.parse::<PartialReference>().map_err(|err| {
            ExtractDatasetFromTableRefsError::ReferenceParse {
                schema: catalog_schema.to_string(),
                source: err,
            }
        })?;

        // Validate that revision (if present) is a Version type, not hash/latest/dev
        if let Some(revision) = &partial_ref.revision
            && !revision.is_version()
        {
            return Err(ExtractDatasetFromTableRefsError::InvalidVersion {
                version: revision.to_string(),
                schema: catalog_schema.to_string(),
            });
        }

        datasets.insert(partial_ref);
    }

    Ok(datasets)
}

/// Extracts dataset names and versions from function names in SQL queries.
///
/// This function processes qualified function names (e.g., `namespace/dataset.function` or `namespace/dataset@version.function`)
/// and extracts the dataset name and optional version from the qualifier.
///
/// # Function Name Format
/// - Simple function names (no qualifier) are assumed to be built-in DataFusion functions
/// - Qualified functions: `namespace/dataset_name.function_name` or `namespace/dataset_name@version.function_name`
/// - Examples: `_/eth_rpc.my_function()` or `_/eth_rpc@1.0.0.my_function()`
///
/// # Returns
/// A set of unique partial references extracted from the function names.
fn dataset_versions_from_function_names<'a>(
    function_names: impl IntoIterator<Item = &'a str>,
) -> Result<BTreeSet<PartialReference>, ExtractDatasetFromFunctionNamesError> {
    let mut datasets = BTreeSet::new();

    for func_name in function_names {
        let parts: Vec<_> = func_name.split('.').collect();
        let fn_dataset = match parts.as_slice() {
            // Simple name assumed to be Datafusion built-in function.
            [_] => continue,
            [dataset, _] => dataset,
            _ => {
                return Err(
                    ExtractDatasetFromFunctionNamesError::InvalidFunctionFormat {
                        function: func_name.to_string(),
                    },
                );
            }
        };

        // Parse using PartialReference to handle namespace/name@version format
        let partial_ref = fn_dataset.parse::<PartialReference>().map_err(|err| {
            ExtractDatasetFromFunctionNamesError::ReferenceParse {
                function: func_name.to_string(),
                source: err,
            }
        })?;

        // Validate that revision (if present) is a Version type, not hash/latest/dev
        if let Some(revision) = &partial_ref.revision
            && !revision.is_version()
        {
            return Err(ExtractDatasetFromFunctionNamesError::InvalidVersion {
                version: revision.to_string(),
                function: func_name.to_string(),
            });
        }

        datasets.insert(partial_ref);
    }

    Ok(datasets)
}

/// Return a table identifier, in the form `{dataset}.blocks`, for the given network.
#[tracing::instrument(skip(dataset_store), err)]
pub async fn resolve_blocks_table(
    dataset_store: &Arc<DatasetStore>,
    root_datasets: BTreeMap<datasets_common::hash::Hash, Arc<Dataset>>,
    network: &str,
) -> Result<PhysicalTable, BoxError> {
    let dataset =
        search_dependencies_for_raw_dataset(dataset_store, root_datasets, network).await?;

    // TODO: Have a dataset name here that is not made up.
    let dataset_name = Name::try_from("blocks_table".to_string())?;
    let manifest_hash = dataset.manifest_hash().clone();
    let reference = PartialReference::new(
        None,
        dataset_name,
        Some(Revision::Hash(manifest_hash.clone())),
    );
    let table = Arc::new(dataset)
        .resolved_tables(reference)
        .find(|t| t.name() == "blocks")
        .ok_or_else(|| {
            BoxError::from(format!(
                "dataset '{}' does not have a 'blocks' table",
                manifest_hash
            ))
        })?;

    PhysicalTable::get_active(&table, dataset_store.metadata_db.clone())
        .await?
        .ok_or_else(|| {
            BoxError::from(format!(
                "table '{}.{}' has not been synced",
                manifest_hash,
                table.name()
            ))
        })
}

// Breadth-first search over dataset dependencies to find a raw dataset matching the target network.
async fn search_dependencies_for_raw_dataset(
    dataset_store: &Arc<DatasetStore>,
    root_datasets: BTreeMap<datasets_common::hash::Hash, Arc<Dataset>>,
    network: &str,
) -> Result<Arc<Dataset>, BoxError> {
    let mut queue: VecDeque<Arc<Dataset>> = root_datasets.values().cloned().collect();
    let mut visited = BTreeSet::new();

    while let Some(dataset) = queue.pop_front() {
        let hash = dataset.manifest_hash().clone();

        // Skip duplicates
        if !visited.insert(hash) {
            continue;
        }

        if dataset.kind != DerivedDatasetKind
            && let Some(dataset_network) = dataset.network.as_ref()
            && dataset_network == network
        {
            // Found matching dataset
            return Ok(dataset);
        }

        // Enqueue dependencies for exploration
        for reference in dataset.dependencies.values() {
            let dataset = dataset_store.get_dataset(reference).await?;
            queue.push_back(dataset);
        }
    }

    Err(format!("no raw dataset found for network '{network}'",).into())
}

/// Return the input datasets and their dataset dependencies. The output set is ordered such that
/// each dataset comes after all datasets it depends on.
pub async fn dataset_and_dependencies(
    store: &Arc<DatasetStore>,
    dataset: Reference,
) -> Result<Vec<Reference>, BoxError> {
    let mut datasets = vec![dataset];
    let mut deps: BTreeMap<Reference, Vec<Reference>> = Default::default();
    while let Some(dataset_ref) = datasets.pop() {
        let dataset = store.get_dataset(&dataset_ref).await?;

        if dataset.kind != DerivedDatasetKind {
            deps.insert(dataset_ref, vec![]);
            continue;
        }

        let refs: Vec<Reference> = dataset.dependencies.values().cloned().collect();
        let mut untracked_refs = refs
            .iter()
            .filter(|r| deps.keys().all(|d| d != *r))
            .cloned()
            .collect();
        datasets.append(&mut untracked_refs);
        deps.insert(dataset_ref, refs);
    }

    dependency_sort(deps)
}

/// Given a map of values to their dependencies, return a set where each value is ordered after
/// all of its dependencies. An error is returned if a cycle is detected.
fn dependency_sort(deps: BTreeMap<Reference, Vec<Reference>>) -> Result<Vec<Reference>, BoxError> {
    let nodes: BTreeSet<&Reference> = deps
        .iter()
        .flat_map(|(ds, deps)| std::iter::once(ds).chain(deps))
        .collect();
    let mut ordered: Vec<Reference> = Default::default();
    let mut visited: BTreeSet<&Reference> = Default::default();
    let mut visited_cycle: BTreeSet<&Reference> = Default::default();
    for node in nodes {
        if !visited.contains(node) {
            common::utils::dfs(node, &deps, &mut ordered, &mut visited, &mut visited_cycle)?;
        }
    }
    Ok(ordered)
}

/// Dataset tag information
///
/// Represents a dataset version tag that points to a specific manifest.
/// This is the dataset-store's public representation of dataset tags,
/// mapped from the internal metadata-db representation.
#[derive(Debug, Clone)]
pub struct DatasetTag {
    /// Dataset namespace identifier
    pub namespace: Namespace,
    /// Dataset name
    pub name: Name,
    /// Version tag
    pub version: Version,
    /// Manifest hash this tag references
    pub hash: Hash,
}

impl From<metadata_db::DatasetTag> for DatasetTag {
    fn from(tag: metadata_db::DatasetTag) -> Self {
        Self {
            namespace: tag.namespace.into(),
            name: tag.name.into(),
            version: tag.version.into(),
            hash: tag.hash.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use datasets_common::revision::Revision;

    #[test]
    fn dependency_sort_order() {
        #[allow(clippy::type_complexity)]
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
