use std::{
    collections::{BTreeMap, BTreeSet},
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
    reference::Reference, version::Version,
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
use metadata_db::MetadataDb;
use parking_lot::RwLock;
use rand::seq::SliceRandom as _;
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
    manifests::DatasetManifestsStore,
    providers::{ProviderConfig, ProviderConfigsStore},
};
pub use self::{
    dataset_kind::{DatasetKind, UnsupportedKindError},
    error::{
        CatalogForSqlError, EthCallForDatasetError, ExtractDatasetFromFunctionNamesError,
        ExtractDatasetFromTableRefsError, GetAllDatasetsError, GetClientError, GetDatasetError,
        GetDerivedManifestError, GetLogicalCatalogError, GetPhysicalCatalogError,
        PlanningCtxForSqlError, RegisterManifestError, SetVersionTagError,
    },
    manifests::StoreError,
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
    dataset_cache: Arc<RwLock<HashMap<String, Dataset>>>,
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

    /// Get a reference to the providers configuration store
    pub fn providers(&self) -> &ProviderConfigsStore {
        &self.provider_configs_store
    }

    /// Register a dataset manifest in both the dataset store and metadata database.
    ///
    /// This operation stores the manifest file and registers it in the metadata database.
    /// The operation is idempotent - if the manifest already exists, no error is raised.
    ///
    /// After registration, the "dev" tag is automatically updated to point to this manifest,
    /// making it the most recently registered manifest for the dataset.
    ///
    /// The manifest must be provided as a JSON string (canonical serialized form).
    ///
    /// ## Transaction Guarantees
    ///
    /// This method performs operations in two phases:
    /// 1. **Object Storage** (outside transaction): Store manifest file - idempotent if file already exists
    /// 2. **Database Transaction**: Link manifest to dataset AND update dev tag - both succeed atomically or both fail
    ///
    /// If the database transaction fails, the manifest file will exist in object storage but won't be
    /// linked to any dataset, which is safe due to idempotency and eventual consistency. The operation
    /// can be safely retried from the beginning.
    pub async fn register_manifest(
        &self,
        namespace: &Namespace,
        name: &Name,
        manifest_hash: &Hash,
        manifest_str: String,
    ) -> Result<(), RegisterManifestError> {
        // Store manifest file and register in metadata database (idempotent)
        self.dataset_manifests_store
            .store(manifest_hash, manifest_str)
            .await?;

        // Use transaction to ensure both operations succeed atomically
        let mut tx = self
            .metadata_db
            .begin_txn()
            .await
            .map_err(RegisterManifestError::MetadataRegistration)?;

        // Link manifest to dataset (idempotent)
        metadata_db::datasets::link_manifest_to_dataset(&mut tx, namespace, name, manifest_hash)
            .await
            .map_err(RegisterManifestError::MetadataRegistration)?;

        // Automatically update dev tag to point to the newly registered manifest
        metadata_db::datasets::set_dev_tag(&mut tx, namespace, name, manifest_hash)
            .await
            .map_err(RegisterManifestError::MetadataRegistration)?;

        // Commit transaction - both operations succeed or both are rolled back
        tx.commit()
            .await
            .map_err(RegisterManifestError::TransactionCommit)?;

        Ok(())
    }

    /// Set a semantic version tag for a dataset manifest.
    ///
    /// Creates or updates a semantic version tag (e.g., "1.0.0", "2.3.1") that points to a manifest.
    /// The manifest must already be registered, otherwise an error will occur.
    ///
    /// After setting the version tag, this method automatically updates the "latest" tag if the new
    /// version is higher than the current "latest" version.
    ///
    /// ## Idempotency
    ///
    /// This operation is idempotent:
    /// - If the tag doesn't exist, it is created
    /// - If the tag exists with the same hash, no changes are made
    /// - If the tag exists with a different hash, it is updated
    ///
    /// ## Transaction Guarantees
    ///
    /// This method uses a database transaction with row-level locking to ensure atomicity
    /// and prevent race conditions when updating the "latest" tag:
    ///
    /// - **Atomicity**: Version tag registration and "latest" tag update succeed together or fail together
    /// - **Isolation**: Uses `SELECT FOR UPDATE` to lock the "latest" tag row, preventing concurrent
    ///   processes from reading stale values or creating race conditions
    /// - **Consistency**: The "latest" tag is guaranteed to point to the highest version number,
    ///   even under concurrent version tag registrations
    ///
    /// ### Race Condition Prevention
    ///
    /// Without row locking, two concurrent processes could both read the same "latest" version,
    /// both determine they should update it, and the last writer would win regardless of which
    /// version is higher. The `get_latest_tag_for_update` call prevents this by ensuring only
    /// one transaction can read and modify the "latest" tag at a time.
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
            metadata_db::datasets::get_latest_tag_for_update(&mut tx, namespace, name)
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

    /// Retrieves a dataset by name and optional version, with in-memory caching.
    ///
    /// This method resolves the dataset's manifest hash from the namespace+name+version tuple,
    /// checks an in-memory cache keyed by the namespace+name+version, and loads the dataset
    /// from storage on cache miss:
    ///
    /// 1. Validate the dataset name and resolves the version (uses latest if not provided)
    /// 2. Check the in-memory cache using the namespace+name+version as the key
    /// 3. On cache miss, resolve the manifest hash for the namespace+name+version tuple
    /// 4. Load the dataset from storage and caches it
    ///
    /// Returns `None` if the dataset cannot be found in the metadata database or manifest store.
    pub async fn get_dataset(
        &self,
        name: &str,
        version: impl Into<Option<&Version>>,
    ) -> Result<Option<Dataset>, GetDatasetError> {
        let (namespace, name, version) = {
            // TODO: Pass the actual namespace instead of using a placeholder
            let namespace = "_"
                .parse::<Namespace>()
                .expect("'_' should be a valid namespace");
            let name = name
                .parse::<Name>()
                .map_err(|err| GetDatasetError::InvalidDatasetName {
                    name: name.to_string(),
                    source: err,
                })?;

            // If no version is provided, try to get the latest version from the metadata database
            // or use a placeholder version (default version, i.e., v0.0.0) if not found.
            let version = match version.into() {
                Some(v) => v.clone(),
                None => metadata_db::datasets::get_latest_tag(&self.metadata_db, &namespace, &name)
                    .await
                    .map_err(|source| GetDatasetError::GetLatestVersion {
                        namespace: namespace.to_string(),
                        name: name.to_string(),
                        source,
                    })?
                    .map(|details| details.version.into())
                    .unwrap_or_default(),
            };

            (namespace, name, version)
        };

        // Check cache using `<namespace>/<name>@<version>` as the key
        let cache_key = format!("{}/{}@{}", &namespace, &name, &version);
        if let Some(dataset) = self.dataset_cache.read().get(&cache_key).cloned() {
            tracing::trace!(
                dataset_namespace = %namespace,
                dataset_name = %name,
                dataset_version = %version,
                "Cache hit, returning cached dataset"
            );
            return Ok(Some(dataset));
        }

        tracing::debug!(
            dataset_namespace = %namespace,
            dataset_name = %name,
            dataset_version = %version,
            "Cache miss, loading from store"
        );

        let Some(dataset) = self.load_dataset(&namespace, &name, &version).await? else {
            return Ok(None);
        };

        // Cache the dataset.
        self.dataset_cache
            .write()
            .insert(cache_key, dataset.clone());

        tracing::debug!(
            dataset_namespace = %namespace,
            dataset_name = %name,
            dataset_version = %version,
            "Dataset loaded (and cached) successfully"
        );

        Ok(Some(dataset))
    }

    /// Loads a dataset by retrieving and parsing its manifest content.
    ///
    /// This function fetches the manifest hash from the metadata DB, then retrieves the manifest
    /// content from the object store, parses it to determine the dataset kind, and creates the
    /// appropriate dataset instance:
    ///
    /// 1. Resolve the manifest hash for the namespace/name/version from metadata DB
    /// 2. Retrieve the manifest content from the manifest store using the hash
    /// 3. Parse the common manifest structure to identify the dataset kind
    /// 4. Parse the kind-specific manifest (EvmRpc, EthBeacon, Firehose, or Derived)
    /// 5. Create and return the typed dataset instance
    ///
    /// Returns `None` if the manifest cannot be found in the store, or an error if parsing or
    /// [`Dataset`] instance creation fails.
    async fn load_dataset(
        &self,
        namespace: &Namespace,
        name: &Name,
        version: &Version,
    ) -> Result<Option<Dataset>, GetDatasetError> {
        // Get the manifest hash from the metadata database
        let Some(dataset_details) =
            metadata_db::datasets::get_version_tag(&self.metadata_db, namespace, name, version)
                .await
                .map_err(|source| GetDatasetError::ManifestRetrievalError {
                    namespace: namespace.to_string(),
                    name: name.to_string(),
                    version: Some(version.to_string()),
                    source: Box::new(source),
                })?
        else {
            return Ok(None);
        };

        // Load the manifest content using the hash
        let manifest_hash: Hash = dataset_details.hash.into();
        let Some(manifest_content) = self
            .dataset_manifests_store
            .get(&manifest_hash)
            .await
            .map_err(|err| GetDatasetError::ManifestRetrievalError {
                namespace: namespace.to_string(),
                name: name.to_string(),
                version: Some(version.to_string()),
                source: Box::new(err),
            })?
        else {
            return Ok(None);
        };

        let manifest = manifest_content
            .try_into_manifest::<CommonManifest>()
            .map_err(|err| GetDatasetError::ManifestParseError {
                namespace: namespace.to_string(),
                name: name.to_string(),
                version: Some(version.to_string()),
                source: err,
            })?;

        tracing::debug!(
            dataset_namespace = %namespace,
            dataset_name = %name,
            dataset_version = %version,
            dataset_kind = %manifest.kind,
            "Loaded dataset manifest: {manifest:?}"
        );

        let kind: DatasetKind = manifest.kind.parse().map_err(|err: UnsupportedKindError| {
            GetDatasetError::UnsupportedKind {
                namespace: namespace.to_string(),
                name: name.to_string(),
                version: Some(version.to_string()),
                kind: err.kind,
            }
        })?;

        let dataset = match kind {
            DatasetKind::EvmRpc => {
                let manifest = manifest_content
                    .try_into_manifest::<EvmRpcManifest>()
                    .map_err(|err| GetDatasetError::ManifestParseError {
                        namespace: namespace.to_string(),
                        name: name.to_string(),
                        version: Some(version.to_string()),
                        source: err,
                    })?;
                evm_rpc_datasets::dataset(manifest)
            }
            DatasetKind::EthBeacon => {
                let manifest = manifest_content
                    .try_into_manifest::<EthBeaconManifest>()
                    .map_err(|err| GetDatasetError::ManifestParseError {
                        namespace: namespace.to_string(),
                        name: name.to_string(),
                        version: Some(version.to_string()),
                        source: err,
                    })?;
                eth_beacon_datasets::dataset(manifest)
            }
            DatasetKind::Firehose => {
                let manifest = manifest_content
                    .try_into_manifest::<FirehoseManifest>()
                    .map_err(|err| GetDatasetError::ManifestParseError {
                        namespace: namespace.to_string(),
                        name: name.to_string(),
                        version: Some(version.to_string()),
                        source: err,
                    })?;
                firehose_datasets::evm::dataset(manifest)
            }
            DatasetKind::Derived => {
                let manifest = manifest_content
                    .try_into_manifest::<DerivedManifest>()
                    .map_err(|err| GetDatasetError::ManifestParseError {
                        namespace: namespace.to_string(),
                        name: name.to_string(),
                        version: Some(version.to_string()),
                        source: err,
                    })?;
                derived::dataset(manifest).map_err(|err| GetDatasetError::DerivedCreationError {
                    namespace: namespace.to_string(),
                    name: name.to_string(),
                    version: Some(version.to_string()),
                    source: err,
                })?
            }
        };

        Ok(Some(dataset))
    }

    pub async fn get_all_datasets(&self) -> Result<Vec<Dataset>, GetAllDatasetsError> {
        let list = metadata_db::datasets::list_all(&self.metadata_db)
            .await
            .map_err(GetAllDatasetsError::ListDatasetsFromDb)?;

        let mut datasets = Vec::new();
        for dataset_info in list {
            let namespace: Namespace = dataset_info.namespace.into();
            let name: Name = dataset_info.name.into();
            let version: Version = dataset_info.version.into();

            match self.get_dataset(&name, &version).await.map_err(|err| {
                GetAllDatasetsError::LoadDataset {
                    namespace: namespace.to_string(),
                    name: name.to_string(),
                    version: version.to_string(),
                    source: err,
                }
            })? {
                Some(dataset) => datasets.push(dataset),
                None => {
                    tracing::warn!(
                        dataset_namespace = %namespace,
                        dataset_name = %name,
                        dataset_version = %version,
                        "Dataset listed in metadata DB but not found when loading"
                    );
                }
            }
        }

        Ok(datasets)
    }

    #[tracing::instrument(skip(self), err)]
    pub async fn get_derived_manifest(
        &self,
        name: &str,
        version: impl Into<Option<&Version>> + std::fmt::Debug,
    ) -> Result<Option<DerivedManifest>, GetDerivedManifestError> {
        let namespace = &"_"
            .parse::<Namespace>()
            .expect("'_' should be a valid namespace");

        // Validate dataset name
        let name =
            &name
                .parse::<Name>()
                .map_err(|err| GetDerivedManifestError::InvalidDatasetName {
                    name: name.to_string(),
                    source: err,
                })?;

        // If no version is provided use a placeholder version (default version, i.e., v0.0.0).
        let version = &version.into().cloned().unwrap_or_default();

        // Get manifest hash from metadata database
        let Some(dataset_details) =
            metadata_db::datasets::get_version_tag(&self.metadata_db, namespace, name, version)
                .await
                .map_err(|source| GetDerivedManifestError::ManifestRetrievalError {
                    name: name.to_string(),
                    version: Some(version.to_string()),
                    source: Box::new(source),
                })?
        else {
            return Ok(None);
        };

        // Load the manifest content using the hash
        let manifest_hash: Hash = dataset_details.hash.into();
        let Some(dataset_src) = self
            .dataset_manifests_store
            .get(&manifest_hash)
            .await
            .map_err(|err| GetDerivedManifestError::ManifestRetrievalError {
                name: name.to_string(),
                version: Some(version.to_string()),
                source: Box::new(err),
            })?
        else {
            return Ok(None);
        };

        let manifest = dataset_src
            .try_into_manifest::<CommonManifest>()
            .map_err(|err| GetDerivedManifestError::ManifestParseError {
                name: name.to_string(),
                version: Some(version.to_string()),
                source: err,
            })?;

        tracing::debug!(
            dataset_namespace = %namespace,
            dataset_name = %name,
            dataset_version = %version,
            "Loaded manifest: {manifest:?}"
        );

        let kind = DatasetKind::from_str(&manifest.kind).map_err(|err: UnsupportedKindError| {
            GetDerivedManifestError::UnsupportedKind {
                name: name.to_string(),
                version: Some(version.to_string()),
                kind: err.kind,
            }
        })?;

        if kind != DatasetKind::Derived {
            return Err(GetDerivedManifestError::UnsupportedKind {
                name: name.to_string(),
                version: Some(version.to_string()),
                kind: kind.to_string(),
            });
        }

        let manifest = dataset_src
            .try_into_manifest::<DerivedManifest>()
            .map_err(|err| GetDerivedManifestError::ManifestParseError {
                name: name.to_string(),
                version: Some(version.to_string()),
                source: err,
            })?;

        Ok(Some(manifest))
    }

    #[tracing::instrument(skip(self), err)]
    pub async fn get_client(
        &self,
        dataset_name: &str,
        dataset_version: impl Into<Option<&Version>> + std::fmt::Debug,
        meter: Option<&monitoring::telemetry::metrics::Meter>,
    ) -> Result<Option<impl BlockStreamer>, GetClientError> {
        let dataset_name =
            &dataset_name
                .parse::<Name>()
                .map_err(|err| GetClientError::InvalidDatasetName {
                    name: dataset_name.to_string(),
                    source: err,
                })?;
        let dataset_version = dataset_version.into();

        // Get manifest hash from metadata database
        let namespace = "_"
            .parse::<Namespace>()
            .expect("'_' should be a valid namespace");

        let dataset_details = match dataset_version {
            Some(version) => metadata_db::datasets::get_version_tag(
                &self.metadata_db,
                &namespace,
                dataset_name,
                version,
            )
            .await
            .map_err(|source| GetClientError::ManifestRetrievalError {
                name: dataset_name.to_string(),
                version: Some(version.to_string()),
                source: Box::new(source),
            })?,
            None => {
                metadata_db::datasets::get_latest_tag(&self.metadata_db, &namespace, dataset_name)
                    .await
                    .map_err(|source| GetClientError::ManifestRetrievalError {
                        name: dataset_name.to_string(),
                        version: None,
                        source: Box::new(source),
                    })?
            }
        };

        let Some(dataset_details) = dataset_details else {
            return Ok(None);
        };

        // Load the manifest content using the hash
        let manifest_hash: Hash = dataset_details.hash.into();
        let Some(manifest_content) = self
            .dataset_manifests_store
            .get(&manifest_hash)
            .await
            .map_err(|err| GetClientError::ManifestRetrievalError {
                name: dataset_name.to_string(),
                version: dataset_version.map(|v| v.to_string()),
                source: Box::new(err),
            })?
        else {
            return Ok(None);
        };

        let manifest = manifest_content
            .try_into_manifest::<CommonManifest>()
            .map_err(|err| GetClientError::CommonManifestParseError {
                name: dataset_name.to_string(),
                version: dataset_version.map(|v| v.to_string()),
                source: err,
            })?;

        tracing::debug!(
            dataset_name = %dataset_name,
            dataset_version = %dataset_version.map(|v| v.to_string()).as_deref().unwrap_or("latest"),
            "Loaded manifest: {manifest:?}"
        );

        let kind: DatasetKind = manifest.kind.parse().map_err(|err: UnsupportedKindError| {
            GetClientError::UnsupportedKind {
                name: dataset_name.to_string(),
                version: dataset_version.map(|v| v.to_string()),
                kind: err.kind,
            }
        })?;
        if !kind.is_raw() {
            return Err(GetClientError::UnsupportedKind {
                name: dataset_name.to_string(),
                version: dataset_version.map(|v| v.to_string()),
                kind: manifest.kind,
            });
        }

        let Some(network) = manifest.network else {
            tracing::warn!(
                dataset_name = %dataset_name,
                dataset_version = ?dataset_version,
                dataset_kind = %kind,
                "dataset is missing required 'network' field for raw dataset kind"
            );
            return Err(GetClientError::MissingNetwork {
                name: dataset_name.to_string(),
                version: dataset_version.map(|v| v.to_string()),
            });
        };

        let Some(config) = self.find_provider(kind, network.clone()).await else {
            tracing::warn!(
                provider_kind = %manifest.kind,
                provider_network = %network,
                "no providers available for the requested kind-network configuration"
            );

            return Err(GetClientError::ProviderNotFound {
                name: dataset_name.to_string(),
                dataset_kind: kind,
                network,
            });
        };

        let client = match kind {
            DatasetKind::EvmRpc => {
                let config = config
                    .try_into_config::<EvmRpcProviderConfig>()
                    .map_err(|err| GetClientError::ProviderConfigParseError {
                        name: dataset_name.to_string(),
                        source: err,
                    })?;
                evm_rpc_datasets::client(config, meter)
                    .await
                    .map(BlockStreamClient::EvmRpc)
                    .map_err(|err| GetClientError::EvmRpcClientError {
                        name: dataset_name.to_string(),
                        source: err,
                    })?
            }
            DatasetKind::EthBeacon => {
                let config = config
                    .try_into_config::<EthBeaconProviderConfig>()
                    .map_err(|err| GetClientError::ProviderConfigParseError {
                        name: dataset_name.to_string(),
                        source: err,
                    })?;
                BlockStreamClient::EthBeacon(eth_beacon_datasets::client(config))
            }
            DatasetKind::Firehose => {
                let config = config
                    .try_into_config::<FirehoseProviderConfig>()
                    .map_err(|err| GetClientError::ProviderConfigParseError {
                        name: dataset_name.to_string(),
                        source: err,
                    })?;
                firehose_datasets::Client::new(config, meter)
                    .await
                    .map(|c| BlockStreamClient::Firehose(Box::new(c)))
                    .map_err(|err| GetClientError::FirehoseClientError {
                        name: dataset_name.to_string(),
                        source: err,
                    })?
            }
            _ => {
                unreachable!("non-raw dataset kinds are filtered out earlier");
            }
        };

        Ok(Some(client.with_retry()))
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
        for (dataset_name, dataset_version) in datasets {
            let Some(dataset) = self
                .get_dataset(&dataset_name, &dataset_version)
                .await
                .map_err(GetLogicalCatalogError::GetDataset)?
            else {
                return Err(GetLogicalCatalogError::DatasetNotFound {
                    name: dataset_name.to_string(),
                    version: dataset_version.clone(),
                });
            };

            if dataset.kind == EvmRpcDatasetKind {
                let udf = self.eth_call_for_dataset(&dataset).await.map_err(|err| {
                    GetLogicalCatalogError::EthCallUdfCreation {
                        dataset: dataset_name.to_string(),
                        source: err,
                    }
                })?;
                if let Some(udf) = udf {
                    udfs.push(udf);
                }
            }

            // Add JS UDFs
            for udf in dataset.functions(isolate_pool.clone()) {
                udfs.push(udf.into());
            }

            for mut table in Arc::new(dataset).resolved_tables() {
                // Only include tables that are actually referenced in the query
                let is_referenced = table_refs.iter().any(|table_ref| {
                    match (table_ref.schema(), table_ref.table()) {
                        (Some(schema), table_name) => {
                            let versioned_name = match &dataset_version {
                                None => dataset_name.to_string(),
                                Some(v) => {
                                    format!("{}__{}", dataset_name, v.to_underscore_version())
                                }
                            };

                            schema == versioned_name && table_name == table.name()
                        }
                        _ => false, // Unqualified table
                    }
                });

                if is_referenced {
                    let versioned_name = match &dataset_version {
                        None => dataset_name.to_string(),
                        Some(v) => {
                            format!("{}__{}", dataset_name, v.to_underscore_version())
                        }
                    };

                    let table_ref = TableReference::partial(versioned_name, table.name());
                    table.update_table_ref(table_ref);
                    resolved_tables.push(table);
                }
            }
        }

        Ok(LogicalCatalog {
            tables: resolved_tables,
            udfs,
        })
    }

    /// Returns cached eth_call scalar UDF, otherwise loads the UDF and caches it.
    async fn eth_call_for_dataset(
        &self,
        dataset: &Dataset,
    ) -> Result<Option<ScalarUDF>, EthCallForDatasetError> {
        let name = &dataset.name;
        let version = dataset.version.clone().unwrap_or_default();

        if dataset.kind != EvmRpcDatasetKind {
            return Ok(None);
        }

        // Check if we already have the provider cached.
        let cache_key = format!("{}__{}", name, version.to_underscore_version());
        if let Some(udf) = self.eth_call_cache.read().get(&cache_key) {
            return Ok(Some(udf.clone()));
        }

        // Load the provider from the dataset definition.
        let Some(network) = &dataset.network else {
            tracing::warn!(
                dataset_name = %name,
                dataset_version = %version,
                "dataset is missing required 'network' field for evm-rpc kind"
            );
            return Err(EthCallForDatasetError::MissingNetwork {
                dataset_name: name.clone(),
                dataset_version: version.clone(),
            });
        };

        let Some(config) = self
            .find_provider(DatasetKind::EvmRpc, network.clone())
            .await
        else {
            tracing::warn!(
                dataset_name = %name,
                dataset_version = %version,
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
            AsyncScalarUDF::new(Arc::new(EthCall::new(&dataset.name, provider))).into_scalar_udf();

        // Cache the EthCall UDF
        self.eth_call_cache.write().insert(cache_key, udf.clone());

        Ok(Some(udf))
    }
}

/// Extracts dataset names and versions from table references in SQL queries.
///
/// This function processes table references from SQL queries and extracts the dataset names
/// and optional versions from the schema portion of qualified table names.
///
/// # Table Reference Format
/// - All tables must be qualified with a dataset name: `dataset_name.table_name`
/// - Versioned datasets: `dataset_name__x_y_z.table_name` where x, y, z are version numbers
/// - Catalog-qualified tables are not supported
///
/// # Returns
/// A set of unique (dataset_name, version) tuples extracted from the table references.
fn dataset_versions_from_table_refs<'a>(
    table_refs: impl Iterator<Item = &'a TableReference>,
) -> Result<BTreeSet<(Name, Option<Version>)>, ExtractDatasetFromTableRefsError> {
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

        // Parse and extract dataset name and version if present.
        let (name_str, version_str) = catalog_schema
            .rsplit_once("__")
            .map(|(name_str, version_str)| (name_str, Some(version_str)))
            .unwrap_or((catalog_schema, None));

        let name = name_str.parse::<Name>().map_err(|err| {
            ExtractDatasetFromTableRefsError::InvalidDatasetName {
                name: name_str.to_string(),
                schema: catalog_schema.to_string(),
                source: err,
            }
        })?;

        let version = version_str
            .map(|v| v.replace("_", "."))
            .map(|v| {
                v.parse::<Version>()
                    .map_err(|_| ExtractDatasetFromTableRefsError::InvalidVersion {
                        version: v,
                        schema: catalog_schema.to_string(),
                    })
            })
            .transpose()?;

        datasets.insert((name, version));
    }

    Ok(datasets)
}

/// Extracts dataset names and versions from function names in SQL queries.
///
/// This function processes qualified function names (e.g., `dataset.function` or `dataset__1_0_0.function`)
/// and extracts the dataset name and optional version from the qualifier.
///
/// # Function Name Format
/// - Simple function names (no qualifier) are assumed to be built-in DataFusion functions
/// - Qualified functions: `dataset_name.function_name` or `dataset_name__x_y_z.function_name`
/// - Version format in qualifier: `__x_y_z` where x, y, z are version numbers
///
/// # Returns
/// A set of unique (dataset_name, version) tuples extracted from the function names.
fn dataset_versions_from_function_names<'a>(
    function_names: impl IntoIterator<Item = &'a str>,
) -> Result<BTreeSet<(Name, Option<Version>)>, ExtractDatasetFromFunctionNamesError> {
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

        // Parse and extract dataset name and version if present.
        let (name_str, version_str) = fn_dataset
            .rsplit_once("__")
            .map(|(name_str, version_str)| (name_str, Some(version_str)))
            .unwrap_or((fn_dataset, None));

        let name = name_str.parse::<Name>().map_err(|err| {
            ExtractDatasetFromFunctionNamesError::InvalidDatasetName {
                name: name_str.to_string(),
                function: fn_dataset.to_string(),
                source: err,
            }
        })?;

        let version = version_str
            .map(|v| v.replace("_", "."))
            .map(|v| {
                v.parse::<Version>().map_err(|_| {
                    ExtractDatasetFromFunctionNamesError::InvalidVersion {
                        version: v,
                        function: fn_dataset.to_string(),
                    }
                })
            })
            .transpose()?;

        datasets.insert((name, version));
    }

    Ok(datasets)
}

/// Return a table identifier, in the form `{dataset}.blocks`, for the given network.
#[tracing::instrument(skip(dataset_store), err)]
pub async fn resolve_blocks_table(
    dataset_store: &Arc<DatasetStore>,
    src_datasets: &BTreeSet<&str>,
    network: &str,
) -> Result<PhysicalTable, BoxError> {
    let dataset = {
        let mut datasets: Vec<Dataset> = dataset_store
            .get_all_datasets()
            .await?
            .into_iter()
            .filter(|d| d.kind != DerivedDatasetKind)
            .filter(|d| {
                let dataset_network = d
                    .network
                    .as_ref()
                    .expect("network should be set for raw datasets");
                dataset_network == network
            })
            .collect();

        if datasets.is_empty() {
            return Err(format!("no provider found for network {network}").into());
        }

        match datasets
            .iter()
            .position(|d| src_datasets.contains(d.name.as_str()))
        {
            Some(index) => datasets.remove(index),
            None => {
                // Make sure fallback provider selection is deterministic.
                datasets.sort_by(|a, b| a.name.cmp(&b.name));
                if datasets.len() > 1 {
                    tracing::debug!(
                        "selecting provider {} for network {}",
                        datasets[0].name,
                        network
                    );
                }
                datasets.remove(0)
            }
        }
    };

    let dataset_name = dataset.name.clone();
    let table = Arc::new(dataset)
        .resolved_tables()
        .find(|t| t.name() == "blocks")
        .ok_or_else(|| {
            BoxError::from(format!(
                "dataset '{}' does not have a 'blocks' table",
                dataset_name
            ))
        })?;

    PhysicalTable::get_active(&table, dataset_store.metadata_db.clone())
        .await?
        .ok_or_else(|| {
            BoxError::from(format!(
                "table '{}.{}' has not been synced",
                dataset_name,
                table.name()
            ))
        })
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
        let Some(dataset) = store
            .get_dataset(dataset_ref.name(), dataset_ref.revision().as_version())
            .await?
        else {
            return Err(format!("Dataset '{}' not found", dataset_ref).into());
        };

        if dataset.kind != DerivedDatasetKind {
            deps.insert(dataset_ref, vec![]);
            continue;
        }

        let manifest = store
            .get_derived_manifest(&dataset.name, dataset.version.as_ref())
            .await?
            .ok_or_else(|| format!("Derived dataset '{}' not found", dataset.name))?;

        let refs: Vec<Reference> = manifest.dependencies.into_values().collect();
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
