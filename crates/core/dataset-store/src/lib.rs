use std::{collections::BTreeSet, num::NonZeroU32, str::FromStr, sync::Arc};

use common::{
    BlockStreamer, BlockStreamerExt, BoxError, Dataset, LogicalCatalog, PlanningContext,
    catalog::physical::{Catalog, PhysicalTable},
    evm::{self, udfs::EthCall},
    manifest::{common::schema_from_tables, derived},
    query_context::QueryEnv,
    sql_visitors::all_function_names,
};
use datafusion::{
    common::HashMap,
    logical_expr::{ScalarUDF, async_udf::AsyncScalarUDF},
    sql::{TableReference, parser, resolve::resolve_table_references},
};
use datasets_common::{manifest::Manifest as CommonManifest, name::Name, version::Version};
use datasets_derived::Manifest as DerivedManifest;
use eth_beacon_datasets::{
    Manifest as EthBeaconManifest, ProviderConfig as EthBeaconProviderConfig,
};
use evm_rpc_datasets::{
    DATASET_KIND as EVM_RPC_DATASET_KIND, Manifest as EvmRpcManifest,
    ProviderConfig as EvmRpcProviderConfig,
};
use firehose_datasets::{
    DATASET_KIND as FIREHOSE_DATASET_KIND,
    dataset::{Manifest as FirehoseManifest, ProviderConfig as FirehoseProviderConfig},
};
use js_runtime::isolate_pool::IsolatePool;
use metadata_db::MetadataDb;
use parking_lot::RwLock;
use rand::seq::SliceRandom as _;
use substreams_datasets::dataset::{
    Manifest as SubstreamsManifest, ProviderConfig as SubstreamsProviderConfig,
};
use url::Url;

mod block_stream_client;
mod dataset_kind;
mod error;
pub mod manifests;
pub mod providers;

use self::{
    block_stream_client::BlockStreamClient,
    manifests::{DatasetManifestsStore, GetLatestVersionError},
    providers::{ProviderConfig, ProviderConfigsStore},
};
pub use self::{
    dataset_kind::{DatasetKind, UnsupportedKindError},
    error::{
        CatalogForSqlError, EthCallForDatasetError, ExtractDatasetFromFunctionNamesError,
        ExtractDatasetFromTableRefsError, GetAllDatasetsError, GetClientError, GetDatasetError,
        GetDerivedManifestError, GetLogicalCatalogError, GetPhysicalCatalogError,
        IsRegisteredError, PlanningCtxForSqlError, RegisterManifestError,
    },
    manifests::StoreError,
};

const PLACEHOLDER_OWNER: &'static str = "no-owner";

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

    /// Initialize the dataset store by loading existing manifests into the metadata database
    ///
    /// This method ensures that all manifests present in the object store are registered in the
    /// metadata database. It's idempotent and will only register manifests that don't already exist.
    /// The initialization runs only once per instance.
    pub async fn init(&self) {
        self.dataset_manifests_store.init().await
    }

    /// Get a reference to the providers configuration store
    pub fn providers(&self) -> &ProviderConfigsStore {
        &self.provider_configs_store
    }

    /// Register a dataset manifest in both the dataset store and metadata database.
    ///
    /// The operation is atomic - either both the store and metadata database are updated
    /// or neither is updated.
    pub async fn register_manifest<M>(
        &self,
        name: &Name,
        version: &Version,
        manifest: &M,
    ) -> Result<(), RegisterManifestError>
    where
        M: serde::Serialize,
    {
        // Check for existing datasets with the same name and version
        if self.is_registered(name, version).await? {
            return Err(RegisterManifestError::DatasetExists {
                name: name.clone(),
                version: version.clone(),
            });
        }

        // Store manifest in the underlying store first
        let manifest_path = self
            .dataset_manifests_store
            .store(name, version, manifest)
            .await?;

        // Register dataset metadata in database
        // TODO: Extract the dataset owner from the manifest
        let owner = PLACEHOLDER_OWNER;

        self.metadata_db
            .register_dataset(owner, name, version, &manifest_path.to_string())
            .await
            .map_err(RegisterManifestError::MetadataRegistration)?;

        Ok(())
    }

    /// Check if a dataset with the given name and version is registered
    ///
    /// Returns true if the dataset is registered, false otherwise.
    /// This operation queries the metadata database directly.
    pub async fn is_registered(
        &self,
        name: &Name,
        version: &Version,
    ) -> Result<bool, IsRegisteredError> {
        self.metadata_db
            .dataset_exists(name, version)
            .await
            .map_err(IsRegisteredError)
    }

    pub async fn get_dataset(
        self: &Arc<Self>,
        name: &str,
        version: impl Into<Option<&Version>>,
    ) -> Result<Option<Dataset>, GetDatasetError> {
        let name = &name
            .parse::<Name>()
            .map_err(|err| GetDatasetError::InvalidDatasetName {
                name: name.to_string(),
                source: err,
            })?;

        // If no version is provided, try to get the latest version from the mainfests store
        // or use a placeholder version (default version, i.e., v0.0.0) if not found.
        let version = match version.into() {
            Some(v) => v.clone(),
            None => self
                .dataset_manifests_store
                .get_latest_version(name)
                .await
                .map_err(
                    |GetLatestVersionError(source)| GetDatasetError::GetLatestVersion {
                        name: name.to_string(),
                        source,
                    },
                )?
                .unwrap_or_default(),
        };

        let cache_key = format!("{}__{}", name, version.to_underscore_version());
        if let Some(dataset) = self.dataset_cache.read().get(&cache_key) {
            tracing::trace!(
                dataset_name = %name,
                dataset_version = %version,
                "Cache hit, returning cached dataset"
            );

            return Ok(Some(dataset.clone()));
        }

        tracing::debug!(
            dataset_name = %name,
            dataset_version = %version,
            "Cache miss, loading from store"
        );

        // Try to load the dataset manifest using ManifestsStore
        let Some(manifest_content) = self
            .dataset_manifests_store
            .get(name, &version)
            .await
            .map_err(|err| GetDatasetError::ManifestRetrievalError {
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
                name: name.to_string(),
                version: Some(version.to_string()),
                source: err,
            })?;

        tracing::debug!(
            dataset_name = %name,
            dataset_version = %version,
            dataset_kind = %manifest.kind,
            "Loaded dataset manifest: {manifest:?}"
        );

        let kind: DatasetKind = manifest.kind.parse().map_err(|err: UnsupportedKindError| {
            GetDatasetError::UnsupportedKind {
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
                        name: name.to_string(),
                        version: Some(version.to_string()),
                        source: err,
                    })?;
                firehose_datasets::evm::dataset(manifest)
            }
            DatasetKind::Substreams => {
                let value = manifest_content
                    .try_into_manifest::<SubstreamsManifest>()
                    .map_err(|err| GetDatasetError::ManifestParseError {
                        name: name.to_string(),
                        version: Some(version.to_string()),
                        source: err,
                    })?;
                let dataset = substreams_datasets::dataset(value).await.map_err(|err| {
                    GetDatasetError::SubstreamsCreationError {
                        name: name.to_string(),
                        version: Some(version.to_string()),
                        source: err,
                    }
                })?;
                let builtin_schema = schema_from_tables(&dataset.tables);

                if let Some(manifest_schema) = &manifest.schema
                    && manifest_schema != &builtin_schema
                {
                    return Err(GetDatasetError::SchemaMismatch {
                        name: name.to_string(),
                        version: Some(version.to_string()),
                    });
                }

                dataset
            }
            DatasetKind::Derived => {
                let manifest = manifest_content
                    .try_into_manifest::<DerivedManifest>()
                    .map_err(|err| GetDatasetError::ManifestParseError {
                        name: name.to_string(),
                        version: Some(version.to_string()),
                        source: err,
                    })?;
                let dataset = derived::dataset(manifest).map_err(|err| {
                    GetDatasetError::DerivedCreationError {
                        name: name.to_string(),
                        version: Some(version.to_string()),
                        source: err.into(),
                    }
                })?;
                dataset
            }
        };

        // Cache the dataset.
        self.dataset_cache
            .write()
            .insert(cache_key.to_string(), dataset.clone());

        tracing::debug!(
            dataset_name = %name,
            dataset_version = %version,
            dataset_kind = %kind,
            "Dataset loaded (and cached) successfully"
        );

        Ok(Some(dataset))
    }

    pub async fn get_all_datasets(self: &Arc<Self>) -> Result<Vec<Dataset>, GetAllDatasetsError> {
        let list = self.dataset_manifests_store.list().await;

        let mut datasets = Vec::new();
        for (name, version) in list {
            match self.get_dataset(&name, &version).await? {
                Some(dataset) => datasets.push(dataset),
                None => {
                    tracing::warn!(
                        dataset_name = %name,
                        dataset_version = %version,
                        "Dataset manifest listed but not found when loading"
                    );
                }
            }
        }

        Ok(datasets)
    }

    #[tracing::instrument(skip(self), err)]
    pub async fn get_derived_manifest(
        self: &Arc<Self>,
        name: &str,
        version: impl Into<Option<&Version>> + std::fmt::Debug,
    ) -> Result<Option<DerivedManifest>, GetDerivedManifestError> {
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

        // Try to load the dataset manifest using ManifestsStore
        let Some(dataset_src) = self
            .dataset_manifests_store
            .get(name, version)
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
        only_finalized_blocks: bool,
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

        // Get the dataset manifest for the given name and version
        let Some(manifest_content) = self
            .dataset_manifests_store
            .get(dataset_name, dataset_version)
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

        let Some(config) = self.find_provider(kind, manifest.network.clone()).await else {
            tracing::warn!(
                provider_kind = %manifest.kind,
                provider_network = %manifest.network,
                "no providers available for the requested kind-network configuration"
            );

            return Err(GetClientError::ProviderNotFound {
                name: dataset_name.to_string(),
                dataset_kind: kind,
                network: manifest.network,
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
                evm_rpc_datasets::client(config, only_finalized_blocks, meter)
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
                BlockStreamClient::EthBeacon(eth_beacon_datasets::client(
                    config,
                    only_finalized_blocks,
                ))
            }
            DatasetKind::Firehose => {
                let config = config
                    .try_into_config::<FirehoseProviderConfig>()
                    .map_err(|err| GetClientError::ProviderConfigParseError {
                        name: dataset_name.to_string(),
                        source: err,
                    })?;
                firehose_datasets::Client::new(config, only_finalized_blocks, meter)
                    .await
                    .map(BlockStreamClient::Firehose)
                    .map_err(|err| GetClientError::FirehoseClientError {
                        name: dataset_name.to_string(),
                        source: err,
                    })?
            }
            DatasetKind::Substreams => {
                let config = config
                    .try_into_config::<SubstreamsProviderConfig>()
                    .map_err(|err| GetClientError::ProviderConfigParseError {
                        name: dataset_name.to_string(),
                        source: err,
                    })?;
                let manifest = manifest_content
                    .try_into_manifest::<SubstreamsManifest>()
                    .map_err(|err| GetClientError::SubstreamsManifestParseError {
                        name: dataset_name.to_string(),
                        version: dataset_version.map(|v| v.to_string()),
                        source: err,
                    })?;
                substreams_datasets::Client::new(config, manifest, only_finalized_blocks, meter)
                    .await
                    .map(BlockStreamClient::Substreams)
                    .map_err(|err| GetClientError::SubstreamsClientError {
                        name: dataset_name.to_string(),
                        source: err.into(),
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
                if let Err(err) = common::env_substitute::substitute_env_vars(value) {
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
        self: &Arc<Self>,
        query: &parser::Statement,
        env: QueryEnv,
    ) -> Result<Catalog, CatalogForSqlError> {
        let (tables, _) = resolve_table_references(query, true)
            .map_err(|err| CatalogForSqlError::TableReferenceResolution { source: err.into() })?;
        let function_names = all_function_names(query)
            .map_err(|err| CatalogForSqlError::FunctionNameExtraction { source: err.into() })?;

        self.get_physical_catalog(tables, function_names, &env)
            .await
            .map_err(CatalogForSqlError::GetPhysicalCatalog)
    }

    /// Looks up the datasets for the given table references and gets them into a catalog.
    pub async fn get_physical_catalog(
        self: &Arc<Self>,
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
                    source: err.into(),
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
        self: Arc<Self>,
        query: &parser::Statement,
    ) -> Result<PlanningContext, PlanningCtxForSqlError> {
        let (tables, _) = resolve_table_references(query, true).map_err(|err| {
            PlanningCtxForSqlError::TableReferenceResolution { source: err.into() }
        })?;
        let function_names = all_function_names(query)
            .map_err(|err| PlanningCtxForSqlError::FunctionNameExtraction { source: err.into() })?;
        let resolved_tables = self
            .get_logical_catalog(tables, function_names, &IsolatePool::dummy())
            .await?;
        Ok(PlanningContext::new(resolved_tables))
    }

    /// Looks up the datasets for the given table references and creates resolved tables. Create
    /// UDFs specific to the referenced datasets.
    async fn get_logical_catalog(
        self: &Arc<Self>,
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

            if dataset.kind == EVM_RPC_DATASET_KIND {
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

        if dataset.kind != EVM_RPC_DATASET_KIND {
            return Ok(None);
        }

        // Check if we already have the provider cached.
        let cache_key = format!("{}__{}", name, version.to_underscore_version());
        if let Some(udf) = self.eth_call_cache.read().get(&cache_key) {
            return Ok(Some(udf.clone()));
        }

        // Load the provider from the dataset definition.
        let Some(config) = self
            .find_provider(DatasetKind::EvmRpc, dataset.network.clone())
            .await
        else {
            tracing::warn!(
                dataset_name = %name,
                dataset_version = %version,
                provider_kind = %DatasetKind::EvmRpc,
                provider_network = %dataset.network,
                "no providers available for the requested kind-network configuration"
            );
            return Err(EthCallForDatasetError::ProviderNotFound {
                dataset_kind: DatasetKind::EvmRpc,
                network: dataset.network.clone(),
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
                .map_err(|err| EthCallForDatasetError::IpcConnection(err.into()))?
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
        fn is_raw_dataset(kind: &str) -> bool {
            [EVM_RPC_DATASET_KIND, FIREHOSE_DATASET_KIND].contains(&kind)
        }

        let mut datasets: Vec<Dataset> = dataset_store
            .get_all_datasets()
            .await?
            .into_iter()
            .filter(|d| d.network == network)
            .filter(|d| is_raw_dataset(d.kind.as_str()))
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
