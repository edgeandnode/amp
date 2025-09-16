use std::{
    collections::BTreeSet,
    num::NonZeroU32,
    str::FromStr,
    sync::{Arc, RwLock},
};

use common::{
    BlockNum, BlockStreamer, BlockStreamerExt, BoxError, Dataset, LogicalCatalog, PlanningContext,
    RawDatasetRows, Store,
    catalog::physical::{Catalog, PhysicalTable},
    config::Config,
    evm::{self, udfs::EthCall},
    manifest::{
        self,
        common::{schema_from_table_slice, schema_from_tables},
        derived::Manifest,
        sql_datasets::SqlDataset,
    },
    query_context::QueryEnv,
    sql_visitors::all_function_names,
};
use datafusion::{
    common::HashMap,
    logical_expr::{ScalarUDF, async_udf::AsyncScalarUDF},
    sql::{TableReference, parser, resolve::resolve_table_references},
};
use datasets_common::{manifest::Manifest as CommonManifest, name::Name, version::Version};
use futures::{FutureExt as _, Stream, TryFutureExt as _, future::BoxFuture};
use js_runtime::isolate_pool::IsolatePool;
use metadata_db::MetadataDb;
use object_store::ObjectStore;
use rand::seq::SliceRandom;
use tracing::instrument;
use url::Url;

mod dataset_kind;
mod error;
pub mod providers;
pub mod sql_datasets;

use self::providers::ProvidersConfigStore;
pub use self::{
    dataset_kind::{DatasetKind, UnsupportedKindError},
    error::{DatasetError, Error, RegistrationError},
};
/// Alias for TOML value type used in provider configurations.
// TODO: #[deprecated(note = "use dataset_store::providers::ProviderConfig instead")]
pub type ProviderConfigTomlValue = toml::Value;

#[derive(Clone)]
pub struct DatasetStore {
    metadata_db: Arc<MetadataDb>,
    // Cache maps dataset name to eth_call UDF.
    eth_call_cache: Arc<RwLock<HashMap<String, ScalarUDF>>>,
    // This cache maps dataset name to the dataset definition.
    dataset_cache: Arc<RwLock<HashMap<String, Dataset>>>,
    // Provider store for managing provider configurations and caching.
    providers: ProvidersConfigStore,
    // Store for dataset definitions (manifests).
    pub(crate) dataset_defs_store: Arc<Store>,
}

impl DatasetStore {
    pub fn new(config: Arc<Config>, metadata_db: Arc<MetadataDb>) -> Arc<Self> {
        let providers_store = ProvidersConfigStore::new(config.providers_store.prefixed_store());
        Arc::new(Self {
            metadata_db,
            eth_call_cache: Default::default(),
            dataset_cache: Default::default(),
            // manifests: manifests_store,
            providers: providers_store,
            dataset_defs_store: config.dataset_defs_store.clone(),
        })
    }

    /// Get a reference to the providers configuration store
    pub fn providers(&self) -> &ProvidersConfigStore {
        &self.providers
    }

    pub async fn migrate_stored_files(&self) -> Result<(), DatasetError> {
        // Migrate any datasets in the store without version suffix
        let all_objs = &self
            .dataset_defs_store
            .list_all_shallow()
            .await
            .map_err(DatasetError::no_context)?;

        // Filter the datasets which have a version suffix
        let all_objs = all_objs
            .into_iter()
            .filter_map(|obj| obj.location.filename())
            .filter(|filename| {
                filename
                    .trim_end_matches(".json")
                    .split_once("__")
                    .is_none()
            })
            .collect::<Vec<_>>();

        for filename in all_objs {
            // Load and parse the manifest to ensure it's valid
            let manifest_str = self
                .dataset_defs_store
                .get_string(filename)
                .await
                .map_err(DatasetError::no_context)?;

            let original_path = object_store::path::Path::from(filename);
            let new_path = object_store::path::Path::from(format!(
                "{}__0_0_0.json",
                filename.trim_end_matches(".json")
            ));

            // Write it back with version suffix `0.0.0`
            self.dataset_defs_store
                .prefixed_store()
                .put(&new_path, manifest_str.into())
                .await
                .map_err(DatasetError::unknown)?;

            // Delete the old file
            self.dataset_defs_store
                .prefixed_store()
                .delete(&original_path)
                .await
                .map_err(DatasetError::unknown)?;
        }

        Ok(())
    }

    pub async fn initialize(&self) -> Result<(), DatasetError> {
        // Register the datasets
        let all_objs = &self
            .dataset_defs_store
            .list_all_shallow()
            .await
            .map_err(DatasetError::no_context)?;

        let all_objs = all_objs
            .iter()
            .filter_map(|obj| obj.location.filename())
            .filter_map(|filename| filename.strip_suffix(".json"))
            .filter_map(|filename| filename.split_once("__"))
            .collect::<Vec<_>>();

        for (name, version) in all_objs {
            let Ok(name) = name.parse::<Name>() else {
                tracing::warn!("Skipping dataset with invalid name: {}", name);
                continue;
            };

            let Ok(version) = Version::from_str(&version.replace('_', ".")) else {
                tracing::warn!("Skipping dataset with invalid version: {}", version);
                continue;
            };

            if self
                .metadata_db
                .dataset_exists(&name, &version)
                .await
                .map_err(DatasetError::no_context)?
            {
                // Dataset already registered, skip
                continue;
            }

            let filename = format!("{}__{}.json", name, version.to_underscore_version());

            // Load and parse the manifest to ensure it's valid
            let manifest = self
                .dataset_defs_store
                .get_string(filename.as_str())
                .await
                .map_err(DatasetError::no_context)?;

            let Ok(manifest) = serde_json::from_str::<'_, serde_json::Value>(&manifest) else {
                tracing::error!("Failed to parse dataset manifest: '{}'", filename,);
                continue;
            };

            if let Err(err) = self.register_manifest(&name, &version, &manifest).await {
                tracing::error!(
                    "Failed to register dataset manifest '{}': {}",
                    filename,
                    err
                );
                continue;
            }
        }

        Ok(())
    }

    /// Register a manifest in the dataset store and metadata database
    ///
    /// This function validates and registers a dataset manifest by:
    /// 1. Checking if the dataset already exists with the given name and version
    /// 2. Extracting dataset owner from the manifest dependencies
    /// 3. Storing the manifest JSON in the dataset definitions store
    /// 4. Registering the dataset metadata in the database
    pub async fn register_manifest<M>(
        &self,
        name: &Name,
        version: &Version,
        manifest: &M,
    ) -> Result<(), RegistrationError>
    where
        M: serde::Serialize,
    {
        // Check if the dataset with the given name and version already exists in the registry.
        if self
            .metadata_db
            .dataset_exists(name, version)
            .await
            .map_err(RegistrationError::ExistenceCheck)?
        {
            return Err(RegistrationError::DatasetExists {
                name: name.clone(),
                version: version.clone(),
            });
        }

        // Prepare manifest data for storage
        let manifest_json =
            serde_json::to_string(&manifest).map_err(RegistrationError::ManifestSerialization)?;

        // Store manifest in dataset definitions store
        let manifest_path = object_store::path::Path::from(format!(
            "{}__{}.json",
            name,
            version.to_underscore_version()
        ));

        self.dataset_defs_store
            .prefixed_store()
            .put(&manifest_path, manifest_json.into())
            .await
            .map_err(RegistrationError::ManifestStorage)?;

        // TODO: Extract the dataset owner from the manifest
        let dataset_owner = "no-owner";

        // Register dataset metadata in database
        self.metadata_db
            .register_dataset(
                dataset_owner,
                name.as_str(),
                version,
                manifest_path.as_ref(),
            )
            .await
            .map_err(RegistrationError::MetadataRegistration)?;

        Ok(())
    }

    pub async fn try_load_dataset(
        self: &Arc<Self>,
        name: &str,
        version: impl Into<Option<&Version>>,
    ) -> Result<Option<Dataset>, DatasetError> {
        // Initialize the dataset store if not already done
        self.migrate_stored_files().await?;
        self.initialize().await?;

        let version = version.into();
        let Some(filename) = self
            .resolve_dataset_manifest_filename(name, version)
            .await
            .map_err(|err| DatasetError::from((name, err)))?
        else {
            return Ok(None);
        };

        // If we have it cached, return it
        if let Some(dataset) = self.dataset_cache.read().unwrap().get(&filename) {
            return Ok(Some(dataset.clone()));
        }

        let manifest_str = self
            .dataset_defs_store
            .get_string(format!("{}.json", filename))
            .await
            .map_err(|err| DatasetError::from((name, err.into())))?;

        let common = serde_json::from_str::<CommonManifest>(&manifest_str)
            .map_err(|err| DatasetError::from((name, err.into())))?;

        let kind = common
            .kind
            .parse::<DatasetKind>()
            .map_err(|err| DatasetError::from((name, err.into())))?;
        let value = serde_json::Value::from_str(&manifest_str)
            .map_err(|err| DatasetError::from((name, err.into())))?;
        let (dataset, ground_truth_schema) = match kind {
            DatasetKind::EvmRpc => {
                let builtin_schema =
                    schema_from_tables(evm_rpc_datasets::tables::all(&common.network));
                (
                    evm_rpc_datasets::dataset(value)
                        .map_err(|err| DatasetError::from((name, err.into())))?,
                    Some(builtin_schema),
                )
            }
            DatasetKind::Firehose => {
                let builtin_schema =
                    schema_from_tables(firehose_datasets::evm::tables::all(&common.network));
                (
                    firehose_datasets::evm::dataset(value).map_err(|err| (name, err.into()))?,
                    Some(builtin_schema),
                )
            }
            DatasetKind::Substreams => {
                let dataset = substreams_datasets::dataset(value)
                    .await
                    .map_err(|err| DatasetError::from((name, err.into())))?;
                let store_schema = schema_from_table_slice(dataset.tables.as_slice());
                (dataset, Some(store_schema))
            }
            DatasetKind::Sql => {
                let store_dataset = Arc::clone(self)
                    .sql_dataset(value)
                    .await
                    .map_err(|err| DatasetError::from((name, err.into())))?
                    .dataset;
                (store_dataset, None)
            }
            DatasetKind::Manifest => {
                let manifest = serde_json::from_str::<'_, Manifest>(&manifest_str)
                    .map_err(|err| (name, err.into()))?;
                let dataset = manifest::derived::dataset(manifest)
                    .map_err(Error::Unknown)
                    .map_err(|err| DatasetError::from((name, err)))?;
                (dataset, None)
            }
        };

        if let Some(ground_truth_schema) = ground_truth_schema {
            let Some(loaded_schema) = common.schema else {
                return Err(DatasetError::from((
                    name,
                    Error::SchemaMissing { dataset_kind: kind },
                )));
            };
            if loaded_schema != ground_truth_schema {
                return Err(DatasetError::from((name, Error::SchemaMismatch)));
            }
        }

        // Cache the dataset.
        self.dataset_cache
            .write()
            .unwrap()
            .insert(filename.to_string(), dataset.clone());

        Ok(Some(dataset))
    }

    pub async fn load_dataset(
        self: &Arc<Self>,
        name: &str,
        version: impl Into<Option<&Version>>,
    ) -> Result<Dataset, DatasetError> {
        let version = version.into();
        match self.try_load_dataset(name, version).await? {
            Some(dataset) => Ok(dataset),
            None => Err(DatasetError::from((
                name,
                Error::DatasetVersionNotFound(name.to_string(), version.map(|v| v.to_string())),
            ))),
        }
    }

    /// Resolve dataset manifest filename from registry metadata.
    ///
    /// This method determines the appropriate dataset manifest filename to use:
    /// - If a specific version is requested, looks up the exact version in the registry
    /// - If no version is specified, finds the latest version and constructs a versioned filename
    /// - Falls back to the base name if no registry entry exists
    async fn resolve_dataset_manifest_filename(
        &self,
        name: &str,
        version: impl Into<Option<&Version>>,
    ) -> Result<Option<String>, Error> {
        let filename = match version.into() {
            Some(version) => {
                self.metadata_db
                    .get_dataset_with_details(name, version)
                    .await
            }
            None => {
                self.metadata_db
                    .get_dataset_latest_version_with_details(name)
                    .await
            }
        }
        .map_err(|err| Error::MetadataDbError(err))?
        .map(|details| details.manifest_path.trim_end_matches(".json").to_string());

        Ok(filename)
    }

    // Only used in `get_all` admin-api handler and in the streaming query data path
    pub async fn all_datasets(self: &Arc<Self>) -> Result<Vec<Dataset>, DatasetError> {
        // Initialize the dataset store if not already done
        self.migrate_stored_files().await?;
        self.initialize().await?;

        let all_objs = self
            .dataset_defs_store
            .list_all_shallow()
            .await
            .map_err(DatasetError::no_context)?;

        let all_objs = all_objs
            .iter()
            .filter_map(|obj| obj.location.filename())
            .filter_map(|filename| filename.strip_suffix(".json"))
            .filter_map(|filename| filename.split_once("__"))
            .collect::<Vec<_>>();

        let mut datasets = Vec::new();
        for (name, version) in all_objs {
            let cache_key = format!("{}__{}", name, version);
            if let Some(dataset) = self.dataset_cache.read().unwrap().get(&cache_key) {
                datasets.push(dataset.clone());
                continue;
            }

            let Ok(name) = name.parse::<Name>() else {
                tracing::warn!("Skipping dataset with invalid name: {}", name);
                continue;
            };

            let Ok(version) = Version::from_str(&version.replace('_', ".")) else {
                tracing::warn!("Skipping dataset with invalid version: {}", version);
                continue;
            };
            datasets.push(self.load_dataset(&name, &version).await?);
        }
        Ok(datasets)
    }

    #[instrument(skip(self), err)]
    pub async fn load_sql_dataset(
        self: &Arc<Self>,
        name: &str,
    ) -> Result<SqlDataset, DatasetError> {
        // initialize the dataset store if not already done
        self.migrate_stored_files().await?;
        self.initialize().await?;

        // We migrated the dataset store to always have version suffixes, so all datasets
        // should have `__0_0_0.json` suffix if they are not versioned.
        let raw_dataset = self
            .dataset_defs_store
            .get_string(format!("{}__0_0_0.json", name))
            .await
            .map_err(DatasetError::no_context)?;

        let common = serde_json::from_str::<CommonManifest>(&raw_dataset)
            .map_err(DatasetError::no_context)?;

        let kind: DatasetKind = common.kind.parse().map_err(DatasetError::no_context)?;
        if kind != DatasetKind::Sql {
            return Err(DatasetError::no_context(Error::UnsupportedKind(
                kind.to_string(),
            )));
        }

        let value = serde_json::Value::from_str(&raw_dataset).map_err(DatasetError::no_context)?;
        self.sql_dataset(value)
            .await
            .map_err(DatasetError::no_context)
    }

    pub async fn load_derived_dataset(
        self: &Arc<Self>,
        name: &str,
        version: &Version,
    ) -> Result<SqlDataset, DatasetError> {
        // initialize the dataset store if not already done
        self.migrate_stored_files().await?;
        self.initialize().await?;

        let Some(filename) = self
            .resolve_dataset_manifest_filename(name, version)
            .await
            .map_err(|err| DatasetError::from((name, err)))?
        else {
            tracing::error!(
                "Dataset '{}' with version '{}' not found in registry",
                name,
                version
            );
            return Err(DatasetError::from((
                name.to_string(),
                Error::DatasetVersionNotFound(name.to_string(), Some(version.to_string())),
            )));
        };

        let manifest_str = self
            .dataset_defs_store
            .get_string(format!("{}.json", &filename))
            .await
            .map_err(|err| DatasetError::from((name, err.into())))?;
        let manifest: Manifest = serde_json::from_str(&manifest_str)
            .map_err(Error::ManifestError)
            .map_err(|err| DatasetError::from((name, err)))?;

        let queries = manifest
            .queries()
            .map_err(Error::SqlParseError)
            .map_err(|err| DatasetError::from((name, err)))?;

        let dataset = manifest::derived::dataset(manifest)
            .map_err(Error::Unknown)
            .map_err(|err| DatasetError::from((name, err)))?;

        Ok(SqlDataset { dataset, queries }).map_err(|err| (name, err).into())
    }

    #[instrument(skip(self), err)]
    pub async fn load_client(
        &self,
        dataset_name: &str,
        only_finalized_blocks: bool,
    ) -> Result<impl BlockStreamer, DatasetError> {
        // initialize the dataset store if not already done
        self.migrate_stored_files().await?;
        self.initialize().await?;

        // We migrated the dataset store to always have version suffixes, so all datasets
        // should have `__0_0_0.json` suffix if they are not versioned.
        let manifest_str = self
            .dataset_defs_store
            .get_string(format!("{}__0_0_0.json", dataset_name))
            .await
            .map_err(|err| DatasetError::from((dataset_name, err.into())))?;

        let common = serde_json::from_str::<CommonManifest>(&manifest_str)
            .map_err(|err| DatasetError::from((dataset_name, err.into())))?;
        let value = serde_json::Value::from_str(&manifest_str)
            .map_err(|err| DatasetError::from((dataset_name, err.into())))?;
        let kind = common.kind.parse().map_err(|_| {
            DatasetError::from((dataset_name, Error::UnsupportedKind(common.kind.clone())))
        })?;

        let Some((provider_name, provider)) =
            self.find_provider(kind, common.network.clone()).await
        else {
            tracing::warn!(
                provider_kind = %kind,
                provider_network = %common.network,
                "no providers available for the requested kind-network configuration"
            );
            return Err(DatasetError::from((
                dataset_name,
                Error::ProviderNotFound {
                    dataset_kind: kind,
                    network: common.network,
                },
            )));
        };
        match kind {
            DatasetKind::EvmRpc => evm_rpc_datasets::client(
                provider,
                common.network,
                provider_name,
                only_finalized_blocks,
            )
            .await
            .map(BlockStreamClient::EvmRpc)
            .map(|client| client.with_retry())
            .map_err(|err| DatasetError::from((dataset_name, err.into()))),
            DatasetKind::Firehose => firehose_datasets::Client::new(
                provider,
                common.network,
                provider_name,
                only_finalized_blocks,
            )
            .await
            .map(BlockStreamClient::Firehose)
            .map(|client| client.with_retry())
            .map_err(|err| DatasetError::from((dataset_name, err.into()))),

            DatasetKind::Substreams => substreams_datasets::Client::new(
                provider,
                value,
                common.network,
                provider_name,
                only_finalized_blocks,
            )
            .await
            .map(BlockStreamClient::Substreams)
            .map(|client| client.with_retry())
            .map_err(|err| DatasetError::from((dataset_name, err.into()))),

            DatasetKind::Sql | DatasetKind::Manifest => {
                // SQL and Manifest datasets don't have a client.
                Err(DatasetError::from((
                    dataset_name,
                    Error::UnsupportedKind(common.kind),
                )))
            }
        }
    }

    async fn find_provider(
        &self,
        kind: DatasetKind,
        network: String,
    ) -> Option<(String, ProviderConfigTomlValue)> {
        // Collect matching provider configurations into a vector for shuffling
        let mut matching_providers = self
            .providers
            .get_all()
            .await
            .iter()
            .filter(|(_, prov)| prov.kind == kind && prov.network == network)
            .map(|(name, prov)| (name.clone(), prov.clone()))
            .collect::<Vec<_>>();

        if matching_providers.is_empty() {
            return None;
        }

        // Try each provider in random order until we find one with successful env substitution
        matching_providers.shuffle(&mut rand::rng());

        'try_find_provider: for (provider_name, mut provider) in matching_providers {
            // Apply environment variable substitution to the `rest` table values
            for (_key, value) in provider.rest.iter_mut() {
                if let Err(err) = common::env_substitute::substitute_env_vars(value) {
                    tracing::warn!(
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

            // Convert back to toml::Value for compatibility
            // TODO: Remove conversion once the fn callers use ProviderConfig directly
            let value = toml::Value::try_from(provider)
                .expect("ProviderConfig structs should always be convertible to toml::Value");

            return Some((provider_name, value));
        }

        // If we get here, no suitable providers were found
        None
    }

    /// Returns cached eth_call scalar UDF, otherwise loads the UDF and caches it.
    async fn eth_call_for_dataset(&self, dataset: &Dataset) -> Result<Option<ScalarUDF>, Error> {
        #[derive(serde::Deserialize)]
        struct EvmRpcProvider {
            url: Url,
            rate_limit_per_minute: Option<NonZeroU32>,
        }

        if dataset.kind != "evm-rpc" {
            return Ok(None);
        }

        // Check if we already have the provider cached.
        if let Some(udf) = self.eth_call_cache.read().unwrap().get(&dataset.name) {
            return Ok(Some(udf.clone()));
        }

        // Load the provider from the dataset definition.
        let Some((_provider_name, provider)) = self
            .find_provider(DatasetKind::EvmRpc, dataset.network.clone())
            .await
        else {
            tracing::warn!(
                provider_kind = %DatasetKind::EvmRpc,
                provider_network = %dataset.network,
                "no providers available for the requested kind-network configuration"
            );
            return Err(Error::ProviderNotFound {
                dataset_kind: DatasetKind::EvmRpc,
                network: dataset.network.clone(),
            });
        };

        let provider: EvmRpcProvider = provider.try_into()?;
        // Cache the provider.
        let provider = if provider.url.scheme() == "ipc" {
            evm::provider::new_ipc(provider.url.path(), provider.rate_limit_per_minute)
                .await
                .map_err(Error::IpcConnectionError)?
        } else {
            evm::provider::new(provider.url, provider.rate_limit_per_minute)
        };
        let udf =
            AsyncScalarUDF::new(Arc::new(EthCall::new(&dataset.name, provider))).into_scalar_udf();
        self.eth_call_cache
            .write()
            .unwrap()
            .insert(dataset.name.clone(), udf.clone());
        Ok(Some(udf))
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
    ) -> Result<Catalog, DatasetError> {
        // Initialize the dataset store if not already done
        self.migrate_stored_files().await?;
        self.initialize().await?;

        let (tables, _) = resolve_table_references(query, true).map_err(DatasetError::unknown)?;
        let function_names = all_function_names(query).map_err(DatasetError::unknown)?;

        self.load_physical_catalog(tables, function_names, &env)
            .await
    }

    /// Looks up the datasets for the given table references and loads them into a catalog.
    pub async fn load_physical_catalog(
        self: &Arc<Self>,
        table_refs: impl IntoIterator<Item = TableReference>,
        function_names: impl IntoIterator<Item = String>,
        env: &QueryEnv,
    ) -> Result<Catalog, DatasetError> {
        // Initialize the dataset store if not already done
        self.migrate_stored_files().await?;
        self.initialize().await?;

        let logical_catalog = self
            .load_logical_catalog(table_refs, function_names, &env.isolate_pool)
            .await?;

        let mut tables = Vec::new();
        for table in &logical_catalog.tables {
            let physical_table = PhysicalTable::get_active(table, self.metadata_db.clone())
                .await
                .map_err(DatasetError::unknown)?
                .ok_or(DatasetError::unknown(format!(
                    "Table {} has not been synced",
                    table,
                )))?;
            tables.push(physical_table.into());
        }
        Ok(Catalog::new(tables, logical_catalog))
    }

    /// Similar to `catalog_for_sql`, but only for planning and not execution. This does not require a
    /// physical location to exist for the dataset views.
    pub async fn planning_ctx_for_sql(
        self: Arc<Self>,
        query: &parser::Statement,
    ) -> Result<PlanningContext, DatasetError> {
        // Initialize the dataset store if not already done
        self.migrate_stored_files().await?;
        self.initialize().await?;

        let (tables, _) = resolve_table_references(query, true).map_err(DatasetError::unknown)?;
        let function_names = all_function_names(query).map_err(DatasetError::unknown)?;
        let resolved_tables = self
            .load_logical_catalog(tables, function_names, &IsolatePool::dummy())
            .await?;
        Ok(PlanningContext::new(resolved_tables))
    }

    /// Looks up the datasets for the given table references and creates resolved tables. Create
    /// UDFs specific to the referenced datasets.
    async fn load_logical_catalog(
        self: &Arc<Self>,
        table_refs: impl IntoIterator<Item = TableReference>,
        function_names: impl IntoIterator<Item = String>,
        isolate_pool: &IsolatePool,
    ) -> Result<LogicalCatalog, DatasetError> {
        let table_refs: Vec<_> = table_refs.into_iter().collect();
        let mut dataset_names = datasets_from_table_refs(table_refs.iter().cloned())?;
        for func_name in function_names {
            match func_name.split('.').collect::<Vec<_>>().as_slice() {
                // Simple name assumed to be Datafusion built-in function.
                [_] => continue,
                [dataset, _] => {
                    dataset_names.insert(dataset.to_string());
                }
                _ => {
                    return Err(DatasetError::no_context(Error::UnsupportedFunctionName(
                        func_name,
                    )));
                }
            }
        }
        let mut resolved_tables = Vec::new();
        let mut udfs = Vec::new();
        for dataset_name in dataset_names {
            let dataset = self.load_dataset(&dataset_name, None).await?;
            let udf = self
                .eth_call_for_dataset(&dataset)
                .await
                .map_err(|err| (dataset_name.clone(), err))?;
            if let Some(udf) = udf {
                udfs.push(udf);
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
                            schema == dataset_name && table_name == table.name()
                        }
                        _ => false, // Unqualified table
                    }
                });

                if is_referenced {
                    let table_ref = TableReference::partial(dataset_name.clone(), table.name());
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

    /// Each `.sql` file in the directory with the same name as the dataset will be loaded as a table.
    fn sql_dataset(
        self: &Arc<Self>,
        dataset_def: serde_json::Value,
    ) -> BoxFuture<'static, Result<SqlDataset, Error>> {
        sql_datasets::dataset(Arc::clone(self), dataset_def)
            .map_err(Error::SqlDatasetError)
            .boxed()
    }
}

fn datasets_from_table_refs(
    table_refs: impl Iterator<Item = TableReference>,
) -> Result<BTreeSet<String>, DatasetError> {
    let mut dataset_names = BTreeSet::new();
    for t in table_refs {
        if t.catalog().is_some() {
            return Err(DatasetError::no_context(Error::UnsupportedName(
                format!(
                    "found table qualified with catalog '{}', tables must only be qualified with a dataset name",
                    t.table()
                )
                .into(),
            )));
        }

        let Some(catalog_schema) = t.schema() else {
            return Err(DatasetError::no_context(Error::UnsupportedName(
                format!(
                "found unqualified table '{}', all tables must be qualified with a dataset name",
                t.table()
            )
                .into(),
            )));
        };
        dataset_names.insert(catalog_schema.to_string());
    }
    Ok(dataset_names)
}

#[derive(Clone)]
enum BlockStreamClient {
    EvmRpc(evm_rpc_datasets::JsonRpcClient),
    Firehose(firehose_datasets::Client),
    Substreams(substreams_datasets::Client),
}

impl BlockStreamer for BlockStreamClient {
    async fn block_stream(
        self,
        start_block: BlockNum,
        end_block: BlockNum,
    ) -> impl Stream<Item = Result<RawDatasetRows, BoxError>> + Send {
        // Each client returns a different concrete stream type, so we
        // use `stream!` to unify them into a wrapper stream
        async_stream::stream! {
            match self {
                Self::EvmRpc(client) => {
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
                Self::Substreams(client) => {
                    let stream = client.block_stream(start_block, end_block).await;
                    for await item in stream {
                        yield item;
                    }
                }
            }
        }
    }

    async fn latest_block(&mut self) -> Result<BlockNum, BoxError> {
        match self {
            Self::EvmRpc(client) => client.latest_block().await,
            Self::Firehose(client) => client.latest_block().await,
            Self::Substreams(client) => client.latest_block().await,
        }
    }

    fn provider_name(&self) -> &str {
        match self {
            Self::EvmRpc(client) => client.provider_name(),
            Self::Firehose(client) => client.provider_name(),
            Self::Substreams(client) => client.provider_name(),
        }
    }
}

/// Return a table identifier, in the form `{dataset}.blocks`, for the given network.
#[instrument(skip(dataset_store), err)]
pub async fn resolve_blocks_table(
    dataset_store: &Arc<DatasetStore>,
    src_datasets: &BTreeSet<&str>,
    network: &str,
) -> Result<PhysicalTable, BoxError> {
    let dataset = {
        fn is_raw_dataset(kind: &str) -> bool {
            ["evm-rpc", "firehose"].contains(&kind)
        }

        let mut datasets: Vec<Dataset> = dataset_store
            .all_datasets()
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
