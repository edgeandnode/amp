use std::{
    collections::BTreeSet,
    num::NonZeroU32,
    str::FromStr,
    sync::{Arc, RwLock},
};

use async_stream::stream;
use common::{
    BlockNum, BlockStreamer, BlockStreamerExt, BoxError, Dataset, DatasetValue, LogicalCatalog,
    PlanningContext, RawDatasetRows, Store,
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
    metadata_db: MetadataDb,
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
    pub fn new(config: Arc<Config>, metadata_db: MetadataDb) -> Arc<Self> {
        let providers_store = ProvidersConfigStore::new(config.providers_store.prefixed_store());
        Arc::new(Self {
            metadata_db,
            eth_call_cache: Default::default(),
            dataset_cache: Default::default(),
            providers: providers_store,
            dataset_defs_store: config.dataset_defs_store.clone(),
        })
    }

    /// Get a reference to the providers configuration store
    pub fn providers(&self) -> &ProvidersConfigStore {
        &self.providers
    }

    /// Register a dataset manifest in both the dataset store and metadata database.
    /// Validates the manifest doesn't already exist, serializes it to JSON, stores it
    /// in the dataset definitions store, and records metadata in the database.
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

    pub async fn load_dataset(
        self: &Arc<Self>,
        name: &str,
        version: Option<&Version>,
    ) -> Result<Dataset, DatasetError> {
        let dataset_identifier = self.load_dataset_from_registry(name, version).await?;
        self.clone()
            .load_dataset_inner(&dataset_identifier)
            .await
            .map_err(|err| (name, err).into())
    }

    pub async fn try_load_dataset(
        self: &Arc<Self>,
        name: &str,
        version: Option<&Version>,
    ) -> Result<Option<Dataset>, DatasetError> {
        match self.load_dataset(name, version).await {
            Ok(dataset) => Ok(Some(dataset)),
            Err(e) => {
                if e.is_not_found() {
                    Ok(None)
                } else {
                    Err(e)
                }
            }
        }
    }

    pub async fn load_dataset_from_registry(
        self: &Arc<Self>,
        name: &str,
        version: Option<&Version>,
    ) -> Result<String, DatasetError> {
        let dataset_identifier = match version {
            Some(version) => self
                .metadata_db
                .get_dataset_manifest_path(name, version)
                .await
                .map_err(|err| DatasetError::from((name, Error::MetadataDbError(err))))?
                .map(|manifest_path| manifest_path.trim_end_matches(".json").to_string())
                .ok_or_else(|| {
                    DatasetError::from((
                        name,
                        Error::DatasetVersionNotFound(name.to_string(), version.to_string()),
                    ))
                })?,
            None => {
                match self
                    .metadata_db
                    .get_dataset_latest_version_with_details(name)
                    .await
                    .map_err(|err| DatasetError::from((name, Error::MetadataDbError(err))))?
                    .map(|details| details.version)
                {
                    Some(version) => {
                        let version_identifier = Version::from(version).to_underscore_version();
                        format!("{}__{}", name, version_identifier)
                    }
                    None => name.to_string(),
                }
            }
        };
        Ok(dataset_identifier)
    }

    pub async fn all_datasets(self: &Arc<Self>) -> Result<Vec<Dataset>, DatasetError> {
        let all_objs = self
            .dataset_defs_store
            .list_all_shallow()
            .await
            .map_err(DatasetError::no_context)?;

        let mut datasets = Vec::new();
        for obj in all_objs {
            // Unwrap: We listed files.
            let path = std::path::Path::new(obj.location.filename().unwrap());
            let stem = path.file_stem().and_then(|s| s.to_str());
            if stem.is_none() {
                continue;
            }
            datasets.push(self.load_dataset(stem.unwrap(), None).await?);
        }
        Ok(datasets)
    }

    pub async fn load_sql_dataset(
        self: &Arc<Self>,
        dataset: &str,
    ) -> Result<SqlDataset, DatasetError> {
        self.clone()
            .load_sql_dataset_inner(dataset)
            .await
            .map_err(|err| (dataset, err).into())
    }

    /// It's a hack for this to return a `SqlDataset`. Soon we should deprecate `SqlDataset` and
    /// define a new struct to represent manifest datasets.
    pub async fn load_manifest_dataset(
        self: &Arc<Self>,
        dataset: &str,
        version: &Version,
    ) -> Result<SqlDataset, DatasetError> {
        let dataset_identifier = self
            .load_dataset_from_registry(dataset, Some(version))
            .await?;
        self.clone()
            .load_manifest_dataset_inner(&dataset_identifier)
            .await
            .map_err(|err| (dataset, err).into())
    }

    /// Loads a [DatasetSrc] and [common](CommonManifest) data for a TOML or JSON file with the
    /// given name.
    async fn common_data_and_dataset(
        &self,
        dataset_name: &str,
    ) -> Result<(CommonManifest, DatasetSrc), Error> {
        use Error::*;
        let dataset_src = self
            .dataset_defs_store
            .get_string(format!("{}.toml", dataset_name))
            .map_ok(DatasetSrc::Toml)
            .or_else(|err| async move {
                if !err.is_not_found() {
                    return Err(err);
                }
                self.dataset_defs_store
                    .get_string(format!("{}.json", dataset_name))
                    .await
                    .map(DatasetSrc::Json)
            })
            .await?;

        let common: CommonManifest = match &dataset_src {
            DatasetSrc::Toml(src) => {
                tracing::warn!("TOML dataset format is deprecated!");
                toml::from_str::<CommonManifest>(src)?
            }
            DatasetSrc::Json(src) => serde_json::from_str::<CommonManifest>(src)?,
        };

        if common.name != dataset_name && common.kind != "manifest" {
            return Err(NameMismatch(
                common.name.to_string(),
                dataset_name.to_string(),
            ));
        }

        Ok((common, dataset_src))
    }

    async fn load_manifest_dataset_inner(
        self: Arc<Self>,
        dataset_name: &str,
    ) -> Result<SqlDataset, Error> {
        let filename = format!("{}.json", dataset_name);
        let raw_manifest = self.dataset_defs_store.get_string(filename).await?;
        let manifest: Manifest =
            serde_json::from_str(&raw_manifest).map_err(Error::ManifestError)?;
        let queries = manifest.queries().map_err(Error::SqlParseError)?;
        let dataset = manifest::derived::dataset(manifest).map_err(Error::Unknown)?;
        Ok(SqlDataset { dataset, queries })
    }

    async fn load_dataset_inner(
        self: &Arc<Self>,
        dataset_identifier: &str,
    ) -> Result<Dataset, Error> {
        if let Some(dataset) = self.dataset_cache.read().unwrap().get(dataset_identifier) {
            return Ok(dataset.clone());
        }

        let (common, dataset_src) = self.common_data_and_dataset(dataset_identifier).await?;
        let kind = DatasetKind::from_str(&common.kind)?;
        let value = dataset_src.to_value()?;
        let (dataset, ground_truth_schema) = match kind {
            DatasetKind::EvmRpc => {
                let builtin_schema =
                    schema_from_tables(evm_rpc_datasets::tables::all(&common.network));
                (evm_rpc_datasets::dataset(value)?, Some(builtin_schema))
            }
            DatasetKind::EthBeacon => {
                let builtin_schema =
                    schema_from_tables(eth_beacon_datasets::all_tables(common.network.clone()));
                (eth_beacon_datasets::dataset(value)?, Some(builtin_schema))
            }
            DatasetKind::Firehose => {
                let builtin_schema =
                    schema_from_tables(firehose_datasets::evm::tables::all(&common.network));
                (
                    firehose_datasets::evm::dataset(value)?,
                    Some(builtin_schema),
                )
            }
            DatasetKind::Substreams => {
                let dataset = substreams_datasets::dataset(value).await?;
                let store_schema = schema_from_table_slice(dataset.tables.as_slice());
                (dataset, Some(store_schema))
            }
            DatasetKind::Sql => {
                let store_dataset = Arc::clone(self).sql_dataset(value).await?.dataset;
                (store_dataset, None)
            }
            DatasetKind::Manifest => {
                let manifest = dataset_src.to_manifest()?;
                let dataset = manifest::derived::dataset(manifest).map_err(Error::Unknown)?;
                (dataset, None)
            }
        };

        if let Some(ground_truth_schema) = ground_truth_schema {
            let Some(loaded_schema) = common.schema else {
                return Err(Error::SchemaMissing { dataset_kind: kind });
            };
            if loaded_schema != ground_truth_schema {
                return Err(Error::SchemaMismatch);
            }
        }

        // Cache the dataset.
        self.dataset_cache
            .write()
            .unwrap()
            .insert(dataset_identifier.to_string(), dataset.clone());

        Ok(dataset)
    }

    #[instrument(skip(self), err)]
    async fn load_sql_dataset_inner(
        self: Arc<Self>,
        dataset_name: &str,
    ) -> Result<SqlDataset, Error> {
        let (common, raw_dataset) = self.common_data_and_dataset(dataset_name).await?;
        let kind = DatasetKind::from_str(&common.kind)?;

        if kind != DatasetKind::Sql {
            return Err(Error::UnsupportedKind(kind.to_string()));
        }

        let value = raw_dataset.to_value()?;

        self.sql_dataset(value).await
    }

    pub async fn load_client(
        &self,
        dataset: &str,
        only_finalized_blocks: bool,
        meter: Option<&monitoring::telemetry::metrics::Meter>,
    ) -> Result<impl BlockStreamer, DatasetError> {
        self.load_client_inner(dataset, only_finalized_blocks, meter)
            .await
            .map_err(|err| (dataset, err).into())
            .map(|client| client.with_retry())
    }

    #[instrument(skip(self), err)]
    async fn load_client_inner(
        &self,
        dataset_name: &str,
        only_finalized_blocks: bool,
        meter: Option<&monitoring::telemetry::metrics::Meter>,
    ) -> Result<BlockStreamClient, Error> {
        let (common, raw_dataset) = self.common_data_and_dataset(dataset_name).await?;
        let value = raw_dataset.to_value()?;
        let kind = DatasetKind::from_str(&common.kind)?;

        let Some((provider_name, provider)) =
            self.find_provider(kind, common.network.clone()).await
        else {
            tracing::warn!(
                provider_kind = %kind,
                provider_network = %common.network,
                "no providers available for the requested kind-network configuration"
            );
            return Err(Error::ProviderNotFound {
                dataset_kind: kind,
                network: common.network,
            });
        };
        Ok(match kind {
            DatasetKind::EvmRpc => BlockStreamClient::EvmRpc(
                evm_rpc_datasets::client(
                    provider,
                    common.network,
                    provider_name,
                    only_finalized_blocks,
                    meter,
                )
                .await?,
            ),
            DatasetKind::EthBeacon => BlockStreamClient::EthBeacon(eth_beacon_datasets::client(
                provider,
                common.network,
                provider_name,
                only_finalized_blocks,
            )?),
            DatasetKind::Firehose => BlockStreamClient::Firehose(
                firehose_datasets::Client::new(
                    provider,
                    common.network,
                    provider_name,
                    only_finalized_blocks,
                )
                .await?,
            ),
            DatasetKind::Substreams => BlockStreamClient::Substreams(
                substreams_datasets::Client::new(
                    provider,
                    value,
                    common.network,
                    provider_name,
                    only_finalized_blocks,
                )
                .await?,
            ),
            DatasetKind::Sql | DatasetKind::Manifest => {
                // SQL and Manifest datasets don't have a client.
                return Err(Error::UnsupportedKind(common.kind));
            }
        })
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
        self: Arc<Self>,
        dataset_def: DatasetValue,
    ) -> BoxFuture<'static, Result<SqlDataset, Error>> {
        sql_datasets::dataset(self, dataset_def)
            .map_err(Error::SqlDatasetError)
            .boxed()
    }
}

/// Represents a dataset definition source text, either in TOML or JSON format.
enum DatasetSrc {
    Toml(String),
    Json(String),
}

impl DatasetSrc {
    pub fn to_manifest(&self) -> Result<Manifest, Error> {
        let manifest = match self {
            DatasetSrc::Toml(src) => toml::from_str(src)?,
            DatasetSrc::Json(src) => serde_json::from_str(src)?,
        };

        Ok(manifest)
    }

    fn to_value(&self) -> Result<DatasetValue, Error> {
        let value = match self {
            DatasetSrc::Toml(src) => DatasetValue::Toml(toml::Value::from_str(src)?),
            DatasetSrc::Json(src) => DatasetValue::Json(serde_json::Value::from_str(src)?),
        };

        Ok(value)
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
    EthBeacon(eth_beacon_datasets::BeaconClient),
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
        stream! {
            match self {
                Self::EvmRpc(client) => {
                    let stream = client.block_stream(start_block, end_block).await;
                    for await item in stream {
                        yield item;
                    }
                }
                Self::EthBeacon(client) => {
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
            Self::EthBeacon(client) => client.latest_block().await,
            Self::Firehose(client) => client.latest_block().await,
            Self::Substreams(client) => client.latest_block().await,
        }
    }

    fn provider_name(&self) -> &str {
        match self {
            Self::EvmRpc(client) => client.provider_name(),
            Self::EthBeacon(client) => client.provider_name(),
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
