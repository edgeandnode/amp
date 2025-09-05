use std::{
    collections::BTreeSet,
    num::NonZeroU32,
    str::FromStr,
    sync::{Arc, RwLock},
};

use async_stream::stream;
use common::{
    BlockNum, BlockStreamer, BoxError, DataTypeJsonSchema, Dataset, DatasetValue, LogicalCatalog,
    RawDatasetRows, SPECIAL_BLOCK_NUM, Store,
    catalog::physical::{Catalog, PhysicalTable},
    config::Config,
    evm::{self, udfs::EthCall},
    manifest::{self, Manifest, Version},
    query_context::{self, PlanningContext, QueryEnv},
    sql_visitors::all_function_names,
    store::StoreError,
};
use datafusion::{
    common::HashMap,
    logical_expr::{ScalarUDF, async_udf::AsyncScalarUDF},
    sql::{TableReference, parser, resolve::resolve_table_references},
};
use futures::{FutureExt as _, Stream, TryFutureExt as _, future::BoxFuture};
use js_runtime::isolate_pool::IsolatePool;
use metadata_db::MetadataDb;
use rand::seq::SliceRandom;
use sql_datasets::SqlDataset;
use tracing::instrument;
use url::Url;

mod dataset_kind;
pub mod providers;
pub mod sql_datasets;

pub use self::dataset_kind::{DatasetKind, UnsupportedKindError};
use self::providers::ProvidersConfigStore;

/// Alias for TOML value type used in provider configurations.
// TODO: #[deprecated(note = "use dataset_store::providers::ProviderConfig instead")]
pub type ProviderConfigTomlValue = toml::Value;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to fetch: {0}")]
    FetchError(#[from] StoreError),

    #[error("TOML parse error: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Metadata db error: {0}")]
    MetadataDbError(#[from] metadata_db::Error),

    #[error("unsupported dataset kind '{0}'")]
    UnsupportedKind(String),

    #[error("dataset field 'name = \"{0}\"' does not match filename '{1}'")]
    NameMismatch(String, String),

    #[error("Schema mismatch")]
    SchemaMismatch,

    #[error("`schema` field is missing, but required for dataset kind {dataset_kind}")]
    SchemaMissing { dataset_kind: DatasetKind },

    #[error("unsupported table name: {0}")]
    UnsupportedName(BoxError),

    #[error("unsupported function name: {0}")]
    UnsupportedFunctionName(String),

    #[error("EVM RPC error: {0}")]
    EvmRpcError(#[from] evm_rpc_datasets::Error),

    #[error("firehose error: {0}")]
    FirehoseError(#[from] firehose_datasets::Error),

    #[error("error loading sql dataset: {0}")]
    SqlDatasetError(BoxError),

    #[error("error deserializing manifest: {0}")]
    ManifestError(serde_json::Error),

    #[error("error parsing SQL: {0}")]
    SqlParseError(query_context::Error),

    #[error("provider not found for dataset kind '{dataset_kind}' and network '{network}'")]
    ProviderNotFound {
        dataset_kind: DatasetKind,
        network: String,
    },

    #[error("provider configuration file is not valid UTF-8 at {location}: {source}")]
    ProviderInvalidUtf8 {
        location: String,
        #[source]
        source: std::string::FromUtf8Error,
    },

    #[error("provider environment substitution failed at {location}: {source}")]
    ProviderEnvSubstitution {
        location: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("dataset '{0}' version '{1}' not found")]
    DatasetVersionNotFound(String, String),

    #[error("{0}")]
    Unknown(BoxError),
}

impl From<providers::FetchError> for Error {
    fn from(err: providers::FetchError) -> Self {
        match err {
            providers::FetchError::EnvSubstitutionFailed { location, source } => {
                Error::ProviderEnvSubstitution { location, source }
            }
            providers::FetchError::StoreFetchFailed(box_error) => {
                // Try to downcast BoxError to StoreError, otherwise map to Unknown
                match box_error.downcast::<StoreError>() {
                    Ok(store_error) => Error::FetchError(*store_error),
                    Err(box_error) => Error::Unknown(box_error),
                }
            }
            providers::FetchError::TomlParseError(toml_error) => Error::Toml(toml_error),
            providers::FetchError::InvalidUtf8 { location, source } => {
                Error::ProviderInvalidUtf8 { location, source }
            }
        }
    }
}

impl From<UnsupportedKindError> for Error {
    fn from(err: UnsupportedKindError) -> Self {
        Error::UnsupportedKind(err.kind)
    }
}

#[derive(Debug, thiserror::Error)]
pub struct DatasetError {
    pub dataset: Option<String>,

    #[source]
    error: Error,
}

impl DatasetError {
    fn no_context(error: Error) -> Self {
        Self {
            dataset: None,
            error,
        }
    }

    fn unknown(error: impl Into<BoxError>) -> Self {
        Self {
            dataset: None,
            error: Error::Unknown(error.into()),
        }
    }

    pub fn is_not_found(&self) -> bool {
        matches!(
            &self.error,
            Error::FetchError(e) if e.is_not_found()
        ) || matches!(&self.error, Error::DatasetVersionNotFound(_, _))
    }
}

impl From<(&str, Error)> for DatasetError {
    fn from((dataset, error): (&str, Error)) -> Self {
        Self {
            dataset: Some(dataset.to_string()),
            error,
        }
    }
}

impl From<(String, Error)> for DatasetError {
    fn from((dataset, error): (String, Error)) -> Self {
        Self {
            dataset: Some(dataset),
            error,
        }
    }
}

impl std::fmt::Display for DatasetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let e = &self.error;
        match &self.dataset {
            Some(dataset) if self.is_not_found() => {
                write!(f, "dataset '{}' not found, full error: {}", dataset, e)
            }
            Some(dataset) => write!(f, "error with dataset '{}': {}", dataset, e),
            None => write!(f, "{}", e),
        }
    }
}

#[derive(Clone)]
pub struct DatasetStore {
    config: Arc<Config>,
    metadata_db: Arc<MetadataDb>,
    // Cache maps dataset name to eth_call UDF.
    eth_call_cache: Arc<RwLock<HashMap<String, ScalarUDF>>>,
    // This cache maps dataset name to the dataset definition.
    dataset_cache: Arc<RwLock<HashMap<String, Dataset>>>,
    // Provider store for managing provider configurations and caching.
    providers: ProvidersConfigStore,
}

impl DatasetStore {
    pub fn new(config: Arc<Config>, metadata_db: Arc<MetadataDb>) -> Arc<Self> {
        let providers_store = ProvidersConfigStore::new(config.providers_store.prefixed_store());
        Arc::new(Self {
            config,
            metadata_db,
            eth_call_cache: Default::default(),
            dataset_cache: Default::default(),
            providers: providers_store,
        })
    }

    pub async fn load_dataset(
        self: &Arc<Self>,
        dataset: &str,
        version: Option<&Version>,
    ) -> Result<Dataset, DatasetError> {
        let dataset_identifier = self.load_dataset_from_registry(dataset, version).await?;
        self.clone()
            .load_dataset_inner(&dataset_identifier)
            .await
            .map_err(|e| (dataset, e).into())
    }

    pub async fn try_load_dataset(
        self: &Arc<Self>,
        dataset: &str,
        version: Option<&Version>,
    ) -> Result<Option<Dataset>, DatasetError> {
        match self.load_dataset(dataset, version).await {
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
        dataset: &str,
        version: Option<&Version>,
    ) -> Result<String, DatasetError> {
        let dataset_identifier = match version {
            Some(version) => self
                .metadata_db
                .get_dataset(dataset, &version.to_string())
                .await
                .map_err(|e| DatasetError::from((dataset, Error::MetadataDbError(e))))?
                .ok_or_else(|| {
                    DatasetError::from((
                        dataset,
                        Error::DatasetVersionNotFound(dataset.to_string(), version.to_string()),
                    ))
                })?,
            None => {
                let latest_version = self
                    .metadata_db
                    .get_latest_dataset_version(dataset)
                    .await
                    .map_err(|e| DatasetError::from((dataset, Error::MetadataDbError(e))))?;
                match latest_version {
                    Some((dataset, version)) => {
                        let version_identifier =
                            Version::version_identifier(&version).map_err(|e| {
                                DatasetError::from((dataset.as_str(), Error::Unknown(e)))
                            })?;
                        format!("{}__{}", dataset, version_identifier)
                    }
                    None => dataset.to_string(),
                }
            }
        };
        Ok(dataset_identifier)
    }

    pub async fn all_datasets(self: &Arc<Self>) -> Result<Vec<Dataset>, DatasetError> {
        let all_objs = self
            .dataset_defs_store()
            .list_all_shallow()
            .await
            .map_err(|e| DatasetError::no_context(e.into()))?;

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
            .map_err(|e| (dataset, e).into())
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
            .map_err(|e| (dataset, e).into())
    }

    /// Loads a [RawDataset] and [common](DatasetDefsCommon) data for a TOML or JSON file with the
    /// given name.
    async fn common_data_and_dataset(
        &self,
        dataset_name: &str,
    ) -> Result<(DatasetDefsCommon, RawDataset), Error> {
        use Error::*;
        let raw_dataset = self
            .dataset_defs_store()
            .get_string(format!("{}.toml", dataset_name))
            .map_ok(RawDataset::Toml)
            .or_else(|err| async move {
                if !err.is_not_found() {
                    return Err(err);
                }
                self.dataset_defs_store()
                    .get_string(format!("{}.json", dataset_name))
                    .await
                    .map(RawDataset::Json)
            })
            .await?;

        let common: DatasetDefsCommon = match raw_dataset {
            RawDataset::Toml(ref raw) => {
                tracing::warn!("TOML dataset format is deprecated!");
                toml::from_str::<DatasetDefsCommon>(raw)?
            }
            RawDataset::Json(ref raw) => serde_json::from_str::<DatasetDefsCommon>(raw)?,
        };

        if common.name != dataset_name && common.kind != "manifest" {
            return Err(NameMismatch(common.name, dataset_name.to_string()));
        }

        Ok((common, raw_dataset))
    }

    async fn load_manifest_dataset_inner(
        self: Arc<Self>,
        dataset_name: &str,
    ) -> Result<SqlDataset, Error> {
        let filename = format!("{}.json", dataset_name);
        let raw_manifest = self.dataset_defs_store().get_string(filename).await?;
        let manifest: Manifest =
            serde_json::from_str(&raw_manifest).map_err(Error::ManifestError)?;
        let queries = manifest.queries().map_err(Error::SqlParseError)?;
        let dataset = manifest::dataset(manifest).map_err(Error::Unknown)?;
        Ok(SqlDataset { dataset, queries })
    }

    async fn load_dataset_inner(
        self: &Arc<Self>,
        dataset_identifier: &str,
    ) -> Result<Dataset, Error> {
        if let Some(dataset) = self.dataset_cache.read().unwrap().get(dataset_identifier) {
            return Ok(dataset.clone());
        }

        let (common, raw_dataset) = self.common_data_and_dataset(dataset_identifier).await?;
        let kind = DatasetKind::from_str(&common.kind)?;
        let value = raw_dataset.to_value()?;
        let (dataset, ground_truth_schema) = match kind {
            DatasetKind::EvmRpc => {
                let builtin_schema: SerializableSchema =
                    evm_rpc_datasets::tables::all(&common.network).into();
                (evm_rpc_datasets::dataset(value)?, Some(builtin_schema))
            }
            DatasetKind::Firehose => {
                let builtin_schema: SerializableSchema =
                    firehose_datasets::evm::tables::all(&common.network).into();
                (
                    firehose_datasets::evm::dataset(value)?,
                    Some(builtin_schema),
                )
            }
            DatasetKind::Substreams => {
                let dataset = substreams_datasets::dataset(value).await?;
                let store_schema: SerializableSchema = dataset.tables.as_slice().into();
                (dataset, Some(store_schema))
            }
            DatasetKind::Sql => {
                let store_dataset = Arc::clone(self).sql_dataset(value).await?.dataset;
                (store_dataset, None)
            }
            DatasetKind::Manifest => {
                let manifest = raw_dataset.to_manifest()?;
                let dataset = manifest::dataset(manifest).map_err(Error::Unknown)?;
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
    ) -> Result<impl BlockStreamer, DatasetError> {
        self.load_client_inner(dataset, only_finalized_blocks)
            .await
            .map_err(|e| (dataset, e).into())
    }

    async fn load_client_inner(
        &self,
        dataset_name: &str,
        only_finalized_blocks: bool,
    ) -> Result<BlockStreamClient, Error> {
        let (common, raw_dataset) = self.common_data_and_dataset(dataset_name).await?;
        let value = raw_dataset.to_value()?;
        let kind = DatasetKind::from_str(&common.kind)?;

        let provider = self.find_provider(kind, common.network.clone()).await?;
        Ok(match kind {
            DatasetKind::EvmRpc => BlockStreamClient::EvmRpc(
                evm_rpc_datasets::client(provider, common.network, only_finalized_blocks).await?,
            ),
            DatasetKind::Firehose => BlockStreamClient::Firehose(
                firehose_datasets::Client::new(provider, common.network, only_finalized_blocks)
                    .await?,
            ),
            DatasetKind::Substreams => BlockStreamClient::Substreams(
                substreams_datasets::Client::new(
                    provider,
                    value,
                    common.network,
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
    ) -> Result<ProviderConfigTomlValue, Error> {
        // Collect matching provider configurations into a vector for shuffling
        let mut matching_providers = self
            .providers
            .get_all()
            .await
            .values()
            .filter(|prov| prov.kind == kind && prov.network == network)
            .cloned()
            .collect::<Vec<_>>();

        if matching_providers.is_empty() {
            return Err(Error::ProviderNotFound {
                dataset_kind: kind,
                network,
            });
        }

        // Shuffle to provide load balancing across providers
        matching_providers.shuffle(&mut rand::rng());

        // Return the first (randomly selected) matching provider configuration
        let Some(provider) = matching_providers.into_iter().next() else {
            unreachable!("matching_providers should not be empty at this point")
        };

        // Convert back to toml::Value for compatibility
        // TODO: Remove conversion once the fn callers use ProviderConfig directly
        let value = toml::Value::try_from(provider)
            .expect("ProviderConfig structs should always be convertible to toml::Value");

        Ok(value)
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
        let provider = self
            .find_provider(DatasetKind::EvmRpc, dataset.network.clone())
            .await?;
        let provider: EvmRpcProvider = provider.try_into()?;
        // Cache the provider.
        let provider = evm::provider::new(provider.url, provider.rate_limit_per_minute);
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
                .map_err(|e| (dataset_name.clone(), e))?;
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
        dataset_def: common::DatasetValue,
    ) -> BoxFuture<'static, Result<SqlDataset, Error>> {
        sql_datasets::dataset(self, dataset_def)
            .map_err(Error::SqlDatasetError)
            .boxed()
    }

    pub fn dataset_defs_store(&self) -> &Arc<Store> {
        &self.config.dataset_defs_store
    }
}

/// Represents a raw dataset definition, either in TOML or JSON format.
enum RawDataset {
    Toml(String),
    Json(String),
}

impl RawDataset {
    pub fn to_manifest(&self) -> Result<Manifest, Error> {
        let manifest = match self {
            RawDataset::Toml(raw) => toml::from_str(raw)?,
            RawDataset::Json(raw) => serde_json::from_str(raw)?,
        };

        Ok(manifest)
    }

    fn to_value(&self) -> Result<DatasetValue, Error> {
        let value = match self {
            RawDataset::Toml(raw) => DatasetValue::Toml(toml::Value::from_str(raw)?),
            RawDataset::Json(raw) => DatasetValue::Json(serde_json::Value::from_str(raw)?),
        };

        Ok(value)
    }
}

/// All dataset definitions must have a kind, network and name. The name must match the filename. Schema is optional for TOML dataset format.
#[derive(serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
pub struct DatasetDefsCommon {
    /// Dataset kind. See specific dataset definitions for supported values.
    pub kind: String,
    /// Network name, e.g. "mainnet".
    pub network: String,
    /// Dataset name.
    pub name: String,
    /// Dataset schema. Lists the tables defined by this dataset.
    pub schema: Option<SerializableSchema>,
}

/// A serializable representation of a collection of [`arrow::datatypes::Schema`]s, without any metadata.
#[derive(Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
pub struct SerializableSchema(
    std::collections::HashMap<String, std::collections::HashMap<String, DataTypeJsonSchema>>,
);

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
        stream! {
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
}

impl From<Vec<common::catalog::logical::Table>> for SerializableSchema {
    fn from(tables: Vec<common::catalog::logical::Table>) -> Self {
        let inner = tables
            .into_iter()
            .map(|table| {
                let inner_map = table
                    .schema()
                    .fields()
                    .iter()
                    .filter(|&field| field.name() != SPECIAL_BLOCK_NUM)
                    .fold(std::collections::HashMap::new(), |mut acc, field| {
                        acc.insert(
                            field.name().clone(),
                            DataTypeJsonSchema(field.data_type().clone()),
                        );
                        acc
                    });
                (table.name().to_string().clone(), inner_map)
            })
            .collect();

        Self(inner)
    }
}

impl From<&[common::catalog::logical::Table]> for SerializableSchema {
    fn from(tables: &[common::catalog::logical::Table]) -> Self {
        let inner = tables
            .iter()
            .map(|table| {
                let inner_map = table
                    .schema()
                    .fields()
                    .iter()
                    .filter(|&field| field.name() != SPECIAL_BLOCK_NUM)
                    .fold(std::collections::HashMap::new(), |mut acc, field| {
                        acc.insert(
                            field.name().clone(),
                            DataTypeJsonSchema(field.data_type().clone()),
                        );
                        acc
                    });
                (table.name().to_string().clone(), inner_map)
            })
            .collect();

        Self(inner)
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
