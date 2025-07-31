pub mod sql_datasets;

use core::fmt;
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU32,
    str::FromStr,
    sync::{Arc, RwLock},
};

use async_stream::stream;
use common::{
    BlockNum, BlockStreamer, BoxError, DataTypeJsonSchema, Dataset, DatasetValue, LogicalCatalog,
    QueryContext, RawDatasetRows, SPECIAL_BLOCK_NUM, Store,
    catalog::physical::{Catalog, PhysicalTable},
    config::Config,
    evm::{self, udfs::EthCall},
    manifest::{Manifest, TableInput},
    query_context::{self, PlanningContext, QueryEnv, parse_sql},
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
use object_store::ObjectMeta;
use rand::seq::SliceRandom;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sql_datasets::SqlDataset;
use thiserror::Error;
use tracing::{error, instrument};
use url::Url;

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to fetch: {0}")]
    FetchError(#[from] StoreError),

    #[error("TOML parse error: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),

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

    #[error("{0}")]
    Unknown(BoxError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DatasetKind {
    EvmRpc,
    Firehose,
    Substreams,
    Sql, // Will be deprecated in favor of `Manifest`
    Manifest,
}

impl DatasetKind {
    pub fn as_str(&self) -> &str {
        match self {
            Self::EvmRpc => evm_rpc_datasets::DATASET_KIND,
            Self::Firehose => firehose_datasets::DATASET_KIND,
            Self::Substreams => substreams_datasets::DATASET_KIND,
            Self::Sql => sql_datasets::DATASET_KIND,
            Self::Manifest => common::manifest::DATASET_KIND,
        }
    }

    pub fn is_raw(&self) -> bool {
        match self {
            Self::EvmRpc | Self::Firehose | Self::Substreams => true,
            Self::Sql | Self::Manifest => false,
        }
    }
}

impl fmt::Display for DatasetKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EvmRpc => f.write_str(evm_rpc_datasets::DATASET_KIND),
            Self::Firehose => f.write_str(firehose_datasets::DATASET_KIND),
            Self::Substreams => f.write_str(substreams_datasets::DATASET_KIND),
            Self::Sql => f.write_str(sql_datasets::DATASET_KIND),
            Self::Manifest => f.write_str(common::manifest::DATASET_KIND),
        }
    }
}

impl FromStr for DatasetKind {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            evm_rpc_datasets::DATASET_KIND => Ok(Self::EvmRpc),
            firehose_datasets::DATASET_KIND => Ok(Self::Firehose),
            substreams_datasets::DATASET_KIND => Ok(Self::Substreams),
            sql_datasets::DATASET_KIND => Ok(Self::Sql),
            common::manifest::DATASET_KIND => Ok(Self::Manifest),
            k => Err(Error::UnsupportedKind(k.to_string())),
        }
    }
}

#[derive(Debug, Error)]
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
        )
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

impl fmt::Display for DatasetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
    pub metadata_db: Arc<MetadataDb>,
    // Cache maps dataset name to eth_call UDF.
    eth_call_cache: Arc<RwLock<HashMap<String, ScalarUDF>>>,
    // This cache maps dataset name to the dataset definition.
    dataset_cache: Arc<RwLock<HashMap<String, Dataset>>>,
    // Cache of provider definitions.
    providers_cache: Arc<RwLock<Option<Vec<ObjectMeta>>>>,
}

impl DatasetStore {
    pub fn new(config: Arc<Config>, metadata_db: Arc<MetadataDb>) -> Arc<Self> {
        Arc::new(Self {
            config,
            metadata_db,
            eth_call_cache: Default::default(),
            dataset_cache: Default::default(),
            providers_cache: Default::default(),
        })
    }

    // TODO: Update to return a Result<Option<..>, Error> if the dataset is not found
    pub async fn load_dataset(self: &Arc<Self>, dataset: &str) -> Result<Dataset, DatasetError> {
        self.clone()
            .load_dataset_inner(dataset)
            .await
            .map_err(|e| (dataset, e).into())
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
            datasets.push(self.load_dataset(stem.unwrap()).await?);
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
    ) -> Result<SqlDataset, DatasetError> {
        self.clone()
            .load_manifest_dataset_inner(dataset)
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
                toml::from_str::<DatasetDefsCommon>(raw)?.into()
            }
            RawDataset::Json(ref raw) => serde_json::from_str::<DatasetDefsCommon>(raw)?.into(),
        };

        if common.name != dataset_name {
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
        let dataset = Dataset::from(manifest.clone());
        let mut queries = BTreeMap::new();
        for table in manifest.tables {
            let TableInput::View(query) = table.1.input;
            let query = parse_sql(&query.sql).map_err(Error::SqlParseError)?;
            queries.insert(table.0, query);
        }
        Ok(SqlDataset { dataset, queries })
    }

    async fn load_dataset_inner(self: &Arc<Self>, dataset_name: &str) -> Result<Dataset, Error> {
        if let Some(dataset) = self.dataset_cache.read().unwrap().get(dataset_name) {
            return Ok(dataset.clone());
        }

        let (common, raw_dataset) = self.common_data_and_dataset(dataset_name).await?;
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
                let store_dataset = Arc::clone(&self).sql_dataset(value).await?.dataset;
                (store_dataset, None)
            }
            DatasetKind::Manifest => {
                let manifest = raw_dataset.to_manifest()?;
                let dataset = Dataset::from(manifest);
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
            .insert(dataset_name.to_string(), dataset.clone());

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

    pub async fn load_client(&self, dataset: &str) -> Result<impl BlockStreamer, DatasetError> {
        self.load_client_inner(dataset)
            .await
            .map_err(|e| (dataset, e).into())
    }

    async fn load_client_inner(&self, dataset_name: &str) -> Result<BlockStreamClient, Error> {
        let (common, raw_dataset) = self.common_data_and_dataset(dataset_name).await?;
        let value = raw_dataset.to_value()?;
        let kind = DatasetKind::from_str(&common.kind)?;

        let provider = self.find_provider(kind, common.network.clone()).await?;
        Ok(match kind {
            DatasetKind::EvmRpc => {
                BlockStreamClient::EvmRpc(evm_rpc_datasets::client(provider, common.network).await?)
            }
            DatasetKind::Firehose => BlockStreamClient::Firehose(
                firehose_datasets::Client::new(provider, common.network).await?,
            ),
            DatasetKind::Substreams => BlockStreamClient::Substreams(
                substreams_datasets::Client::new(provider, value, common.network).await?,
            ),
            DatasetKind::Sql | DatasetKind::Manifest => {
                // SQL and Manifest datasets don't have a client.
                return Err(Error::UnsupportedKind(common.kind));
            }
        })
    }

    async fn find_provider(
        &self,
        dataset_kind: DatasetKind,
        dataset_network: String,
    ) -> Result<toml::Value, Error> {
        let mut providers = self.all_providers().await?;
        // Shuffle to provide load balancing across providers.
        providers.shuffle(&mut rand::rng());
        for obj in providers {
            let location = obj.location.to_string();
            let (kind, network, provider) = self.kind_network_provider(&location).await?;
            if kind != dataset_kind || network != dataset_network {
                continue;
            }
            return Ok(provider);
        }
        Err(Error::ProviderNotFound {
            dataset_kind,
            network: dataset_network,
        })
    }

    async fn all_providers(&self) -> Result<Vec<ObjectMeta>, Error> {
        if let Some(providers) = self.providers_cache.read().unwrap().as_ref() {
            // Cache is already populated.
            return Ok(providers.clone());
        }
        let providers = self
            .providers_store()
            .list_all_shallow()
            .await
            .map_err(Error::FetchError)?;
        *self.providers_cache.write().unwrap() = Some(providers.clone());
        Ok(providers)
    }

    async fn kind_network_provider(
        &self,
        location: &str,
    ) -> Result<(DatasetKind, String, toml::Value), Error> {
        let raw = self.providers_store().get_string(location).await?;
        let toml_value: toml::Value = toml::from_str(&raw)?;
        let common_fields = ProviderDefsCommon::deserialize(toml_value.clone())?;
        let kind = DatasetKind::from_str(&common_fields.kind)?;
        Ok((kind, common_fields.network, toml_value))
    }

    /// Returns cached eth_call scalar UDF, otherwise loads the UDF and caches it.
    async fn eth_call_for_dataset(&self, dataset: &Dataset) -> Result<Option<ScalarUDF>, Error> {
        #[derive(Deserialize)]
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
    pub async fn ctx_for_sql(
        self: &Arc<Self>,
        query: &parser::Statement,
        env: QueryEnv,
    ) -> Result<QueryContext, DatasetError> {
        let (tables, _) = resolve_table_references(query, true).map_err(DatasetError::unknown)?;
        let function_names = all_function_names(query).map_err(DatasetError::unknown)?;

        let catalog = self
            .load_physical_catalog(tables, function_names, &env)
            .await?;
        QueryContext::for_catalog(catalog, env).map_err(DatasetError::unknown)
    }

    /// Looks up the datasets for the given table references and loads them into a catalog.
    pub async fn load_physical_catalog<'a>(
        self: &Arc<Self>,
        table_refs: impl IntoIterator<Item = TableReference>,
        function_names: impl IntoIterator<Item = String>,
        env: &QueryEnv,
    ) -> Result<Catalog, DatasetError> {
        let logical_catalog = self
            .load_logical_catalog(table_refs, function_names, &env.isolate_pool)
            .await?;

        let mut tables = Vec::new();
        for table in logical_catalog.tables {
            let physical_table = PhysicalTable::get_active(&table, self.metadata_db.clone())
                .await
                .map_err(DatasetError::unknown)?
                .ok_or(DatasetError::unknown(format!(
                    "Table {} has not been synced",
                    table,
                )))?;
            tables.push(physical_table.into());
        }
        Ok(Catalog::new(tables, logical_catalog.udfs))
    }

    /// Similar to `ctx_for_sql`, but only for planning and not execution. This does not require a
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
        let mut dataset_names = datasets_from_table_refs(table_refs.into_iter())?;
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
            let dataset = self.load_dataset(&dataset_name).await?;
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

            for table in Arc::new(dataset).resolved_tables() {
                resolved_tables.push(table);
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

    fn providers_store(&self) -> &Arc<Store> {
        &self.config.providers_store
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
#[derive(Deserialize, Serialize, JsonSchema)]
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
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
pub struct SerializableSchema(
    std::collections::HashMap<String, std::collections::HashMap<String, DataTypeJsonSchema>>,
);

/// All providers definitions must have a kind and network.
#[derive(Deserialize)]
struct ProviderDefsCommon {
    kind: String,
    network: String,
}

fn datasets_from_table_refs<'a>(
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

    async fn latest_block(&mut self, finalized: bool) -> Result<BlockNum, BoxError> {
        match self {
            Self::EvmRpc(client) => client.latest_block(finalized).await,
            Self::Firehose(client) => client.latest_block(finalized).await,
            Self::Substreams(client) => client.latest_block(finalized).await,
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
pub async fn resolve_blocks_table(
    dataset_store: &Arc<DatasetStore>,
    src_datasets: &BTreeSet<&str>,
    network: &str,
) -> Result<String, BoxError> {
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

    assert!(dataset.tables.iter().any(|t| t.name() == "blocks"));
    Ok(format!("{}.blocks", dataset.name))
}
