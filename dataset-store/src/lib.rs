pub mod sql_datasets;

use core::fmt;
use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
    sync::{Arc, RwLock},
};

use async_udf::functions::AsyncScalarUDF;
use common::{
    catalog::physical::{Catalog, PhysicalDataset},
    config::Config,
    evm::udfs::EthCall,
    manifest::{Manifest, TableInput},
    query_context::{self, parse_sql, PlanningContext, ResolvedTable, ResolvedTables},
    store::StoreError,
    BlockNum, BlockStreamer, BoxError, Dataset, DatasetWithProvider, QueryContext, Store,
};
use datafusion::{
    catalog::resolve_table_references,
    common::HashMap,
    execution::runtime_env::RuntimeEnv,
    logical_expr::ScalarUDF,
    sql::{parser, TableReference},
};
use futures::{future::BoxFuture, FutureExt as _, TryFutureExt as _};
use metadata_db::MetadataDb;
use serde::Deserialize;
use sql_datasets::SqlDataset;
use thiserror::Error;
use tokio::sync::mpsc;
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

    #[error("unsupported table name: {0}")]
    UnsupportedName(BoxError),

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

    #[error("{0}")]
    Unknown(BoxError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    pub metadata_db: Option<MetadataDb>,
    // Cache maps dataset name to eth_call UDF.
    eth_call_cache: Arc<RwLock<HashMap<String, ScalarUDF>>>,
}

impl DatasetStore {
    pub fn new(config: Arc<Config>, metadata_db: Option<MetadataDb>) -> Arc<Self> {
        Arc::new(Self {
            config,
            metadata_db,
            eth_call_cache: Default::default(),
        })
    }

    pub async fn load_dataset(
        self: &Arc<Self>,
        dataset: &str,
    ) -> Result<DatasetWithProvider, DatasetError> {
        self.clone()
            .load_dataset_inner(dataset)
            .await
            .map_err(|e| (dataset, e).into())
    }

    pub async fn all_datasets(self: &Arc<Self>) -> Result<Vec<DatasetWithProvider>, DatasetError> {
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

    async fn load_dataset_inner(
        self: Arc<Self>,
        dataset_name: &str,
    ) -> Result<DatasetWithProvider, Error> {
        let (kind, raw_dataset) = self.kind_and_dataset(dataset_name).await?;

        let dataset = match kind {
            DatasetKind::EvmRpc => {
                let dataset_toml: toml::Value = toml::from_str(&raw_dataset)?;
                evm_rpc_datasets::dataset(dataset_toml)?
            }
            DatasetKind::Firehose => {
                let dataset_toml: toml::Value = toml::from_str(&raw_dataset)?;
                firehose_datasets::evm::dataset(dataset_toml)?
            }
            DatasetKind::Substreams => {
                let dataset_toml: toml::Value = toml::from_str(&raw_dataset)?;
                substreams_datasets::dataset(dataset_toml).await?
            }
            DatasetKind::Sql => {
                let dataset_toml: toml::Value = toml::from_str(&raw_dataset)?;
                DatasetWithProvider {
                    dataset: self.sql_dataset(dataset_toml).await?.dataset,
                    provider: None,
                }
            }
            DatasetKind::Manifest => {
                let filename = format!("{}.json", dataset_name);
                let raw_manifest = self.dataset_defs_store().get_string(filename).await?;
                let manifest: Manifest =
                    serde_json::from_str(&raw_manifest).map_err(Error::ManifestError)?;
                DatasetWithProvider {
                    dataset: Dataset::from(manifest.clone()),
                    provider: None,
                }
            }
        };

        Ok(dataset)
    }

    #[instrument(skip(self), err)]
    async fn load_sql_dataset_inner(
        self: Arc<Self>,
        dataset_name: &str,
    ) -> Result<SqlDataset, Error> {
        let (kind, raw_dataset) = self.kind_and_dataset(dataset_name).await?;
        if kind != DatasetKind::Sql {
            return Err(Error::UnsupportedKind(kind.to_string()));
        }

        let dataset_toml: toml::Value = toml::from_str(&raw_dataset)?;

        self.sql_dataset(dataset_toml).await
    }

    pub async fn load_client(&self, dataset: &str) -> Result<impl BlockStreamer, DatasetError> {
        self.load_client_inner(dataset)
            .await
            .map_err(|e| (dataset, e).into())
    }

    #[instrument(skip(self), err)]
    async fn load_client_inner(&self, dataset_name: &str) -> Result<impl BlockStreamer, Error> {
        #[derive(Clone)]
        pub enum BlockStreamClient {
            EvmRpc(evm_rpc_datasets::JsonRpcClient),
            Firehose(firehose_datasets::Client),
            Substreams(substreams_datasets::Client),
        }

        impl BlockStreamer for BlockStreamClient {
            async fn block_stream(
                self,
                start_block: BlockNum,
                end_block: BlockNum,
                tx: mpsc::Sender<common::DatasetRows>,
            ) -> Result<(), BoxError> {
                match self {
                    Self::EvmRpc(client) => client.block_stream(start_block, end_block, tx).await,
                    Self::Firehose(client) => client.block_stream(start_block, end_block, tx).await,
                    Self::Substreams(client) => {
                        client.block_stream(start_block, end_block, tx).await
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

        let (kind, raw_dataset) = self.kind_and_dataset(dataset_name).await?;
        let toml: toml::Value = toml::from_str(&raw_dataset)?;

        match kind {
            DatasetKind::EvmRpc => {
                let client = evm_rpc_datasets::client(toml, &self.providers_store()).await?;
                Ok(BlockStreamClient::EvmRpc(client))
            }
            DatasetKind::Firehose => {
                let client = firehose_datasets::Client::new(toml, self.providers_store()).await?;
                Ok(BlockStreamClient::Firehose(client))
            }
            DatasetKind::Substreams => {
                let client = substreams_datasets::Client::new(toml, self.providers_store()).await?;
                Ok(BlockStreamClient::Substreams(client))
            }
            DatasetKind::Sql | DatasetKind::Manifest => {
                Err(Error::UnsupportedKind(kind.to_string()))
            }
        }
    }

    async fn kind_and_dataset(&self, dataset_name: &str) -> Result<(DatasetKind, String), Error> {
        use Error::*;

        let (raw, common_fields) = {
            let filename = format!("{}.toml", dataset_name);
            match self.dataset_defs_store().get_string(filename).await {
                Ok(raw_dataset) => {
                    let toml_value: toml::Value = toml::from_str(&raw_dataset)?;
                    (raw_dataset, DatasetDefsCommon::deserialize(toml_value)?)
                }
                Err(e) if e.is_not_found() => {
                    // If it's not a TOML file, it might be a JSON file.
                    // Ideally we migrate everything to JSON, but this is a stopgap.
                    let filename = format!("{}.json", dataset_name);
                    let raw_dataset = self.dataset_defs_store().get_string(filename).await?;
                    let json_value: serde_json::Value = serde_json::from_str(&raw_dataset)?;
                    (raw_dataset, DatasetDefsCommon::deserialize(json_value)?)
                }
                Err(e) => return Err(e.into()),
            }
        };
        if common_fields.name != dataset_name {
            return Err(NameMismatch(common_fields.name, dataset_name.to_string()));
        }
        let kind = DatasetKind::from_str(&common_fields.kind)?;

        Ok((kind, raw))
    }

    /// Returns cached eth_call scalar UDF, otherwise loads the UDF and caches it.
    async fn eth_call_for_dataset(
        &self,
        DatasetWithProvider { dataset, provider }: &DatasetWithProvider,
    ) -> Result<Option<ScalarUDF>, Error> {
        #[derive(Deserialize)]
        struct EvmRpcProvider {
            url: Url,
        }

        if dataset.kind != "evm-rpc" {
            return Ok(None);
        }

        // Check if we already have the provider cached.
        if let Some(udf) = self.eth_call_cache.read().unwrap().get(&dataset.name) {
            return Ok(Some(udf.clone()));
        }

        // Load the provider from the dataset definition.
        if let Some(provider_location) = provider {
            let provider = self
                .providers_store()
                .get_string(provider_location.clone())
                .await?;
            let provider: EvmRpcProvider = if provider_location.ends_with(".toml") {
                toml::from_str(&provider)?
            } else if provider_location.ends_with(".json") {
                serde_json::from_str(&provider)?
            } else {
                return Err(Error::Unknown(
                    format!("unknown provider format: {provider_location}").into(),
                ));
            };
            // Cache the provider.
            let provider = alloy::providers::ProviderBuilder::new().on_http(provider.url);
            let udf = AsyncScalarUDF::new(Arc::new(EthCall::new(&dataset.name, provider)))
                .into_scalar_udf();
            let udf = Arc::into_inner(udf).unwrap();
            self.eth_call_cache
                .write()
                .unwrap()
                .insert(dataset.name.clone(), udf.clone());
            return Ok(Some(udf));
        }

        Ok(None)
    }

    /// Creates a `QueryContext` for a SQL query, this will infer and load any dependencies. The
    /// procedure is:
    ///
    /// 1. Collect table references in the query.
    /// 2. Assume that in `foo.bar`, `foo` is a dataset name.
    /// 3. Look up the dataset names in the configured dataset store.
    /// 4. Collect the datasets into a catalog.
    pub async fn ctx_for_sql(
        self: Arc<Self>,
        query: &parser::Statement,
        env: Arc<RuntimeEnv>,
    ) -> Result<QueryContext, DatasetError> {
        let (tables, _) = resolve_table_references(query, true).map_err(DatasetError::unknown)?;
        let catalog = self.load_catalog_for_table_refs(tables.iter()).await?;
        QueryContext::for_catalog(catalog, env.clone()).map_err(DatasetError::unknown)
    }

    /// Looks up the datasets for the given table references and loads them into a catalog.
    pub async fn load_catalog_for_table_refs<'a>(
        self: Arc<Self>,
        table_refs: impl Iterator<Item = &'a TableReference>,
    ) -> Result<Catalog, DatasetError> {
        let dataset_names = datasets_from_table_refs(table_refs)?;
        let mut catalog = Catalog::empty();
        for dataset_name in dataset_names {
            let dataset = self.load_dataset(&dataset_name).await?;

            // We currently assume all datasets live in the same `data_store`.
            let physical_dataset = PhysicalDataset::from_dataset_at(
                dataset.dataset.clone(),
                self.config.data_store.clone(),
                self.metadata_db.as_ref(),
                true,
            )
            .await
            .map_err(|e| (dataset_name.clone(), Error::Unknown(e)))?;
            catalog.add_dataset(physical_dataset);

            // Create the `eth_call` UDF for the dataset.
            let udf = self
                .eth_call_for_dataset(&dataset)
                .await
                .map_err(|e| (dataset_name.clone(), e))?;
            if let Some(udf) = udf {
                catalog.add_udf(udf);
            }
        }
        Ok(catalog)
    }

    /// Initial catalog including UDFs for all datasets.
    pub async fn initial_catalog(self: &Arc<Self>) -> Result<Catalog, DatasetError> {
        let mut catalog = Catalog::empty();
        for dataset in self.all_datasets().await? {
            let udf = self
                .eth_call_for_dataset(&dataset)
                .await
                .map_err(|e| (dataset.dataset.name.clone(), e))?;
            if let Some(udf) = udf {
                catalog.add_udf(udf);
            }
        }
        Ok(catalog)
    }

    /// Similar to `ctx_for_sql`, but only for planning and not execution. This does not require a
    /// physical location to exist for the dataset views.
    pub async fn planning_ctx_for_sql(
        self: Arc<Self>,
        query: &parser::Statement,
    ) -> Result<PlanningContext, DatasetError> {
        let (tables, _) = resolve_table_references(query, true).map_err(DatasetError::unknown)?;
        let resolved_tables = self.load_resolved_tables(tables.iter()).await?;
        Ok(PlanningContext::new(resolved_tables))
    }

    /// Looks up the datasets for the given table references and creates resolved tables. Create
    /// UDFs specific to the referenced datasets.
    async fn load_resolved_tables(
        self: Arc<Self>,
        table_refs: impl Iterator<Item = &TableReference>,
    ) -> Result<ResolvedTables, DatasetError> {
        let dataset_names = datasets_from_table_refs(table_refs)?;
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
            for table in dataset.dataset.tables {
                resolved_tables.push(ResolvedTable::new(dataset_name.clone(), table));
            }
        }
        Ok(ResolvedTables {
            tables: resolved_tables,
            udfs,
        })
    }

    /// Each `.sql` file in the directory with the same name as the dataset will be loaded as a table.
    fn sql_dataset(
        self: Arc<Self>,
        dataset_def: toml::Value,
    ) -> BoxFuture<'static, Result<SqlDataset, Error>> {
        sql_datasets::dataset(self, dataset_def)
            .map_err(Error::SqlDatasetError)
            .boxed()
    }

    fn providers_store(&self) -> &Arc<Store> {
        &self.config.providers_store
    }

    fn dataset_defs_store(&self) -> &Arc<Store> {
        &self.config.dataset_defs_store
    }
}

/// All dataset definitions must have a kind and name. The name must match the filename.
#[derive(Deserialize)]
struct DatasetDefsCommon {
    kind: String,
    name: String,
}

fn datasets_from_table_refs<'a>(
    table_refs: impl Iterator<Item = &'a TableReference>,
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
