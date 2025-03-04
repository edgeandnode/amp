pub mod sql_datasets;

use core::fmt;
use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
    sync::Arc,
};

use common::{
    catalog::physical::Catalog,
    config::Config,
    manifest::{Manifest, TableInput},
    query_context::{self, parse_sql, PlanningContext, ResolvedTable},
    store::StoreError,
    BlockNum, BlockStreamer, BoxError, Dataset, QueryContext, Store,
};
use datafusion::{
    catalog_common::resolve_table_references,
    execution::runtime_env::RuntimeEnv,
    sql::{parser, TableReference},
};
use futures::{future::BoxFuture, FutureExt as _, TryFutureExt as _};
use metadata_db::MetadataDb;
use serde::Deserialize;
use sql_datasets::SqlDataset;
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::{error, instrument};

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

pub struct DatasetStore {
    config: Arc<Config>,
    pub metadata_db: Option<MetadataDb>,
}

impl DatasetStore {
    pub fn new(config: Arc<Config>, metadata_db: Option<MetadataDb>) -> Arc<Self> {
        Arc::new(Self {
            config,
            metadata_db,
        })
    }

    pub async fn load_dataset(self: &Arc<Self>, dataset: &str) -> Result<Dataset, DatasetError> {
        self.clone()
            .load_dataset_inner(dataset)
            .await
            .map_err(|e| (dataset, e).into())
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

    async fn load_dataset_inner(self: Arc<Self>, dataset_name: &str) -> Result<Dataset, Error> {
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
                self.sql_dataset(dataset_toml).await?.dataset
            }
            DatasetKind::Manifest => {
                let filename = format!("{}.json", dataset_name);
                let raw_manifest = self.dataset_defs_store().get_string(filename).await?;
                let manifest: Manifest =
                    serde_json::from_str(&raw_manifest).map_err(Error::ManifestError)?;
                Dataset::from(manifest)
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

        /// All dataset definitions must have a kind and name. The name must match the filename.
        #[derive(Deserialize)]
        struct CommonFields {
            kind: String,
            name: String,
        }

        let (raw, common_fields) = {
            let filename = format!("{}.toml", dataset_name);
            match self.dataset_defs_store().get_string(filename).await {
                Ok(raw_dataset) => {
                    let toml_value: toml::Value = toml::from_str(&raw_dataset)?;
                    (raw_dataset, CommonFields::deserialize(toml_value)?)
                }
                Err(e) if e.is_not_found() => {
                    // If it's not a TOML file, it might be a JSON file.
                    // Ideally we migrate everything to JSON, but this is a stopgap.
                    let filename = format!("{}.json", dataset_name);
                    let raw_dataset = self.dataset_defs_store().get_string(filename).await?;
                    let json_value: serde_json::Value = serde_json::from_str(&raw_dataset)?;
                    (raw_dataset, CommonFields::deserialize(json_value)?)
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
            catalog
                .register(
                    &dataset,
                    self.config.data_store.clone(),
                    self.metadata_db.as_ref(),
                )
                .await
                .map_err(|e| (dataset_name, Error::Unknown(e)))?;
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

    /// Looks up the datasets for the given table references and creates resolved tables.
    async fn load_resolved_tables(
        self: Arc<Self>,
        table_refs: impl Iterator<Item = &TableReference>,
    ) -> Result<Vec<ResolvedTable>, DatasetError> {
        let dataset_names = datasets_from_table_refs(table_refs)?;
        let mut resolved_tables = Vec::new();
        for dataset_name in dataset_names {
            let dataset = self.load_dataset(&dataset_name).await?;
            for table in dataset.tables {
                resolved_tables.push(ResolvedTable::new(dataset_name.clone(), table));
            }
        }
        Ok(resolved_tables)
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
