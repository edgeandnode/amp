use core::fmt;
use std::{collections::BTreeSet, sync::Arc};

use common::{
    catalog::physical::Catalog, config::Config, store::StoreError, BlockStreamer, BoxError,
    Dataset, Store,
};
use datafusion::sql::TableReference;
use thiserror::Error;
use tokio::sync::mpsc;

#[derive(Debug, Error)]
enum Error {
    #[error("failed to fetch: {0}")]
    FetchError(#[from] StoreError),

    #[error("TOML parse error: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("kind parse error: {0}")]
    KindParseError(&'static str),

    #[error("unsupported dataset kind '{0}'")]
    UnsupportedKind(String),

    #[error("unsupported table name: {0}")]
    UnsupportedName(BoxError),

    #[error("firehose error: {0}")]
    FirehoseError(#[from] firehose_datasets::Error),
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

    pub fn is_not_found(&self) -> bool {
        matches!(
            self.error,
            Error::FetchError(StoreError::ObjectStore(
                object_store::Error::NotFound { .. }
            ))
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
        match &self.dataset {
            Some(dataset) => write!(f, "error with dataset '{}': {}", dataset, self.error),
            None => write!(f, "{}", self.error),
        }
    }
}

pub struct DatasetStore {
    dataset_defs_store: Arc<Store>,
    providers_store: Arc<Store>,
    data_store: Arc<Store>,
}

impl DatasetStore {
    pub fn new(config: &Config) -> Self {
        let dataset_defs_store = config.dataset_defs_store.clone();
        let providers_store = config.providers_store.clone();
        let data_store = config.data_store.clone();

        Self {
            dataset_defs_store,
            providers_store,
            data_store,
        }
    }

    pub async fn load_dataset(&self, dataset: &str) -> Result<Dataset, DatasetError> {
        self.load_dataset_inner(dataset)
            .await
            .map_err(|e| (dataset, e).into())
    }

    async fn load_dataset_inner(&self, dataset_name: &str) -> Result<Dataset, Error> {
        let (kind, dataset_toml) = self.kind_and_dataset(&dataset_name).await?;

        let dataset = match kind.as_str() {
            firehose_datasets::DATASET_KIND => firehose_datasets::evm::dataset(dataset_toml)?,
            substreams_datasets::DATASET_KIND => substreams_datasets::dataset(dataset_toml).await?,
            _ => return Err(Error::UnsupportedKind(kind)),
        };

        Ok(dataset)
    }

    pub async fn load_client(&self, dataset: &str) -> Result<impl BlockStreamer, DatasetError> {
        self.load_client_inner(dataset)
            .await
            .map_err(|e| (dataset, e).into())
    }

    async fn load_client_inner(&self, dataset_name: &str) -> Result<impl BlockStreamer, Error> {
        #[derive(Clone)]
        pub enum BlockStreamClient {
            Firehose(firehose_datasets::Client),
            Substreams(substreams_datasets::Client),
        }

        impl BlockStreamer for BlockStreamClient {
            async fn block_stream(
                self,
                start_block: u64,
                end_block: u64,
                tx: mpsc::Sender<common::DatasetRows>,
            ) -> Result<(), BoxError> {
                match self {
                    Self::Firehose(client) => client.block_stream(start_block, end_block, tx).await,
                    Self::Substreams(client) => {
                        client.block_stream(start_block, end_block, tx).await
                    }
                }
            }
        }

        let (kind, toml) = self.kind_and_dataset(dataset_name).await?;

        match kind.as_str() {
            firehose_datasets::DATASET_KIND => {
                let client = firehose_datasets::Client::new(toml, &self.providers_store).await?;
                Ok(BlockStreamClient::Firehose(client))
            }
            substreams_datasets::DATASET_KIND => {
                let client = substreams_datasets::Client::new(toml, &self.providers_store).await?;
                Ok(BlockStreamClient::Substreams(client))
            }
            _ => Err(Error::UnsupportedKind(kind)),
        }
    }

    async fn kind_and_dataset(&self, dataset_name: &str) -> Result<(String, toml::Value), Error> {
        use Error::*;

        let filename = format!("{}.toml", dataset_name);
        let raw_dataset = self.dataset_defs_store.get_string(filename).await?;
        let dataset_def: toml::Value = toml::from_str(&raw_dataset)?;
        let kind = dataset_def
            .get("kind")
            .ok_or(KindParseError("missing 'kind' field in dataset definiton"))?
            .as_str()
            .ok_or(KindParseError("expected 'kind' field to be a string"))?;
        Ok((kind.to_string(), dataset_def))
    }

    /// Looks up the datasets for the given table references and loads them into a catalog.
    ///
    /// The
    pub async fn load_catalog_for_table_refs(
        &self,
        table_refs: impl IntoIterator<Item = TableReference>,
    ) -> Result<Catalog, DatasetError> {
        let dataset_names = datasets_from_table_refs(table_refs)?;
        let mut catalog = Catalog::empty();
        for dataset_name in dataset_names {
            let dataset = self.load_dataset(&dataset_name).await?;

            // We currently assume all datasets live in the same `data_store`.
            catalog
                .register(&dataset, self.data_store.clone())
                .map_err(|e| (dataset_name, Error::UnsupportedName(e)))?;
        }
        Ok(catalog)
    }
}

fn datasets_from_table_refs(
    table_refs: impl IntoIterator<Item = TableReference>,
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
