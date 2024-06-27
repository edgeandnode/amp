use std::sync::Arc;

use common::{config::Config, store::StoreError, BlockStreamer, BoxError, Dataset, Store};
use thiserror::Error;
use tokio::sync::mpsc;

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to fetch: {0}")]
    FetchError(#[from] StoreError),

    #[error("TOML parse error: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("kind parse error: {0}")]
    KindParseError(&'static str),

    #[error("unsupported dataset kind '{0}'")]
    UnsupportedKind(String),

    #[error("firehose error: {0}")]
    FirehoseError(#[from] firehose_datasets::Error),
}

impl Error {
    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            Self::FetchError(StoreError::ObjectStore(
                object_store::Error::NotFound { .. }
            ))
        )
    }
}

pub struct DatasetStore {
    dataset_defs_store: Arc<Store>,
    providers_store: Arc<Store>,
}

impl DatasetStore {
    pub fn new(config: &Config) -> Self {
        let dataset_defs_store = config.dataset_defs_store.clone();
        let providers_store = config.providers_store.clone();

        Self {
            dataset_defs_store,
            providers_store,
        }
    }

    pub async fn load_dataset(&self, dataset_name: &str) -> Result<Dataset, Error> {
        let (kind, dataset_toml) = self.kind_and_dataset(&dataset_name).await?;

        let dataset = match kind.as_str() {
            firehose_datasets::DATASET_KIND => firehose_datasets::evm::dataset(dataset_toml)?,
            substreams_datasets::DATASET_KIND => substreams_datasets::dataset(dataset_toml).await?,
            _ => return Err(Error::UnsupportedKind(kind)),
        };

        Ok(dataset)
    }

    pub async fn load_client(&self, dataset_name: &str) -> Result<impl BlockStreamer, Error> {
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
}
