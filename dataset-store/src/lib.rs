use common::{BlockStreamer, BoxError, Dataset, Store};
use tokio::sync::mpsc;

pub async fn load_dataset(dataset_name: &str, dataset_store: &Store) -> Result<Dataset, BoxError> {
    let (kind, dataset_toml) = kind_and_dataset(&dataset_name, dataset_store).await?;

    let dataset = match kind.as_str() {
        firehose_datasets::DATASET_KIND => firehose_datasets::evm::dataset(dataset_toml)?,
        substreams_datasets::DATASET_KIND => substreams_datasets::dataset(dataset_toml).await?,
        _ => return Err(format!("unsupported dataset kind '{}'", kind).into()),
    };

    Ok(dataset)
}

pub async fn load_client(
    dataset_name: &str,
    dataset_store: &Store,
    provider_store: &Store,
) -> Result<impl BlockStreamer, BoxError> {
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
                Self::Substreams(client) => client.block_stream(start_block, end_block, tx).await,
            }
        }
    }

    let (kind, dataset_toml) = kind_and_dataset(dataset_name, dataset_store).await?;

    match kind.as_str() {
        firehose_datasets::DATASET_KIND => {
            let client = firehose_datasets::Client::new(dataset_toml, provider_store).await?;
            Ok(BlockStreamClient::Firehose(client))
        }
        substreams_datasets::DATASET_KIND => {
            let client = substreams_datasets::Client::new(dataset_toml, provider_store).await?;
            Ok(BlockStreamClient::Substreams(client))
        }
        _ => Err(format!("unsupported dataset kind '{}'", kind).into()),
    }
}

async fn kind_and_dataset(
    dataset_name: &str,
    dataset_store: &Store,
) -> Result<(String, toml::Value), BoxError> {
    let filename = format!("{}.toml", dataset_name);
    let raw_dataset = dataset_store.get_string(filename).await?;
    let dataset_def: toml::Value = toml::from_str(&raw_dataset)?;
    let kind = dataset_def
        .get("kind")
        .ok_or("missing 'kind' field in dataset definiton")?
        .as_str()
        .ok_or("expected 'kind' field to be a string")?;
    Ok((kind.to_string(), dataset_def))
}
