pub mod client;
pub mod tables;
pub mod transform;

pub use client::Client;
use dataset::DatasetDef;
pub use dataset::DATASET_KIND;

mod dataset;
mod proto;

use common::Dataset;
use firehose_datasets::Error;
use proto::sf::substreams::v1::Package;
use tables::Tables;

/// Does an network request to fetch the spkg from the URL in the `manifest` config key.
pub async fn dataset(dataset_cfg: toml::Value) -> Result<Dataset, Error> {
    let dataset_def: DatasetDef = dataset_cfg.try_into()?;
    let package = Package::from_url(dataset_def.manifest.as_str()).await?;

    let tables = Tables::from_package(&package, &dataset_def.module)
        .map_err(|_| Error::AssertFail("failed to build tables from spkg".into()))?;

    Ok(Dataset {
        kind: dataset_def.kind,
        name: dataset_def.name,
        tables: tables.tables,
    })
}
