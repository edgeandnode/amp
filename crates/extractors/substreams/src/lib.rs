pub mod client;
pub mod tables;
pub mod transform;

pub use client::Client;
pub use dataset::DATASET_KIND;
use dataset::DatasetDef;

pub mod dataset;
mod proto;

use common::Dataset;
use firehose_datasets::Error;
use proto::sf::substreams::v1::Package;
use tables::Tables;

/// Does an network request to fetch the spkg from the URL in the `manifest` config key.
pub async fn dataset(dataset_cfg: common::DatasetValue) -> Result<Dataset, Error> {
    let dataset_def = DatasetDef::from_value(dataset_cfg)?;
    let package = Package::from_url(dataset_def.manifest.as_str()).await?;
    let tables = Tables::from_package(&package, &dataset_def.module)
        .map_err(|_| Error::AssertFail("failed to build tables from spkg".into()))?;

    Ok(Dataset {
        kind: dataset_def.kind,
        name: dataset_def.name,
        version: None,
        start_block: None,
        tables: tables.tables,
        functions: vec![],
        network: dataset_def.network,
    })
}

pub async fn tables(
    dataset_def: DatasetDef,
) -> Result<Vec<common::catalog::logical::Table>, Error> {
    let package = Package::from_url(dataset_def.manifest.as_str()).await?;
    let tables = Tables::from_package(&package, &dataset_def.module)
        .map_err(|_| Error::AssertFail("failed to build tables from spkg".into()))?;

    Ok(tables.tables)
}
