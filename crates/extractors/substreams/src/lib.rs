use common::Dataset;
use datasets_common::value::ManifestValue;
use firehose_datasets::Error;

pub mod client;
pub mod tables;
pub mod transform;

use dataset::Manifest;
pub mod dataset;
mod dataset_kind;
#[allow(dead_code)]
mod proto;

pub use self::{
    client::Client,
    dataset_kind::{DATASET_KIND, SubstreamsDatasetKind, SubstreamsDatasetKindError},
};
use self::{proto::sf::substreams::v1::Package, tables::Tables};

/// Does a network request to fetch the spkg from the URL in the `manifest` config key.
pub async fn dataset(value: ManifestValue) -> Result<Dataset, Error> {
    let manifest: Manifest = value.try_into_manifest()?;
    let package = Package::from_url(manifest.manifest.as_str()).await?;
    let tables = Tables::from_package(&package, &manifest.module)
        .map_err(|_| Error::AssertFail("failed to build tables from spkg".into()))?;

    Ok(Dataset {
        name: manifest.name,
        kind: manifest.kind.to_string(),
        version: Some(manifest.version),
        start_block: None,
        tables: tables.tables,
        functions: vec![],
        network: manifest.network,
    })
}

pub async fn tables(dataset_def: Manifest) -> Result<Vec<common::catalog::logical::Table>, Error> {
    let package = Package::from_url(dataset_def.manifest.as_str()).await?;
    let tables = Tables::from_package(&package, &dataset_def.module)
        .map_err(|_| Error::AssertFail("failed to build tables from spkg".into()))?;

    Ok(tables.tables)
}
