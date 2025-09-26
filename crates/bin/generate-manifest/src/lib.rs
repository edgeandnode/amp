use std::io;

use common::{BoxError, manifest::common::schema_from_tables};
use datasets_common::{
    manifest::{Manifest, Schema},
    name::Name,
};

pub async fn run<T: io::Write>(
    network: String,
    kind: String,
    name: String,
    manifest: Option<String>,
    module: Option<String>,
    w: &mut T,
) -> Result<(), BoxError> {
    // Validate dataset name and kind
    let name = name.parse::<Name>()?;
    let kind = kind.parse()?;

    let schema: Schema = match kind {
        dataset_store::DatasetKind::EvmRpc => {
            schema_from_tables(evm_rpc_datasets::tables::all(&network))
        }
        dataset_store::DatasetKind::EthBeacon => {
            schema_from_tables(eth_beacon_datasets::all_tables(network.clone()))
        }
        dataset_store::DatasetKind::Firehose => {
            schema_from_tables(firehose_datasets::evm::tables::all(&network))
        }
        dataset_store::DatasetKind::Substreams => {
            let (Some(manifest), Some(module)) = (manifest, module) else {
                return Err(
                    "`manifest` and `module` arguments are required for `DatasetKind::Substreams`"
                        .into(),
                );
            };
            let dataset_def = substreams_datasets::dataset::DatasetDef {
                kind: kind.to_string(),
                network: network.clone(),
                name: name.to_string(),
                manifest,
                module,
            };
            schema_from_tables(substreams_datasets::tables(dataset_def).await?)
        }
        dataset_store::DatasetKind::Sql => {
            return Err("`DatasetKind::Sql` doesn't support dataset generation".into());
        }
        dataset_store::DatasetKind::Manifest => {
            return Err("`DatasetKind::Manifest` doesn't support dataset generation".into());
        }
    };
    let dataset = serde_json::to_vec(&Manifest {
        name,
        version: Default::default(),
        kind: kind.to_string(),
        network,
        schema: Some(schema),
    })?;
    w.write_all(&dataset)?;

    Ok(())
}
