use std::{io, str::FromStr};

use common::BoxError;
use dataset_store::SerializableSchema;
use substreams_datasets;

pub async fn run<T: io::Write>(
    network: String,
    kind: String,
    name: String,
    manifest: Option<String>,
    module: Option<String>,
    w: &mut T,
) -> Result<(), BoxError> {
    // Validate dataset kind.
    let dataset_kind = dataset_store::DatasetKind::from_str(&kind)?;

    let schema: SerializableSchema = match dataset_kind {
        dataset_store::DatasetKind::EvmRpc => evm_rpc_datasets::tables::all(&network).into(),
        dataset_store::DatasetKind::EthBeacon => {
            vec![eth_beacon_datasets::blocks::table(network.to_string())].into()
        }
        dataset_store::DatasetKind::Firehose => {
            firehose_datasets::evm::tables::all(&network).into()
        }
        dataset_store::DatasetKind::Substreams => {
            let (Some(manifest), Some(module)) = (manifest, module) else {
                return Err(
                    "`manifest` and `module` arguments are required for `DatasetKind::Substreams`"
                        .into(),
                );
            };
            let dataset_def = substreams_datasets::dataset::DatasetDef {
                kind: kind.clone(),
                network: network.clone(),
                name: name.clone(),
                manifest,
                module,
            };
            substreams_datasets::tables(dataset_def)
                .await
                .map(Into::into)?
        }
        dataset_store::DatasetKind::Sql => {
            return Err("`DatasetKind::Sql` doesn't support dataset generation".into());
        }
        dataset_store::DatasetKind::Manifest => {
            return Err("`DatasetKind::Manifest` doesn't support dataset generation".into());
        }
    };
    let dataset = serde_json::to_vec(&dataset_store::DatasetDefsCommon {
        network,
        kind,
        name,
        schema: Some(schema),
    })?;
    w.write_all(&dataset)?;

    Ok(())
}
