use std::io;

use common::{
    BoxError,
    manifest::common::{Manifest, Name, Schema},
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
        dataset_store::DatasetKind::EvmRpc => evm_rpc_datasets::tables::all(&network).into(),
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
                kind: kind.to_string(),
                network: network.clone(),
                name: name.to_string(),
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
    let dataset = serde_json::to_vec(&Manifest {
        network,
        kind: kind.to_string(),
        name,
        schema: Some(schema),
    })?;
    w.write_all(&dataset)?;

    Ok(())
}
