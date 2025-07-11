use std::{io, str::FromStr};

use common::BoxError;
use dataset_store::vec_table_to_schema;

pub fn run<T: io::Write>(
    network: String,
    kind: String,
    name: String,
    w: &mut T,
) -> Result<(), BoxError> {
    // Validate dataset kind.
    let dataset_kind = dataset_store::DatasetKind::from_str(&kind)?;

    let schema = match dataset_kind {
        dataset_store::DatasetKind::EvmRpc => {
            Some(vec_table_to_schema(evm_rpc_datasets::tables::all(&network)))
        }
        dataset_store::DatasetKind::Firehose => Some(vec_table_to_schema(
            firehose_datasets::evm::tables::all(&network),
        )),
        // TODO
        dataset_store::DatasetKind::Substreams => todo!(),
        dataset_store::DatasetKind::Sql => todo!(),
        dataset_store::DatasetKind::Manifest => todo!(),
    };
    let dataset = serde_json::to_vec(&dataset_store::DatasetDefsCommon {
        network,
        kind,
        name,
        schema,
    })?;
    w.write_all(&dataset)?;

    Ok(())
}
