use std::{io, str::FromStr};

use common::BoxError;

pub fn run<T: io::Write>(
    network: String,
    kind: String,
    name: String,
    w: &mut T,
) -> Result<(), BoxError> {
    // Validate dataset kind.
    let _ = dataset_store::DatasetKind::from_str(&kind)?;

    let dataset = serde_json::to_vec(&dataset_store::DatasetDefsCommon {
        network,
        kind,
        name,
    })?;
    w.write_all(&dataset)?;

    Ok(())
}
