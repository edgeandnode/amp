use std::{io::Write, str::FromStr};

use common::BoxError;

pub fn run(network: String, kind: String, name: String) -> Result<(), BoxError> {
    // Validate dataset kind.
    let _ = dataset_store::DatasetKind::from_str(&kind)?;

    let mut json_file = std::fs::File::create(format!("{}.json", kind))?;
    let dataset = serde_json::to_vec(&dataset_store::DatasetDefsCommon {
        network,
        kind,
        name,
    })?;
    json_file.write_all(&dataset)?;

    Ok(())
}
