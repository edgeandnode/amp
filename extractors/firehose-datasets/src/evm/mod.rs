pub use crate::proto::sf::ethereum::r#type::v2 as pbethereum;
use crate::{Error, dataset::DatasetDef};
pub mod pb_to_rows;
pub mod tables;

use common::Dataset;

pub fn dataset(dataset_cfg: common::DatasetValue) -> Result<Dataset, Error> {
    let dataset_def = DatasetDef::from_value(dataset_cfg)?;
    Ok(Dataset {
        kind: dataset_def.kind,
        name: dataset_def.name,
        version: None,
        start_block: Some(dataset_def.start_block),
        tables: tables::all(&dataset_def.network),
        network: dataset_def.network,
        functions: vec![],
    })
}

/// Automatically generate a README.md file with the schema whenever tests are executed.
#[tokio::test]
async fn print_schema_to_readme() {
    fs_err::write(
        "../../docs/schemas/firehose-evm.md",
        common::catalog::schema_to_markdown(tables::all("test_network")).await,
    )
    .unwrap();
}
