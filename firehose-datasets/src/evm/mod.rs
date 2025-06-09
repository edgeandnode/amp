pub use crate::proto::sf::ethereum::r#type::v2 as pbethereum;
use crate::{dataset::DatasetDef, Error};
pub mod pb_to_rows;
pub mod tables;

use common::{Dataset, DatasetWithProvider};

pub fn dataset(
    dataset_cfg: toml::Value,
    provider: toml::Value,
) -> Result<DatasetWithProvider, Error> {
    let dataset_def: DatasetDef = dataset_cfg.try_into()?;
    Ok(DatasetWithProvider {
        dataset: Dataset {
            kind: dataset_def.kind,
            name: dataset_def.name,
            tables: tables::all(&dataset_def.network),
            functions: vec![],
        },
        provider: Some(provider),
    })
}

/// Automatically generate a README.md file with the schema whenever tests are executed.
#[tokio::test]
async fn print_schema_to_readme() {
    fs_err::write(
        "src/evm/README.md",
        common::catalog::schema_to_markdown(tables::all("test_network")).await,
    )
    .unwrap();
}
