pub use crate::proto::sf::ethereum::r#type::v2 as pbethereum;
use crate::{Error, dataset::Manifest};
pub mod pb_to_rows;
pub mod tables;

use common::Dataset;
use datasets_common::value::ManifestValue;

pub fn dataset(value: ManifestValue) -> Result<Dataset, Error> {
    let manifest: Manifest = value.try_into_manifest()?;
    Ok(Dataset {
        name: manifest.name,
        version: Some(manifest.version),
        kind: manifest.kind.to_string(),
        start_block: Some(manifest.start_block),
        tables: tables::all(&manifest.network),
        network: manifest.network,
        functions: vec![],
    })
}

/// Automatically generate a README.md file with the schema whenever tests are executed.
#[tokio::test]
async fn print_schema_to_readme() {
    fs_err::write(
        "../../../docs/schemas/firehose-evm.md",
        common::catalog::schema_to_markdown(tables::all("test_network")).await,
    )
    .unwrap();
}
