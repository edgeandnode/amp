use common::Dataset;

use crate::dataset::Manifest;
pub use crate::proto::sf::ethereum::r#type::v2 as pbethereum;
pub mod pb_to_rows;
pub mod tables;

pub fn dataset(manifest: Manifest) -> Dataset {
    Dataset {
        name: manifest.name,
        version: Some(manifest.version),
        kind: manifest.kind.to_string(),
        start_block: Some(manifest.start_block),
        finalized_blocks_only: manifest.finalized_blocks_only,
        tables: tables::all(&manifest.network),
        network: Some(manifest.network),
        functions: vec![],
    }
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
