use common::Dataset;
use datasets_common::{name::Name, namespace::Namespace, version::Version};

use crate::dataset::Manifest;
pub use crate::proto::sf::ethereum::r#type::v2 as pbethereum;
pub mod pb_to_rows;
pub mod tables;

/// Convert a Firehose manifest into a logical dataset representation.
///
/// Dataset identity (namespace, name, version) must be provided externally as they are not part
/// of the manifest.
pub fn dataset(namespace: Namespace, name: Name, version: Version, manifest: Manifest) -> Dataset {
    let network = manifest.network;
    Dataset {
        namespace,
        name,
        version: Some(version),
        kind: manifest.kind.to_string(),
        start_block: Some(manifest.start_block),
        finalized_blocks_only: manifest.finalized_blocks_only,
        tables: tables::all(&network),
        network: Some(network),
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
