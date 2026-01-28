use datasets_common::hash_reference::HashReference;

use crate::dataset::Manifest;
pub use crate::proto::sf::ethereum::r#type::v2 as pbethereum;
pub mod pb_to_rows;
pub mod tables;

/// Convert a Firehose manifest into a logical dataset representation.
///
/// Dataset identity (namespace, name, version, hash reference) must be provided externally as they
/// are not part of the manifest.
pub fn dataset(reference: HashReference, manifest: Manifest) -> crate::dataset::Dataset {
    let network = manifest.network;
    crate::dataset::Dataset {
        reference,
        kind: manifest.kind,
        start_block: Some(manifest.start_block),
        finalized_blocks_only: manifest.finalized_blocks_only,
        tables: tables::all(&network),
    }
}
