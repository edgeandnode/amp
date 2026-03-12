use datasets_common::hash_reference::HashReference;
use datasets_raw::dataset::Dataset as RawDataset;

pub mod tables;

// Re-export dataset kind and manifest types from datasets-raw for backward compatibility
pub use datasets_common::manifest::{ArrowSchema, Field, TableSchema};
pub use datasets_raw::{
    dataset_kind::{EvmRpcDatasetKind, EvmRpcDatasetKindError},
    manifest::{EvmRpcManifest as Manifest, Table},
};

/// Convert an EVM RPC manifest into a logical dataset representation.
///
/// Dataset identity (namespace, name, version, hash reference) must be provided externally as they
/// are not part of the manifest.
pub fn dataset(reference: HashReference, manifest: Manifest) -> RawDataset {
    let network = manifest.network;
    RawDataset::new(
        reference,
        manifest.kind.into(),
        network.clone(),
        tables::all(&network),
        Some(manifest.start_block),
        manifest.finalized_blocks_only,
    )
}
