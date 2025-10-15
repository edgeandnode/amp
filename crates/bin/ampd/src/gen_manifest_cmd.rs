use std::io;

use dataset_store::DatasetKind;
use datasets_common::name::Name;

pub async fn run(
    name: Name,
    kind: impl Into<DatasetKind>,
    network: String,
    start_block: Option<u64>,
    manifest: Option<String>,
    module: Option<String>,
    writer: &mut impl io::Write,
) -> Result<(), Error> {
    let kind = kind.into();

    let dataset_bytes = match kind {
        dataset_store::DatasetKind::EvmRpc => {
            let manifest = evm_rpc_datasets::Manifest {
                name,
                version: Default::default(),
                kind: kind.as_str().parse().expect("kind is valid"),
                network,
                start_block: start_block.unwrap_or(0),
            };
            serde_json::to_vec(&manifest).map_err(Error::Serialization)?
        }
        dataset_store::DatasetKind::EthBeacon => {
            let manifest = eth_beacon_datasets::Manifest {
                name,
                version: Default::default(),
                kind: kind.as_str().parse().expect("kind is valid"),
                network,
                start_block: start_block.unwrap_or(0),
            };
            serde_json::to_vec(&manifest).map_err(Error::Serialization)?
        }
        dataset_store::DatasetKind::Firehose => {
            let manifest = firehose_datasets::dataset::Manifest {
                name,
                version: Default::default(),
                kind: kind.as_str().parse().expect("kind is valid"),
                network,
                start_block: start_block.unwrap_or(0),
            };
            serde_json::to_vec(&manifest).map_err(Error::Serialization)?
        }
        dataset_store::DatasetKind::Substreams => {
            let (Some(manifest_url), Some(module_name)) = (manifest, module) else {
                return Err(Error::MissingSubstreamsArgs);
            };
            let manifest = substreams_datasets::dataset::Manifest {
                name,
                version: Default::default(),
                kind: kind.as_str().parse().expect("kind is valid"),
                network,
                manifest: manifest_url,
                module: module_name,
            };
            serde_json::to_vec(&manifest).map_err(Error::Serialization)?
        }
        dataset_store::DatasetKind::Derived => {
            return Err(Error::DerivedNotSupported);
        }
    };

    writer
        .write_all(&dataset_bytes)
        .map_err(Error::WriteOutput)?;

    Ok(())
}

/// Errors specific to generate manifest operations
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The provided dataset name failed to parse according to the naming rules.
    ///
    /// This occurs during the initial validation of the dataset name string before
    /// any manifest generation operations are performed. Dataset names must follow
    /// strict rules: start with a lowercase letter or underscore, contain only
    /// lowercase letters, digits, and underscores, and not be empty.
    #[error("Invalid dataset name: {0}")]
    InvalidName(#[source] datasets_common::name::NameError),

    /// The dataset kind specified is not supported.
    ///
    /// This occurs when the `kind` parameter contains a value that doesn't match
    /// any of the supported dataset types (evm-rpc, eth-beacon, firehose, substreams,
    /// derived, sql).
    #[error("Unsupported dataset kind '{0}'")]
    InvalidKind(String),

    /// Required Substreams arguments are missing.
    ///
    /// This occurs when generating a manifest for a Substreams dataset without
    /// providing both the `manifest` (Substreams package URL) and `module`
    /// (output module name) parameters, which are required for Substreams datasets.
    #[error("`manifest` and `module` arguments are required for `DatasetKind::Substreams`")]
    MissingSubstreamsArgs,

    /// Failed to fetch table definitions from a Substreams package.
    ///
    /// This occurs when the system cannot retrieve or process the Substreams package
    /// to extract table definitions, which may be due to:
    /// - Invalid Substreams package URL
    /// - Network connectivity issues
    /// - Invalid package format or structure
    /// - Missing or invalid output module
    #[error("Failed to fetch substreams tables: {0}")]
    SubstreamsTables(#[source] firehose_datasets::Error),

    /// Derived datasets do not support automatic manifest generation.
    ///
    /// This occurs when attempting to generate a manifest for a Derived dataset type.
    /// Derived datasets are defined through SQL transformations and must be created
    /// manually as they require custom query definitions that cannot be auto-generated.
    #[error("`DatasetKind::Derived` doesn't support dataset generation")]
    DerivedNotSupported,

    /// Failed to serialize the manifest to JSON format.
    ///
    /// This occurs when the generated manifest structure cannot be serialized to JSON,
    /// which may happen if the manifest contains invalid data or if there are internal
    /// serialization issues.
    #[error("Failed to serialize manifest: {0}")]
    Serialization(#[source] serde_json::Error),

    /// Failed to write the manifest output.
    ///
    /// This occurs when the system cannot write the generated manifest to the specified
    /// output destination, which may be due to:
    /// - Insufficient permissions
    /// - Disk space issues
    /// - Invalid file path
    /// - Closed or broken output stream
    #[error("Failed to write output: {0}")]
    WriteOutput(#[source] io::Error),
}
