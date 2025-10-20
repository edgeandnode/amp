use std::io;

use dataset_store::DatasetKind;
use datasets_common::{
    manifest::{ArrowSchema, Field, TableSchema},
    name::Name,
};

/// Create a TableSchema from a logical table
fn table_schema_from_logical_table(table: &common::Table) -> TableSchema {
    let fields: Vec<Field> = table
        .schema()
        .fields()
        .iter()
        .filter(|field| field.name() != common::SPECIAL_BLOCK_NUM)
        .map(|field| Field {
            name: field.name().clone(),
            type_: field.data_type().clone().into(),
            nullable: field.is_nullable(),
        })
        .collect();

    TableSchema {
        arrow: ArrowSchema { fields },
    }
}

pub async fn run(
    name: Name,
    kind: impl Into<DatasetKind>,
    network: String,
    start_block: Option<u64>,
    finalized_blocks_only: bool,
    writer: &mut impl io::Write,
) -> Result<(), Error> {
    let kind = kind.into();

    let dataset_bytes = match kind {
        dataset_store::DatasetKind::EvmRpc => {
            let tables = evm_rpc_datasets::tables::all(&network)
                .iter()
                .map(|table| {
                    let schema = table_schema_from_logical_table(table);
                    let manifest_table = evm_rpc_datasets::Table::new(schema, network.clone());
                    (table.name().to_string(), manifest_table)
                })
                .collect();
            let manifest = evm_rpc_datasets::Manifest {
                name,
                version: Default::default(),
                kind: kind.as_str().parse().expect("kind is valid"),
                network: network.clone(),
                start_block: start_block.unwrap_or(0),
                finalized_blocks_only,
                tables,
            };
            serde_json::to_vec_pretty(&manifest).map_err(Error::Serialization)?
        }
        dataset_store::DatasetKind::EthBeacon => {
            let tables = eth_beacon_datasets::all_tables(network.clone())
                .iter()
                .map(|table| {
                    let schema = table_schema_from_logical_table(table);
                    let manifest_table = eth_beacon_datasets::Table::new(schema, network.clone());
                    (table.name().to_string(), manifest_table)
                })
                .collect();
            let manifest = eth_beacon_datasets::Manifest {
                name,
                version: Default::default(),
                kind: kind.as_str().parse().expect("kind is valid"),
                network: network.clone(),
                start_block: start_block.unwrap_or(0),
                finalized_blocks_only,
                tables,
            };
            serde_json::to_vec_pretty(&manifest).map_err(Error::Serialization)?
        }
        dataset_store::DatasetKind::Firehose => {
            let tables = firehose_datasets::evm::tables::all(&network)
                .iter()
                .map(|table| {
                    let schema = table_schema_from_logical_table(table);
                    let manifest_table =
                        firehose_datasets::dataset::Table::new(schema, network.clone());
                    (table.name().to_string(), manifest_table)
                })
                .collect();
            let manifest = firehose_datasets::dataset::Manifest {
                name,
                version: Default::default(),
                kind: kind.as_str().parse().expect("kind is valid"),
                network: network.clone(),
                start_block: start_block.unwrap_or(0),
                finalized_blocks_only,
                tables,
            };
            serde_json::to_vec_pretty(&manifest).map_err(Error::Serialization)?
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
    /// any of the supported dataset types (evm-rpc, eth-beacon, firehose,
    /// derived, sql).
    #[error("Unsupported dataset kind '{0}'")]
    InvalidKind(String),

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
