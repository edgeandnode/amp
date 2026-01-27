//! Dataset manifest generation command.
//!
//! Generates a dataset manifest file for supported dataset kinds by:
//! 1. Accepting dataset configuration parameters (kind, network, etc.)
//! 2. Creating a manifest structure appropriate for the dataset kind
//! 3. Serializing to JSON and writing to output (file or stdout)
//!
//! # Supported Dataset Kinds
//!
//! - **evm-rpc**: EVM RPC dataset manifests
//! - **firehose**: Firehose dataset manifests
//! - **solana**: Solana dataset manifests
//!
//! Note: Derived datasets are not supported for automatic generation as they
//! require custom SQL transformation definitions.
//!
//! # Configuration
//!
//! - Dataset Kind: `--kind` flag or `GM_KIND` env var
//! - Network: `--network` flag or `GM_NETWORK` env var
//! - Output: `--out` flag or `GM_OUT` env var (optional, defaults to stdout)
//! - Start Block: `--start-block` flag or `GM_START_BLOCK` env var (optional, defaults to 0)
//! - Finalized Blocks Only: `--finalized-blocks-only` flag or `GM_FINALIZED_BLOCKS_ONLY` env var

use std::path::PathBuf;

use datasets_common::{
    dataset::Table,
    dataset_kind_str::DatasetKindStr,
    manifest::{ArrowSchema, Field, TableSchema},
};
use datasets_derived::DerivedDatasetKind;
use evm_rpc_datasets::EvmRpcDatasetKind;
use firehose_datasets::FirehoseDatasetKind;
use monitoring::logging;
use solana_datasets::SolanaDatasetKind;

/// Command-line arguments for the `manifest generate` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    /// Kind of the dataset (evm-rpc, firehose, solana).
    #[arg(long, required = true, env = "GM_KIND")]
    pub kind: DatasetKindStr,

    /// The name of the network.
    #[arg(long, required = true, env = "GM_NETWORK")]
    pub network: String,

    /// Output file or directory. If it's a directory, the generated file name will
    /// match the `kind` parameter.
    ///
    /// If not specified, the manifest will be printed to stdout.
    #[arg(short, long, env = "GM_OUT")]
    pub out: Option<PathBuf>,

    /// The starting block number for the dataset. Defaults to 0.
    #[arg(long, env = "GM_START_BLOCK")]
    pub start_block: Option<u64>,

    /// Only include finalized block data.
    #[arg(long, env = "GM_FINALIZED_BLOCKS_ONLY")]
    pub finalized_blocks_only: bool,
}

/// CLI command handler for generating dataset manifests.
///
/// Handles output destination (file vs stdout) and delegates manifest generation
/// to [`generate_manifest`]. This function serves as the entry point from the CLI.
///
/// If `out` is a directory, the filename will be derived from the dataset kind.
/// If `out` is None, output will be written to stdout.
///
/// # Errors
///
/// Returns [`Error`] for unsupported dataset kinds, serialization failures,
/// or write errors.
#[tracing::instrument]
pub async fn run(
    Args {
        kind,
        network,
        out,
        start_block,
        finalized_blocks_only,
    }: Args,
) -> Result<(), Error> {
    // Determine the output destination (file or stdout)
    let mut writer: Box<dyn std::io::Write> = if let Some(mut out_path) = out {
        // If output is a directory, append the filename based on kind
        if out_path.is_dir() {
            out_path.push(format!("{}.json", &kind));
        }

        let file = std::fs::File::create(&out_path).map_err(|err| {
            tracing::error!(path = %out_path.display(), error = %err, error_source = logging::error_source(&err), "Failed to create output file");
            Error::WriteOutput(err)
        })?;

        Box::new(file)
    } else {
        Box::new(std::io::stdout())
    };

    generate_manifest(
        &kind,
        network,
        start_block,
        finalized_blocks_only,
        &mut writer,
    )
    .await
}

/// Generate a dataset manifest and write to the specified output.
///
/// Creates a manifest appropriate for the dataset kind and writes it as JSON.
/// This is the core business logic function used by the CLI and tests.
///
/// # Errors
///
/// Returns [`Error`] for unsupported dataset kinds, serialization failures,
/// or write errors.
#[tracing::instrument(skip(writer))]
pub async fn generate_manifest<W>(
    kind: &DatasetKindStr,
    network: String,
    start_block: Option<u64>,
    finalized_blocks_only: bool,
    writer: &mut W,
) -> Result<(), Error>
where
    W: std::io::Write + ?Sized,
{
    let start_block = start_block.unwrap_or(0);

    let dataset_bytes = if kind == DerivedDatasetKind {
        return Err(Error::DerivedNotSupported);
    } else if kind == EvmRpcDatasetKind {
        generate_evm_rpc_manifest(&network, start_block, finalized_blocks_only)?
    } else if kind == FirehoseDatasetKind {
        generate_firehose_manifest(&network, start_block, finalized_blocks_only)?
    } else if kind == SolanaDatasetKind {
        generate_solana_manifest(&network, start_block, finalized_blocks_only)?
    } else {
        return Err(Error::UnsupportedKind(kind.clone()));
    };

    writer
        .write_all(&dataset_bytes)
        .map_err(Error::WriteOutput)?;

    Ok(())
}

/// Generate an EVM RPC dataset manifest.
#[inline]
fn generate_evm_rpc_manifest(
    network: &str,
    start_block: u64,
    finalized_blocks_only: bool,
) -> Result<Vec<u8>, Error> {
    let tables = evm_rpc_datasets::tables::all(network)
        .iter()
        .map(|table| {
            let schema = table_schema_from_logical_table(table);
            let manifest_table = evm_rpc_datasets::Table::new(schema, network.to_string());
            (table.name().to_string(), manifest_table)
        })
        .collect();

    let manifest = evm_rpc_datasets::Manifest {
        kind: EvmRpcDatasetKind,
        network: network.to_string(),
        start_block,
        finalized_blocks_only,
        tables,
    };

    serde_json::to_vec_pretty(&manifest).map_err(Error::Serialization)
}

/// Generate a Solana dataset manifest.
#[inline]
fn generate_solana_manifest(
    network: &str,
    start_block: u64,
    finalized_blocks_only: bool,
) -> Result<Vec<u8>, Error> {
    let tables = solana_datasets::tables::all(network)
        .iter()
        .map(|table| {
            let schema = table_schema_from_logical_table(table);
            let manifest_table = solana_datasets::Table::new(schema, network.to_string());
            (table.name().to_string(), manifest_table)
        })
        .collect();

    let manifest = solana_datasets::Manifest {
        kind: SolanaDatasetKind,
        network: network.to_string(),
        start_block,
        finalized_blocks_only,
        tables,
    };

    serde_json::to_vec_pretty(&manifest).map_err(Error::Serialization)
}

/// Generate a Firehose dataset manifest.
#[inline]
fn generate_firehose_manifest(
    network: &str,
    start_block: u64,
    finalized_blocks_only: bool,
) -> Result<Vec<u8>, Error> {
    let tables = firehose_datasets::evm::tables::all(network)
        .iter()
        .map(|table| {
            let schema = table_schema_from_logical_table(table);
            let manifest_table =
                firehose_datasets::dataset::Table::new(schema, network.to_string());
            (table.name().to_string(), manifest_table)
        })
        .collect();

    let manifest = firehose_datasets::dataset::Manifest {
        kind: FirehoseDatasetKind,
        network: network.to_string(),
        start_block,
        finalized_blocks_only,
        tables,
    };

    serde_json::to_vec_pretty(&manifest).map_err(Error::Serialization)
}

/// Create a TableSchema from a logical table
fn table_schema_from_logical_table(table: &Table) -> TableSchema {
    let fields: Vec<Field> = table
        .schema()
        .fields()
        .iter()
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

/// Errors specific to generate manifest operations
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Derived datasets do not support automatic manifest generation.
    ///
    /// This occurs when attempting to generate a manifest for a Derived dataset type.
    /// Derived datasets are defined through SQL transformations and must be created
    /// manually as they require custom query definitions that cannot be auto-generated.
    #[error("Derived datasets don't support dataset generation")]
    DerivedNotSupported,

    /// Unsupported dataset kind.
    ///
    /// This occurs when the provided dataset kind is not recognized by the manifest generator.
    /// Supported kinds are: evm-rpc, firehose, solana.
    #[error("Unsupported dataset kind '{0}'")]
    UnsupportedKind(DatasetKindStr),

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
    WriteOutput(#[source] std::io::Error),
}
