//! Dataset verify command.
//!
//! Handles internal and external checks of blocks using the ve crate.
//!
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use datasets_common::reference::Reference;
use monitoring::logging;

use crate::{args::GlobalArgs, client};

/// Command-line arguments for the `dataset verify` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// The dataset reference to verify (namespace/name@version)
    pub reference: Reference,

    /// Number of blocks to verify (default: 100)
    #[arg(long, default_value_t = 100)]
    pub limit: u64,

    #[arg(long, value_enum, default_value_t = VerifyMode::Header)]
    pub mode: VerifyMode,

    #[arg(long = "data-path")]
    pub accumulators_path: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum VerifyMode {
    Header,
    Transactions,
    Receipts,
    HeaderAccumulators,
}

/// Verify internal and external dataset blocks using ve crate.
///
/// This function currently validates that the dataset exists and prints a
/// placeholder message for the ve-based verification logic.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, reference = %reference, limit = %limit))]
pub async fn run(
    Args {
        global,
        reference,
        limit,
        mode,
        accumulators_path,
    }: Args,
) -> Result<(), Error> {
    tracing::debug!(
        "Starting dataset internal verification for {} with limit {}",
        reference,
        limit
    );

    // 1) Basic sanity: ensure the dataset exists via the admin API
    let client = global.build_client().map_err(Error::ClientBuildError)?;

    let dataset = client.datasets().get(&reference).await.map_err(|err| {
        tracing::error!(
            error = %err,
            error_source = logging::error_source(&err),
            "Failed to get dataset"
        );
        Error::ClientError(err)
    })?;

    if dataset.is_none() {
        return Err(Error::DatasetNotFound {
            reference: reference.clone(),
        });
    }

    match mode {
        VerifyMode::Header => internal_verify_header(&global, &reference, limit).await?,

        VerifyMode::Transactions => {
            unimplemented!("internal_verify_transactions is not implemented yet")
        }

        VerifyMode::Receipts => {
            unimplemented!("internal_verify_receipts is not implemented yet")
        }

        VerifyMode::HeaderAccumulators => {
            let _path = accumulators_path.ok_or(Error::MissingExternalPath);
            unimplemented!("internal_verify_header_accumulators is not implemented yet")
        }
    }
    // 2) VE-based verification hook (to be implemented)
    // TODO: plug in ve crate-based checks here, e.g.:
    // - fetch blocks via AMPD blocks API
    // - convert to Header-like structures
    // - verify hashes/consistency using ve primitives
    println!(
        "VE-based verification hook should run here for {} blocks (dataset: {}).",
        limit, reference
    );

    Ok(())
}

#[tracing::instrument(skip_all, fields(reference = %reference, limit = %limit))]
async fn internal_verify_header(
    _global: &GlobalArgs,
    reference: &Reference,
    limit: u64,
) -> Result<(), Error> {
    ve::verify_header_for_dataset(reference, limit)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "header verification failed");
            Error::VerifyError(e)
        })
}

/// Errors for dataset verification operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to build admin API client
    #[error("failed to build admin API client")]
    ClientBuildError(#[source] crate::args::BuildClientError),

    /// Client error from the API
    #[error("client error")]
    ClientError(#[source] client::datasets::GetError),

    /// Dataset not found
    #[error("dataset not found: {reference}")]
    DatasetNotFound { reference: Reference },

    /// Verification failed
    #[error("verification error")]
    VerifyError(#[source] anyhow::Error),

    /// Verification failed
    #[error("accumulator not found")]
    MissingExternalPath(#[source] anyhow::Error),
}
