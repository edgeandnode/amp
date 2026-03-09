//! Dataset rematerialize command.
//!
//! Re-extracts a specified block range for a raw dataset by:
//! 1. Parsing dataset reference (namespace/name@version)
//! 2. POSTing to admin API `/datasets/{namespace}/{name}/versions/{version}/rematerialize` endpoint
//! 3. Returning information about the rematerialize operation
//!
//! # Dataset Reference Format
//!
//! `namespace/name@version` (e.g., `graph/eth_mainnet@1.0.0`)
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use datasets_common::reference::Reference;

use crate::args::GlobalArgs;

/// Command-line arguments for the `rematerialize-dataset` command.
///
/// # Requirements
///
/// The dataset must have an active materialization job running. If no active job exists,
/// the command will fail with a NO_ACTIVE_JOB error. To rematerialize a dataset that
/// is not currently running:
///
/// 1. First deploy the dataset to start an active job
/// 2. Then run the rematerialize command
///
/// # Note
///
/// Rematerializing a raw dataset does NOT automatically update derived datasets that
/// depend on it. You must manually re-deploy any dependent derived datasets after
/// rematerialization completes.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// The dataset reference in format: namespace/name@version
    ///
    /// Examples: my_namespace/my_dataset@1.0.0, my_namespace/my_dataset@latest
    ///
    /// Note: Only raw datasets can be rematerialized. Attempting to rematerialize
    /// a derived dataset will return an error.
    #[arg(value_name = "REFERENCE", required = true, value_parser = clap::value_parser!(Reference))]
    pub dataset_ref: Reference,

    /// Start block of the range to rematerialize (inclusive)
    ///
    /// Must be greater than or equal to the dataset's start_block.
    #[arg(long, required = true)]
    pub start_block: u64,

    /// End block of the range to rematerialize (inclusive)
    ///
    /// Must be greater than or equal to start_block.
    #[arg(long, required = true)]
    pub end_block: u64,
}

/// Rematerialize a block range for a raw dataset.
///
/// Triggers re-extraction of the specified block range by notifying the active
/// materialization job. The job will process the rematerialize range before
/// resuming normal operation.
///
/// # Requirements
///
/// - The dataset must be a raw dataset (not derived)
/// - The dataset must have an active materialization job running
/// - The block range must be valid (start <= end, start >= dataset.start_block)
///
/// # Errors
///
/// Returns [`Error`] for:
/// - No active job exists (NO_ACTIVE_JOB) - deploy the dataset first
/// - Derived dataset (cannot rematerialize derived datasets)
/// - Invalid block range (start > end or start < dataset.start_block)
/// - Network failures or API errors (400/404/500)
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, %dataset_ref, start_block, end_block))]
pub async fn run(
    Args {
        global,
        dataset_ref,
        start_block,
        end_block,
    }: Args,
) -> Result<(), Error> {
    tracing::debug!(
        %dataset_ref,
        start_block,
        end_block,
        "rematerializing dataset range"
    );

    let client = global.build_client().map_err(Error::ClientBuild)?;
    let response = client
        .datasets()
        .rematerialize(&dataset_ref, start_block, end_block)
        .await
        .map_err(Error::Rematerialize)?;

    let result = RematerializeResult {
        job_id: response.job_id,
        new_job_created: response.new_job_created,
        start_block,
        end_block,
    };
    global.print(&result).map_err(Error::JsonSerialization)?;

    Ok(())
}

/// Result of a dataset rematerialize operation.
#[derive(serde::Serialize)]
struct RematerializeResult {
    job_id: amp_worker_core::jobs::job_id::JobId,
    new_job_created: bool,
    start_block: u64,
    end_block: u64,
}

impl std::fmt::Display for RematerializeResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(
            f,
            "{} Rematerialize request accepted",
            console::style("✓").green().bold()
        )?;
        writeln!(f, "{} Job ID: {}", console::style("→").cyan(), self.job_id)?;
        writeln!(
            f,
            "{} Block range: {} to {}",
            console::style("→").cyan(),
            self.start_block,
            self.end_block
        )?;
        if self.new_job_created {
            writeln!(
                f,
                "{} New temporary job created",
                console::style("ℹ").blue()
            )?;
        } else {
            writeln!(
                f,
                "{} Notified active job to process range",
                console::style("ℹ").blue()
            )?;
        }
        Ok(())
    }
}

/// Errors for dataset rematerialize operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuild(#[source] crate::args::BuildClientError),

    /// Rematerialize error from the client
    #[error("rematerialize failed")]
    Rematerialize(#[source] amp_client_admin::datasets::RematerializeError),

    /// Failed to serialize result to JSON
    #[error("failed to serialize result to JSON")]
    JsonSerialization(#[source] serde_json::Error),
}
