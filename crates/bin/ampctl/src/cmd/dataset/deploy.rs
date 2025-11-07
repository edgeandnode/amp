//! Dataset deployment command.
//!
//! Deploys a dataset to start syncing blockchain data by:
//! 1. Parsing dataset reference (namespace/name@version)
//! 2. POSTing to admin API `/datasets/{namespace}/{name}/versions/{version}/deploy` endpoint
//! 3. Returning the job ID of the scheduled deployment
//!
//! # Dataset Reference Format
//!
//! `namespace/name@version` (e.g., `graph/eth_mainnet@1.0.0`)
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - End block: `--end-block` flag (optional) - "latest", block number, or negative offset
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use datasets_common::reference::Reference;
use dump::EndBlock;
use worker::{job::JobId, node_id::NodeId};

use crate::args::GlobalArgs;

/// Command-line arguments for the `dep-dataset` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// The dataset reference in format: namespace/name@version
    ///
    /// Examples: my_namespace/my_dataset@1.0.0, my_namespace/my_dataset@latest
    #[arg(value_name = "REFERENCE", required = true, value_parser = clap::value_parser!(Reference))]
    pub dataset_ref: Reference,

    /// End block configuration for the deployment
    ///
    /// Determines when the dataset should stop syncing blocks:
    /// - Omitted: Continuous syncing (never stops)
    /// - "latest": Stop at the latest available block
    /// - Positive number: Stop at specific block number (e.g., "1000000")
    /// - Negative number: Stop N blocks before latest (e.g., "-100")
    #[arg(long, value_parser = clap::value_parser!(EndBlock))]
    pub end_block: Option<EndBlock>,

    /// Number of parallel workers to run
    ///
    /// Each worker will be responsible for an equal number of blocks.
    /// For example, if extracting blocks 0-10,000,000 with parallelism=10,
    /// each worker will handle a contiguous section of 1 million blocks.
    ///
    /// Only applicable to raw datasets (EVM RPC, Firehose, etc.).
    /// Derived datasets ignore this parameter.
    ///
    /// Defaults to 1 if not specified.
    #[arg(long, default_value = "1")]
    pub parallelism: u16,

    /// Worker ID to assign the job to
    ///
    /// If specified, the job will be assigned to this specific worker.
    /// If not specified, a worker will be selected randomly from available workers.
    ///
    /// The worker must be active (has sent heartbeats recently) for the deployment to succeed.
    #[arg(long, value_parser = clap::value_parser!(NodeId))]
    pub worker_id: Option<NodeId>,
}

/// Deploy a dataset to start syncing blockchain data.
///
/// Schedules a deployment job via the admin API and returns the job ID.
///
/// # Errors
///
/// Returns [`Error`] for invalid paths/URLs, API errors (400/404/500), or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, %dataset_ref))]
pub async fn run(
    Args {
        global,
        dataset_ref,
        end_block,
        parallelism,
        worker_id,
    }: Args,
) -> Result<(), Error> {
    tracing::debug!(
        %dataset_ref,
        ?end_block,
        %parallelism,
        ?worker_id,
        "Deploying dataset"
    );

    let job_id = deploy_dataset(&global, &dataset_ref, end_block, parallelism, worker_id).await?;

    crate::success!("Dataset deployed successfully");
    crate::info!("Job ID: {}", job_id);

    Ok(())
}

/// Deploy a dataset via the admin API.
///
/// POSTs to the versioned `/datasets/{namespace}/{name}/versions/{version}/deploy` endpoint
/// and returns the job ID.
#[tracing::instrument(skip_all, fields(%dataset_ref, ?end_block, %parallelism, ?worker_id))]
async fn deploy_dataset(
    global: &GlobalArgs,
    dataset_ref: &Reference,
    end_block: Option<EndBlock>,
    parallelism: u16,
    worker_id: Option<NodeId>,
) -> Result<JobId, Error> {
    let client = global.build_client()?;
    let job_id = client
        .datasets()
        .deploy(dataset_ref, end_block, parallelism, worker_id)
        .await
        .map_err(|source| Error::Deploy { source })?;

    Ok(job_id)
}

/// Errors for dataset deployment operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuildError {
        #[from]
        source: crate::args::BuildClientError,
    },

    /// Deployment error from the client
    #[error("deployment failed")]
    Deploy {
        #[source]
        source: crate::client::datasets::DeployError,
    },
}
