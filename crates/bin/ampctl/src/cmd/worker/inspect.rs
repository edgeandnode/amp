//! Worker inspect command.
//!
//! Retrieves and displays detailed information about a specific worker through the admin API by:
//! 1. Creating a client for the admin API
//! 2. Using the client's worker get method
//! 3. Displaying the worker as JSON
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use monitoring::logging;
use worker::node_id::NodeId;

use crate::{args::GlobalArgs, client};

/// Command-line arguments for the `worker inspect` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// The worker node identifier to inspect
    #[arg(value_name = "NODE_ID", required = true, value_parser = clap::value_parser!(NodeId))]
    pub node_id: NodeId,
}

/// Result wrapper for worker inspect output.
#[derive(serde::Serialize)]
struct InspectResult {
    #[serde(flatten)]
    data: client::workers::WorkerDetailResponse,
}

impl std::fmt::Display for InspectResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(f, "Node ID: {}", self.data.node_id)?;
        writeln!(f, "Created: {}", self.data.created_at)?;
        writeln!(f, "Registered: {}", self.data.registered_at)?;
        writeln!(f, "Heartbeat: {}", self.data.heartbeat_at)?;
        writeln!(f)?;
        writeln!(f, "Worker Info:")?;
        writeln!(f, "  Version: {}", self.data.info.version)?;
        writeln!(f, "  Commit: {}", self.data.info.commit_sha)?;
        writeln!(f, "  Commit Timestamp: {}", self.data.info.commit_timestamp)?;
        writeln!(f, "  Build Date: {}", self.data.info.build_date)?;
        Ok(())
    }
}

/// Inspect worker details by retrieving them from the admin API.
///
/// Retrieves worker information and displays it based on the output format.
///
/// # Errors
///
/// Returns [`Error`] for API errors (400/404/500) or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, node_id = %node_id))]
pub async fn run(Args { global, node_id }: Args) -> Result<(), Error> {
    tracing::debug!("Retrieving worker from admin API");

    let worker = get_worker(&global, &node_id).await?;
    let result = InspectResult { data: worker };
    global.print(&result).map_err(Error::JsonFormattingError)?;

    Ok(())
}

/// Retrieve a worker from the admin API.
///
/// Creates a client and uses the worker get method.
#[tracing::instrument(skip_all)]
async fn get_worker(
    global: &GlobalArgs,
    node_id: &NodeId,
) -> Result<client::workers::WorkerDetailResponse, Error> {
    let client = global.build_client().map_err(Error::ClientBuildError)?;

    let worker = client.workers().get(node_id).await.map_err(|err| {
        tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to get worker");
        Error::ClientError(err)
    })?;

    match worker {
        Some(worker) => Ok(worker),
        None => Err(Error::WorkerNotFound {
            node_id: node_id.to_string(),
        }),
    }
}

/// Errors for worker inspect operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuildError(#[source] crate::args::BuildClientError),

    /// Client error from the API
    #[error("client error")]
    ClientError(#[source] client::workers::GetError),

    /// Worker not found
    #[error("worker not found: {node_id}")]
    WorkerNotFound { node_id: String },

    /// Failed to format JSON for display
    #[error("failed to format worker JSON")]
    JsonFormattingError(#[source] serde_json::Error),
}
