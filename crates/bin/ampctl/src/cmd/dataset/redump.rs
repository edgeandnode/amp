//! Dataset redump command.
//!
//! Submits a request to re-extract blocks in a given range for a dataset.
//!
//! This is useful when `ampctl verify` identifies corrupted data and the operator
//! needs to re-extract specific block ranges.
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

use crate::{args::GlobalArgs, client::datasets::RedumpResponse};

/// Command-line arguments for the `dataset redump` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// The dataset reference in format: namespace/name@version
    ///
    /// Examples: my_namespace/my_dataset@1.0.0, my_namespace/my_dataset@latest
    #[arg(value_name = "REFERENCE", required = true, value_parser = clap::value_parser!(Reference))]
    pub dataset_ref: Reference,

    /// First block of the range to re-extract (inclusive)
    #[arg(long, value_name = "BLOCK_NUMBER")]
    pub start_block: u64,

    /// Last block of the range to re-extract (inclusive)
    #[arg(long, value_name = "BLOCK_NUMBER")]
    pub end_block: u64,
}

/// Result of a dataset redump operation.
#[derive(serde::Serialize)]
struct RedumpResult {
    request_id: i64,
    job_id: Option<i64>,
}

impl std::fmt::Display for RedumpResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(
            f,
            "{} Redump request submitted (request_id: {})",
            console::style("✓").green().bold(),
            self.request_id,
        )?;
        match self.job_id {
            Some(job_id) => writeln!(
                f,
                "{} New dump job scheduled (job_id: {})",
                console::style("→").cyan(),
                job_id,
            )?,
            None => writeln!(
                f,
                "{} Active dump job already running; it will pick up this request",
                console::style("→").cyan(),
            )?,
        }
        Ok(())
    }
}

/// Submit a redump request for a dataset block range.
///
/// # Errors
///
/// Returns [`Error`] for invalid paths/URLs, API errors (400/404/409/500), or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, %dataset_ref, start_block, end_block))]
pub async fn run(
    Args {
        global,
        dataset_ref,
        start_block,
        end_block,
    }: Args,
) -> Result<(), Error> {
    tracing::debug!(%dataset_ref, start_block, end_block, "Submitting redump request");

    let response = submit_redump(&global, &dataset_ref, start_block, end_block).await?;
    let result = RedumpResult {
        request_id: response.request_id,
        job_id: response.job_id,
    };
    global.print(&result).map_err(Error::JsonSerialization)?;

    Ok(())
}

/// Submit a redump request via the admin API.
#[tracing::instrument(skip_all, fields(%dataset_ref, start_block, end_block))]
async fn submit_redump(
    global: &GlobalArgs,
    dataset_ref: &Reference,
    start_block: u64,
    end_block: u64,
) -> Result<RedumpResponse, Error> {
    let client = global.build_client().map_err(Error::ClientBuild)?;
    let response = client
        .datasets()
        .redump(dataset_ref, start_block, end_block)
        .await
        .map_err(Error::Redump)?;

    Ok(response)
}

/// Errors for dataset redump operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuild(#[source] crate::args::BuildClientError),

    /// Redump error from the client
    #[error("redump failed")]
    Redump(#[source] crate::client::datasets::RedumpError),

    /// Failed to serialize result to JSON
    #[error("failed to serialize result to JSON")]
    JsonSerialization(#[source] serde_json::Error),
}
