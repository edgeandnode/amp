//! Dataset deployment command.
//!
//! Deploys a dataset to start syncing blockchain data by:
//! 1. Parsing dataset reference (namespace/name@version)
//! 2. POSTing to admin API `/datasets/{name}/versions/{version}/dump` endpoint
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
use url::Url;
use worker::JobId;

/// Command-line arguments for the `dep-dataset` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    /// The URL of the engine admin interface
    #[arg(long, env = "AMP_ADMIN_URL", default_value = "http://localhost:1610", value_parser = clap::value_parser!(Url))]
    pub admin_url: Url,

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
}

/// Deploy a dataset to start syncing blockchain data.
///
/// Schedules a deployment job via the admin API and returns the job ID.
///
/// # Errors
///
/// Returns [`Error`] for invalid paths/URLs, API errors (400/404/500), or network failures.
#[tracing::instrument(skip_all, fields(%admin_url, %dataset_ref))]
pub async fn run(
    Args {
        admin_url,
        dataset_ref,
        end_block,
        parallelism,
    }: Args,
) -> Result<(), Error> {
    tracing::debug!(
        %dataset_ref,
        ?end_block,
        %parallelism,
        "Deploying dataset"
    );

    let job_id = deploy_dataset(&admin_url, &dataset_ref, end_block, parallelism).await?;

    crate::success!("Dataset deployed successfully");
    crate::info!("Job ID: {}", job_id);

    Ok(())
}

/// Deploy a dataset via the admin API.
///
/// POSTs to the versioned `/datasets/{name}/versions/{version}/dump` endpoint
/// and returns the job ID.
#[tracing::instrument(skip_all, fields(%dataset_ref, ?end_block, %parallelism))]
async fn deploy_dataset(
    admin_url: &Url,
    dataset_ref: &Reference,
    end_block: Option<EndBlock>,
    parallelism: u16,
) -> Result<JobId, Error> {
    let name = dataset_ref.name();
    let version = dataset_ref.revision();

    // Build URL for versioned dump endpoint
    let url = admin_url
        .join(&format!("datasets/{}/versions/{}/dump", name, version))
        .map_err(|err| {
            tracing::error!(admin_url = %admin_url, error = %err, "Invalid admin URL");
            Error::InvalidAdminUrl {
                url: admin_url.to_string(),
                source: err,
            }
        })?;

    tracing::debug!("Sending deployment request");

    let client = reqwest::Client::new();
    let response = client
        .post(url.as_str())
        .json(&DumpRequest {
            end_block,
            parallelism,
        })
        .send()
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "Network error during API request");
            Error::NetworkError {
                url: url.to_string(),
                source: err,
            }
        })?;

    let status = response.status();
    tracing::debug!(status = %status, "Received API response");

    match status.as_u16() {
        200 => {
            let dump_response = response.json::<DumpResponse>().await.map_err(|err| {
                tracing::error!(
                    status = %status,
                    error = %err,
                    "Failed to parse success response from API"
                );
                Error::UnexpectedResponse {
                    status: status.as_u16(),
                    message: format!("Failed to parse response: {}", err),
                }
            })?;

            tracing::info!(job_id = %dump_response.job_id, "Dataset deployment job scheduled");
            Ok(dump_response.job_id)
        }
        400 | 404 | 500 => {
            let error_response = response.json::<ErrorResponse>().await.map_err(|err| {
                tracing::error!(
                    status = %status,
                    error = %err,
                    "Failed to parse error response from API"
                );
                Error::UnexpectedResponse {
                    status: status.as_u16(),
                    message: format!("Failed to parse error response: {}", err),
                }
            })?;

            tracing::error!(
                status = %status,
                error_code = %error_response.error_code,
                error_message = %error_response.error_message,
                "API returned error response"
            );

            Err(Error::ApiError {
                status: status.as_u16(),
                error_code: error_response.error_code,
                message: error_response.error_message,
            })
        }
        _ => {
            tracing::error!(status = %status, "Unexpected status code from API");
            Err(Error::UnexpectedResponse {
                status: status.as_u16(),
                message: format!("Unexpected status code: {}", status),
            })
        }
    }
}

/// Request body for the POST /datasets/{name}/versions/{version}/dump endpoint.
#[derive(Debug, serde::Serialize)]
struct DumpRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    end_block: Option<EndBlock>,
    parallelism: u16,
}

/// Response from the dump endpoint.
#[derive(Debug, serde::Deserialize)]
struct DumpResponse {
    job_id: JobId,
}

/// Error response from the admin API (400/404/500 status codes).
#[derive(Debug, serde::Deserialize)]
struct ErrorResponse {
    error_code: String,
    error_message: String,
}

/// Errors for dataset deployment operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid admin URL
    #[error("invalid admin URL '{url}'")]
    InvalidAdminUrl {
        url: String,
        source: url::ParseError,
    },

    /// API returned an error response
    #[error("API error ({status}): [{error_code}] {message}")]
    ApiError {
        status: u16,
        error_code: String,
        message: String,
    },

    /// Network or connection error
    #[error("network error connecting to {url}")]
    NetworkError { url: String, source: reqwest::Error },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}
