//! Manifest removal command.
//!
//! Deletes a manifest by its content-addressable hash through the admin API by:
//! 1. Making a DELETE request to admin API `/manifests/{hash}` endpoint
//! 2. Handling success (204), conflict (409), or error responses
//!
//! **Note**: Manifests linked to datasets cannot be deleted (returns 409 Conflict).
//! This endpoint is idempotent: deleting a non-existent manifest returns success.
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use datasets_common::hash::Hash;
use url::Url;

/// Command-line arguments for the `manifest rm` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    /// The URL of the engine admin interface
    #[arg(long, env = "AMP_ADMIN_URL", default_value = "http://localhost:1610", value_parser = clap::value_parser!(Url))]
    pub admin_url: Url,

    /// Manifest content hash to delete
    #[arg(value_name = "HASH", required = true, value_parser = clap::value_parser!(Hash))]
    pub hash: Hash,
}

/// Remove a manifest from content-addressable storage via the admin API.
///
/// Deletes the manifest if it is not linked to any datasets.
///
/// # Errors
///
/// Returns [`Error`] for invalid hash, manifest is linked to datasets (409),
/// API errors (400/500), or network failures.
#[tracing::instrument(skip_all, fields(%admin_url, %hash))]
pub async fn run(Args { admin_url, hash }: Args) -> Result<(), Error> {
    tracing::debug!("Deleting manifest from admin API");

    delete_manifest(&admin_url, &hash).await?;

    crate::success!("Manifest deleted successfully");

    Ok(())
}

/// Delete the manifest from the admin API.
///
/// DELETEs to `/manifests/{hash}` endpoint.
#[tracing::instrument(skip_all)]
async fn delete_manifest(admin_url: &Url, hash: &Hash) -> Result<(), Error> {
    let url = admin_url
        .join(&format!("manifests/{}", hash))
        .map_err(|err| {
            tracing::error!(admin_url = %admin_url, error = %err, "Invalid admin URL");
            Error::InvalidAdminUrl {
                url: admin_url.to_string(),
                source: err,
            }
        })?;

    tracing::debug!("Sending DELETE request");

    let client = reqwest::Client::new();
    let response = client.delete(url.as_str()).send().await.map_err(|err| {
        tracing::error!(error = %err, "Network error during API request");
        Error::NetworkError {
            url: url.to_string(),
            source: err,
        }
    })?;

    let status = response.status();
    tracing::debug!(status = %status, "Received API response");

    match status.as_u16() {
        204 => {
            tracing::info!("Manifest deleted successfully");
            Ok(())
        }
        400 | 409 | 500 => {
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

/// Error response from the admin API (400/409/500 status codes).
#[derive(Debug, serde::Deserialize)]
struct ErrorResponse {
    error_code: String,
    error_message: String,
}

/// Errors for manifest removal operations.
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
