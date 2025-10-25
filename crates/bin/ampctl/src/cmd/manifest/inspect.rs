//! Manifest inspection command.
//!
//! Retrieves and displays a manifest by its content-addressable hash through the admin API by:
//! 1. Making a GET request to admin API `/manifests/{hash}` endpoint
//! 2. Retrieving the raw manifest JSON
//! 3. Pretty-printing the manifest to stdout
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use datasets_common::hash::Hash;
use url::Url;

/// Command-line arguments for the `manifest inspect` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    /// The URL of the engine admin interface
    #[arg(long, env = "AMP_ADMIN_URL", default_value = "http://localhost:1610", value_parser = clap::value_parser!(Url))]
    pub admin_url: Url,

    /// Manifest content hash to retrieve
    #[arg(value_name = "HASH", required = true, value_parser = clap::value_parser!(Hash))]
    pub hash: Hash,
}

/// Inspect a manifest by retrieving it from content-addressable storage via the admin API.
///
/// Retrieves manifest content and displays it as pretty-printed JSON.
///
/// # Errors
///
/// Returns [`Error`] for invalid hash, manifest not found (404),
/// API errors (400/500), or network failures.
#[tracing::instrument(skip_all, fields(%admin_url, %hash))]
pub async fn run(Args { admin_url, hash }: Args) -> Result<(), Error> {
    tracing::debug!("Retrieving manifest from admin API");

    let manifest_json = get_manifest(&admin_url, &hash).await?;

    // Pretty-print the manifest JSON to stdout
    println!("{}", manifest_json);

    Ok(())
}

/// Retrieve the manifest from the admin API.
///
/// GETs from `/manifests/{hash}` endpoint and returns the raw manifest JSON.
#[tracing::instrument(skip_all)]
async fn get_manifest(admin_url: &Url, hash: &Hash) -> Result<String, Error> {
    let url = admin_url
        .join(&format!("manifests/{}", hash))
        .map_err(|err| {
            tracing::error!(admin_url = %admin_url, error = %err, "Invalid admin URL");
            Error::InvalidAdminUrl {
                url: admin_url.to_string(),
                source: err,
            }
        })?;

    tracing::debug!("Sending GET request");

    let client = reqwest::Client::new();
    let response = client.get(url.as_str()).send().await.map_err(|err| {
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
            let manifest_response = response.json::<ManifestResponse>().await.map_err(|err| {
                tracing::error!(error = %err, "Failed to parse manifest response from API");
                Error::UnexpectedResponse {
                    status: status.as_u16(),
                    message: format!("Failed to parse response: {}", err),
                }
            })?;

            // Pretty-print the manifest JSON
            let pretty_json =
                serde_json::to_string_pretty(&manifest_response.manifest).map_err(|err| {
                    tracing::error!(error = %err, "Failed to pretty-print manifest JSON");
                    Error::JsonFormattingError { source: err }
                })?;

            Ok(pretty_json)
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

/// Response body for the GET /manifests/{hash} endpoint (200 success).
#[derive(Debug, serde::Deserialize)]
struct ManifestResponse {
    #[serde(flatten)]
    manifest: serde_json::Value,
}

/// Error response from the admin API (400/404/500 status codes).
#[derive(Debug, serde::Deserialize)]
struct ErrorResponse {
    error_code: String,
    error_message: String,
}

/// Errors for manifest inspection operations.
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

    /// Failed to format JSON for display
    #[error("failed to format manifest JSON")]
    JsonFormattingError { source: serde_json::Error },
}
