//! Provider removal command.
//!
//! Deletes a provider configuration by its name through the admin API by:
//! 1. Making a DELETE request to admin API `/providers/{name}` endpoint
//! 2. Handling success (204), not found (404), or error responses
//!
//! **Note**: This endpoint removes the provider configuration from storage.
//! Any datasets using this provider may fail until a new provider is configured.
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use url::Url;

/// Command-line arguments for the `provider rm` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    /// The URL of the engine admin interface
    #[arg(long, env = "AMP_ADMIN_URL", default_value = "http://localhost:1610", value_parser = clap::value_parser!(Url))]
    pub admin_url: Url,

    /// Provider name to delete
    #[arg(value_name = "NAME", required = true)]
    pub name: String,
}

/// Remove a provider from the admin API.
///
/// Deletes the provider configuration if it exists.
///
/// # Errors
///
/// Returns [`Error`] for invalid name, provider not found (404),
/// API errors (400/500), or network failures.
#[tracing::instrument(skip_all, fields(%admin_url, %name))]
pub async fn run(Args { admin_url, name }: Args) -> Result<(), Error> {
    tracing::debug!("Deleting provider from admin API");

    delete_provider(&admin_url, &name).await?;

    crate::success!("Provider deleted successfully");

    Ok(())
}

/// Delete the provider from the admin API.
///
/// DELETEs to `/providers/{name}` endpoint.
#[tracing::instrument(skip_all)]
async fn delete_provider(admin_url: &Url, name: &str) -> Result<(), Error> {
    let url = admin_url
        .join(&format!("providers/{}", urlencoding::encode(name)))
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
            tracing::info!("Provider deleted successfully");
            Ok(())
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

/// Error response from the admin API (400/404/500 status codes).
#[derive(Debug, serde::Deserialize)]
struct ErrorResponse {
    error_code: String,
    error_message: String,
}

/// Errors for provider removal operations.
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
