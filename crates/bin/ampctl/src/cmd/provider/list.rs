//! Provider listing command.
//!
//! Retrieves and displays all provider configurations through the admin API by:
//! 1. Making a GET request to admin API `/providers` endpoint
//! 2. Retrieving the list of all provider configurations
//! 3. Displaying the providers as JSON
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use url::Url;

/// Command-line arguments for the `provider ls` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    /// The URL of the engine admin interface
    #[arg(long, env = "AMP_ADMIN_URL", default_value = "http://localhost:1610", value_parser = clap::value_parser!(Url))]
    pub admin_url: Url,
}

/// List all providers by retrieving them from the admin API.
///
/// Retrieves all provider configurations and displays them as JSON.
///
/// # Errors
///
/// Returns [`Error`] for API errors (500) or network failures.
#[tracing::instrument(skip_all, fields(%admin_url))]
pub async fn run(Args { admin_url }: Args) -> Result<(), Error> {
    tracing::debug!("Retrieving providers from admin API");

    let providers = get_providers(&admin_url).await?;

    let json = serde_json::to_string_pretty(&providers).map_err(|err| {
        tracing::error!(error = %err, "Failed to serialize providers to JSON");
        Error::JsonFormattingError { source: err }
    })?;
    println!("{}", json);

    Ok(())
}

/// Retrieve all providers from the admin API.
///
/// GETs from `/providers` endpoint and returns the list of providers.
#[tracing::instrument(skip_all)]
async fn get_providers(admin_url: &Url) -> Result<Vec<ProviderInfo>, Error> {
    let url = admin_url.join("providers").map_err(|err| {
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
            let providers_response = response.json::<ProvidersResponse>().await.map_err(|err| {
                tracing::error!(error = %err, "Failed to parse providers response from API");
                Error::UnexpectedResponse {
                    status: status.as_u16(),
                    message: format!("Failed to parse response: {}", err),
                }
            })?;

            Ok(providers_response.providers)
        }
        500 => {
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

/// Response body for the GET /providers endpoint (200 success).
#[derive(Debug, serde::Deserialize)]
struct ProvidersResponse {
    providers: Vec<ProviderInfo>,
}

/// Provider information from the API.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct ProviderInfo {
    name: String,
    kind: String,
    network: String,
    #[serde(flatten)]
    rest: serde_json::Value,
}

/// Error response from the admin API (500 status code).
#[derive(Debug, serde::Deserialize)]
struct ErrorResponse {
    error_code: String,
    error_message: String,
}

/// Errors for provider listing operations.
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
    #[error("failed to format providers JSON")]
    JsonFormattingError { source: serde_json::Error },
}
