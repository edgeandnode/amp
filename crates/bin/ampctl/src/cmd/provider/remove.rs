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

use crate::client::{Client, providers::DeleteError};

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
/// DELETEs to `/providers/{name}` endpoint using the API client.
#[tracing::instrument(skip_all)]
async fn delete_provider(admin_url: &Url, name: &str) -> Result<(), Error> {
    tracing::debug!("Creating API client");

    let client = Client::new(admin_url.clone());

    client
        .providers()
        .delete(name)
        .await
        .map_err(|err| match err {
            DeleteError::InvalidName(source) => Error::InvalidName {
                error_code: source.error_code,
                message: source.error_message,
            },
            DeleteError::NotFound(source) => Error::NotFound {
                error_code: source.error_code,
                message: source.error_message,
            },
            DeleteError::StoreError(source) => Error::StoreError {
                error_code: source.error_code,
                message: source.error_message,
            },
            DeleteError::Network { url, source } => Error::NetworkError { url, source },
            DeleteError::UnexpectedResponse { status, message } => {
                Error::UnexpectedResponse { status, message }
            }
        })
}

/// Errors for provider removal operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid provider name
    #[error("invalid provider name: [{error_code}] {message}")]
    InvalidName { error_code: String, message: String },

    /// Provider not found
    #[error("provider not found: [{error_code}] {message}")]
    NotFound { error_code: String, message: String },

    /// Store error
    #[error("store error: [{error_code}] {message}")]
    StoreError { error_code: String, message: String },

    /// Network or connection error
    #[error("network error connecting to {url}")]
    NetworkError { url: String, source: reqwest::Error },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}
