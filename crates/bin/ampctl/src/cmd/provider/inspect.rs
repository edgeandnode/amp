//! Provider inspection command.
//!
//! Retrieves and displays a provider configuration by its name through the admin API by:
//! 1. Making a GET request to admin API `/providers/{name}` endpoint
//! 2. Retrieving the provider configuration JSON
//! 3. Pretty-printing the provider to stdout
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use crate::args::GlobalArgs;

/// Command-line arguments for the `provider inspect` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// Provider name to retrieve
    #[arg(value_name = "NAME", required = true)]
    pub name: String,
}

/// Result wrapper for provider inspect output.
#[derive(serde::Serialize)]
struct InspectResult {
    #[serde(flatten)]
    data: serde_json::Value,
}

impl std::fmt::Display for InspectResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // For provider inspect, output pretty JSON as provider configs are complex nested structures
        let json = serde_json::to_string_pretty(&self.data).map_err(|_| std::fmt::Error)?;
        write!(f, "{}", json)
    }
}

/// Inspect a provider by retrieving it from the admin API.
///
/// Retrieves provider configuration and displays it based on the output format.
///
/// # Errors
///
/// Returns [`Error`] for invalid name, provider not found (404),
/// API errors (400/500), or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, %name))]
pub async fn run(Args { global, name }: Args) -> Result<(), Error> {
    tracing::debug!("Retrieving provider from admin API");

    let provider_value = get_provider_value(&global, &name).await?;
    let result = InspectResult {
        data: provider_value,
    };

    global.print(&result).map_err(Error::JsonFormattingError)?;

    Ok(())
}

/// Retrieve the provider from the admin API.
///
/// GETs from `/providers/{name}` endpoint and returns the provider value.
#[tracing::instrument(skip_all)]
async fn get_provider_value(global: &GlobalArgs, name: &str) -> Result<serde_json::Value, Error> {
    tracing::debug!("Creating API client");

    let client = global.build_client().map_err(Error::ClientBuildError)?;

    let provider_value = client
        .providers()
        .get(name)
        .await
        .map_err(Error::ClientError)?;

    // Handle None case (404)
    let provider_value = provider_value.ok_or_else(|| Error::ProviderNotFound {
        name: name.to_string(),
    })?;

    Ok(provider_value)
}

/// Errors for provider inspection operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Provider not found (404)
    #[error("provider '{name}' not found")]
    ProviderNotFound { name: String },

    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuildError(#[source] crate::args::BuildClientError),

    /// Client error from the API
    #[error("client error")]
    ClientError(#[source] crate::client::providers::GetError),

    /// Failed to format JSON for display
    #[error("failed to format provider JSON")]
    JsonFormattingError(#[source] serde_json::Error),
}
