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

use monitoring::logging;

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

/// Inspect a provider by retrieving it from the admin API.
///
/// Retrieves provider configuration and displays it as pretty-printed JSON.
///
/// # Errors
///
/// Returns [`Error`] for invalid name, provider not found (404),
/// API errors (400/500), or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, %name))]
pub async fn run(Args { global, name }: Args) -> Result<(), Error> {
    tracing::debug!("Retrieving provider from admin API");

    let provider_json = get_provider(&global, &name).await?;

    // Pretty-print the provider JSON to stdout
    println!("{}", provider_json);

    Ok(())
}

/// Retrieve the provider from the admin API.
///
/// GETs from `/providers/{name}` endpoint and returns the provider JSON.
#[tracing::instrument(skip_all)]
async fn get_provider(global: &GlobalArgs, name: &str) -> Result<String, Error> {
    tracing::debug!("Creating API client");

    let client = global.build_client().map_err(Error::ClientBuildError)?;

    let provider_value = client
        .providers()
        .get(name)
        .await
        .map_err(|err| Error::ClientError { source: err })?;

    // Handle None case (404)
    let provider_value = provider_value.ok_or_else(|| Error::ProviderNotFound {
        name: name.to_string(),
    })?;

    // Pretty-print the provider JSON
    let pretty_json = serde_json::to_string_pretty(&provider_value).map_err(|err| {
        tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to pretty-print provider JSON");
        Error::JsonFormattingError { source: err }
    })?;

    Ok(pretty_json)
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
    ClientError {
        #[source]
        source: crate::client::providers::GetError,
    },

    /// Failed to format JSON for display
    #[error("failed to format provider JSON")]
    JsonFormattingError { source: serde_json::Error },
}
