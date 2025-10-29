//! Provider listing command.
//!
//! Retrieves and displays all provider configurations through the admin API by:
//! 1. Creating a client for the admin API
//! 2. Using the client's provider list method
//! 3. Displaying the providers as JSON
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use url::Url;

use crate::client;

/// Command-line arguments for the `provider ls` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    /// The URL of the engine admin interface
    #[arg(long, env = "AMP_ADMIN_URL", default_value = "http://localhost:1610", value_parser = clap::value_parser!(Url))]
    pub admin_url: Url,

    /// Bearer token for authenticating requests to the admin API
    #[arg(long, env = "AMP_AUTH_TOKEN")]
    pub auth_token: Option<String>,
}

/// List all providers by retrieving them from the admin API.
///
/// Retrieves all provider configurations and displays them as JSON.
///
/// # Errors
///
/// Returns [`Error`] for API errors (500) or network failures.
#[tracing::instrument(skip_all, fields(%admin_url))]
pub async fn run(
    Args {
        admin_url,
        auth_token,
    }: Args,
) -> Result<(), Error> {
    tracing::debug!("Retrieving providers from admin API");

    let providers = get_providers(&admin_url, auth_token.as_deref()).await?;

    let json = serde_json::to_string_pretty(&providers).map_err(|err| {
        tracing::error!(error = %err, "Failed to serialize providers to JSON");
        Error::JsonFormattingError { source: err }
    })?;
    println!("{}", json);

    Ok(())
}

/// Retrieve all providers from the admin API.
///
/// Creates a client and uses the providers list method.
#[tracing::instrument(skip_all)]
async fn get_providers(
    admin_url: &Url,
    auth_token: Option<&str>,
) -> Result<Vec<client::providers::ProviderInfo>, Error> {
    let mut client_builder = crate::client::build(admin_url.clone());

    if let Some(token) = auth_token {
        client_builder = client_builder.with_bearer_token(
            token
                .parse()
                .map_err(|err| Error::InvalidAuthToken { source: err })?,
        );
    }

    let client = client_builder
        .build()
        .map_err(|err| Error::ClientBuildError { source: err })?;

    let providers = client.providers().list().await.map_err(|err| {
        tracing::error!(error = %err, "Failed to list providers");
        Error::ClientError { source: err }
    })?;

    Ok(providers)
}

/// Errors for provider listing operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid authentication token
    #[error("invalid authentication token")]
    InvalidAuthToken {
        #[source]
        source: crate::client::auth::BearerTokenError,
    },

    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuildError {
        #[source]
        source: crate::client::BuildError,
    },

    /// Client error from the API
    #[error("client error")]
    ClientError {
        #[source]
        source: client::providers::ListError,
    },

    /// Failed to format JSON for display
    #[error("failed to format providers JSON")]
    JsonFormattingError { source: serde_json::Error },
}
