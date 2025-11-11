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

use monitoring::logging;

use crate::{args::GlobalArgs, client};

/// Command-line arguments for the `provider ls` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,
}

/// Result wrapper for providers list output.
#[derive(serde::Serialize)]
struct ListResult {
    providers: Vec<client::providers::ProviderInfo>,
}

impl std::fmt::Display for ListResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.providers.is_empty() {
            writeln!(f, "No providers found")
        } else {
            writeln!(f, "Providers:")?;
            for provider in &self.providers {
                writeln!(
                    f,
                    "  {} ({}) - {}",
                    provider.name, provider.kind, provider.network
                )?;
            }
            Ok(())
        }
    }
}

/// List all providers by retrieving them from the admin API.
///
/// Retrieves all provider configurations and displays them based on the output format.
///
/// # Errors
///
/// Returns [`Error`] for API errors (500) or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url))]
pub async fn run(Args { global }: Args) -> Result<(), Error> {
    tracing::debug!("Retrieving providers from admin API");

    let providers = get_providers(&global).await?;
    let result = ListResult { providers };
    global.print(&result).map_err(Error::JsonFormattingError)?;

    Ok(())
}

/// Retrieve all providers from the admin API.
///
/// Creates a client and uses the providers list method.
#[tracing::instrument(skip_all)]
async fn get_providers(global: &GlobalArgs) -> Result<Vec<client::providers::ProviderInfo>, Error> {
    let client = global.build_client().map_err(Error::ClientBuildError)?;

    let providers = client.providers().list().await.map_err(|err| {
        tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to list providers");
        Error::ClientError(err)
    })?;

    Ok(providers)
}

/// Errors for provider listing operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuildError(#[source] crate::args::BuildClientError),

    /// Client error from the API
    #[error("client error")]
    ClientError(#[source] client::providers::ListError),

    /// Failed to format JSON for display
    #[error("failed to format providers JSON")]
    JsonFormattingError(#[source] serde_json::Error),
}
