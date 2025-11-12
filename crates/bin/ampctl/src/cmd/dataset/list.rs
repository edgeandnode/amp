//! Datasets listing command.
//!
//! Retrieves and displays all registered datasets through the admin API by:
//! 1. Creating a client for the admin API
//! 2. Using the client's datasets list_all method
//! 3. Displaying the datasets as JSON
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use monitoring::logging;

use crate::{args::GlobalArgs, client};

/// Command-line arguments for the `dataset list` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,
}

/// Result of a dataset list operation.
#[derive(serde::Serialize)]
struct ListResult {
    datasets: Vec<client::datasets::DatasetSummary>,
}

impl std::fmt::Display for ListResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.datasets.is_empty() {
            writeln!(f, "No datasets found")
        } else {
            writeln!(f, "Datasets:")?;
            for dataset in &self.datasets {
                let versions_str = if dataset.versions.is_empty() {
                    "no versions".to_string()
                } else {
                    format!("{} version(s)", dataset.versions.len())
                };
                writeln!(
                    f,
                    "  {}/{} - {}",
                    dataset.namespace, dataset.name, versions_str
                )?;
            }
            Ok(())
        }
    }
}

/// List all datasets by retrieving them from the admin API.
///
/// Retrieves all datasets and displays them based on the output format.
///
/// # Errors
///
/// Returns [`Error`] for API errors (500) or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url))]
pub async fn run(Args { global }: Args) -> Result<(), Error> {
    tracing::debug!("Retrieving datasets from admin API");

    let datasets_response = get_datasets(&global).await?;
    let result = ListResult {
        datasets: datasets_response.datasets,
    };
    global.print(&result).map_err(Error::JsonFormattingError)?;

    Ok(())
}

/// Retrieve all datasets from the admin API.
///
/// Creates a client and uses the datasets list_all method.
#[tracing::instrument(skip_all)]
async fn get_datasets(global: &GlobalArgs) -> Result<client::datasets::DatasetsResponse, Error> {
    let client = global.build_client().map_err(Error::ClientBuildError)?;

    let datasets_response = client.datasets().list_all().await.map_err(|err| {
        tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to list datasets");
        Error::ClientError(err)
    })?;

    Ok(datasets_response)
}

/// Errors for datasets listing operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuildError(#[source] crate::args::BuildClientError),

    /// Client error from the API
    #[error("client error")]
    ClientError(#[source] client::datasets::ListAllError),

    /// Failed to format JSON for display
    #[error("failed to format datasets JSON")]
    JsonFormattingError(#[source] serde_json::Error),
}
