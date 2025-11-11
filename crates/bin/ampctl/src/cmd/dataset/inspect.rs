//! Dataset inspect command.
//!
//! Retrieves and displays detailed information about a specific dataset through the admin API by:
//! 1. Creating a client for the admin API
//! 2. Using the client's dataset get method
//! 3. Displaying the dataset as JSON
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use datasets_common::reference::Reference;
use monitoring::logging;

use crate::{args::GlobalArgs, client};

/// Command-line arguments for the `dataset inspect` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// The dataset reference to inspect (format: namespace/name@revision)
    /// If no revision is specified, "latest" is used
    pub reference: Reference,
}

/// Result of a dataset inspect operation.
#[derive(serde::Serialize)]
struct InspectResult {
    #[serde(flatten)]
    dataset: client::datasets::DatasetInfo,
}

impl std::fmt::Display for InspectResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(f, "Namespace: {}", self.dataset.namespace)?;
        writeln!(f, "Name: {}", self.dataset.name)?;
        writeln!(f, "Revision: {}", self.dataset.revision)?;
        writeln!(f, "Kind: {}", self.dataset.kind)?;
        writeln!(f, "Manifest Hash: {}", self.dataset.manifest_hash)?;
        Ok(())
    }
}

/// Inspect dataset details by retrieving them from the admin API.
///
/// Retrieves dataset information and displays it based on the output format.
///
/// # Errors
///
/// Returns [`Error`] for API errors (400/404/500) or network failures.
#[tracing::instrument(skip_all, fields(admin_url = %global.admin_url, reference = %reference))]
pub async fn run(Args { global, reference }: Args) -> Result<(), Error> {
    tracing::debug!("Retrieving dataset from admin API");

    let dataset = get_dataset(&global, &reference).await?;
    let result = InspectResult { dataset };
    global.print(&result).map_err(Error::JsonFormattingError)?;

    Ok(())
}

/// Retrieve a dataset from the admin API.
///
/// Creates a client and uses the dataset get method.
#[tracing::instrument(skip_all)]
async fn get_dataset(
    global: &GlobalArgs,
    reference: &Reference,
) -> Result<client::datasets::DatasetInfo, Error> {
    let client = global.build_client().map_err(Error::ClientBuildError)?;

    let dataset = client.datasets().get(reference).await.map_err(|err| {
        tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to get dataset");
        Error::ClientError(err)
    })?;

    match dataset {
        Some(dataset) => Ok(dataset),
        None => Err(Error::DatasetNotFound {
            reference: reference.clone(),
        }),
    }
}

/// Errors for dataset inspect operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuildError(#[source] crate::args::BuildClientError),

    /// Client error from the API
    #[error("client error")]
    ClientError(#[source] client::datasets::GetError),

    /// Dataset not found
    #[error("dataset not found: {reference}")]
    DatasetNotFound { reference: Reference },

    /// Failed to format JSON for display
    #[error("failed to format dataset JSON")]
    JsonFormattingError(#[source] serde_json::Error),
}
