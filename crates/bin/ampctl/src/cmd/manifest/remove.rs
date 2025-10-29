//! Manifest removal command.
//!
//! Deletes a manifest by its content-addressable hash through the admin API by:
//! 1. Making a DELETE request to admin API `/manifests/{hash}` endpoint
//! 2. Handling success (204), conflict (409), or error responses
//!
//! **Note**: Manifests linked to datasets cannot be deleted (returns 409 Conflict).
//! This endpoint is idempotent: deleting a non-existent manifest returns success.
//!
//! # Configuration
//!
//! - Admin URL: `--admin-url` flag or `AMP_ADMIN_URL` env var (default: `http://localhost:1610`)
//! - Logging: `AMP_LOG` env var (`error`, `warn`, `info`, `debug`, `trace`)

use datasets_common::hash::Hash;
use url::Url;

use crate::client::manifests::DeleteError;

/// Command-line arguments for the `manifest rm` command.
#[derive(Debug, clap::Args)]
pub struct Args {
    /// The URL of the engine admin interface
    #[arg(long, env = "AMP_ADMIN_URL", default_value = "http://localhost:1610", value_parser = clap::value_parser!(Url))]
    pub admin_url: Url,

    /// Bearer token for authenticating requests to the admin API
    #[arg(long, env = "AMP_AUTH_TOKEN")]
    pub auth_token: Option<String>,

    /// Manifest content hash to delete
    #[arg(value_name = "HASH", required = true, value_parser = clap::value_parser!(Hash))]
    pub hash: Hash,
}

/// Remove a manifest from content-addressable storage via the admin API.
///
/// Deletes the manifest if it is not linked to any datasets.
///
/// # Errors
///
/// Returns [`Error`] for invalid hash, manifest is linked to datasets (409),
/// API errors (400/500), or network failures.
#[tracing::instrument(skip_all, fields(%admin_url, %hash))]
pub async fn run(
    Args {
        admin_url,
        auth_token,
        hash,
    }: Args,
) -> Result<(), Error> {
    tracing::debug!("Deleting manifest from admin API");

    delete_manifest(&admin_url, auth_token.as_deref(), &hash).await?;

    crate::success!("Manifest deleted successfully");

    Ok(())
}

/// Delete the manifest from the admin API.
///
/// DELETEs to `/manifests/{hash}` endpoint using the admin API client.
#[tracing::instrument(skip_all)]
async fn delete_manifest(
    admin_url: &Url,
    auth_token: Option<&str>,
    hash: &Hash,
) -> Result<(), Error> {
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

    client
        .manifests()
        .delete(hash)
        .await
        .map_err(|err| match err {
            DeleteError::InvalidHash(source) => Error::ApiError {
                error_code: source.error_code,
                message: source.error_message,
            },
            DeleteError::ManifestLinked(source) => Error::ApiError {
                error_code: source.error_code,
                message: source.error_message,
            },
            DeleteError::TransactionBeginError(source) => Error::ApiError {
                error_code: source.error_code,
                message: source.error_message,
            },
            DeleteError::CheckLinksError(source) => Error::ApiError {
                error_code: source.error_code,
                message: source.error_message,
            },
            DeleteError::MetadataDbDeleteError(source) => Error::ApiError {
                error_code: source.error_code,
                message: source.error_message,
            },
            DeleteError::ObjectStoreDeleteError(source) => Error::ApiError {
                error_code: source.error_code,
                message: source.error_message,
            },
            DeleteError::TransactionCommitError(source) => Error::ApiError {
                error_code: source.error_code,
                message: source.error_message,
            },
            DeleteError::Network { url, source } => Error::NetworkError { url, source },
            DeleteError::UnexpectedResponse { status, message } => {
                Error::UnexpectedResponse { status, message }
            }
        })
}

/// Errors for manifest removal operations.
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

    /// API returned an error response
    #[error("API error: [{error_code}] {message}")]
    ApiError { error_code: String, message: String },

    /// Network or connection error
    #[error("network error connecting to {url}")]
    NetworkError { url: String, source: reqwest::Error },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}
