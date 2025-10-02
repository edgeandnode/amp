//! Providers delete handler

use axum::{
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use dataset_store::providers::DeleteError;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for the `DELETE /providers/{name}` endpoint
///
/// Deletes a specific provider configuration by its name from the dataset store.
///
/// ## Path Parameters
/// - `name`: The unique name/identifier of the provider to delete
///
/// ## Response
/// - **204 No Content**: Provider successfully deleted
/// - **400 Bad Request**: Invalid provider name format
/// - **404 Not Found**: Provider with the given name does not exist
/// - **500 Internal Server Error**: Store error occurred during deletion
///
/// ## Error Codes
/// - `INVALID_PROVIDER_NAME`: The provided name is invalid or malformed
/// - `PROVIDER_NOT_FOUND`: No provider exists with the given name
/// - `STORE_ERROR`: Failed to delete provider configuration from store
///
/// This handler:
/// - Validates and extracts the provider name from the URL path
/// - Attempts to delete the provider configuration from both store and cache
/// - Returns appropriate HTTP status codes and error messages
///
/// ## Safety Notes
/// - Deletion removes both the configuration file from storage and the cached entry
/// - Once deleted, the provider configuration cannot be recovered
/// - Any datasets using this provider may fail until a new provider is configured
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        delete,
        path = "/providers/{name}",
        tag = "providers",
        operation_id = "providers_delete",
        params(
            ("name" = String, Path, description = "Provider name")
        ),
        responses(
            (status = 204, description = "Provider successfully deleted"),
            (status = 400, description = "Invalid provider name"),
            (status = 404, description = "Provider not found"),
            (status = 500, description = "Internal server error")
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<String>, PathRejection>,
) -> Result<StatusCode, ErrorResponse> {
    let name = match path {
        Ok(Path(name)) => name,
        Err(err) => {
            tracing::debug!(error=?err, "invalid provider name in path");
            return Err(Error::InvalidName { err }.into());
        }
    };

    ctx.dataset_store
        .providers()
        .delete(&name)
        .await
        .map_err(|err| match err {
            DeleteError::NotFound { name } => {
                tracing::debug!(
                    provider_name = %name,
                    "provider not found for deletion"
                );
                Error::NotFound { name }
            }
            other => {
                tracing::error!(
                    provider_name = %name,
                    error = %other,
                    "failed to delete provider"
                );
                Error::StoreError(other)
            }
        })?;

    tracing::info!(
        provider_name = %name,
        "successfully deleted provider configuration"
    );
    Ok(StatusCode::NO_CONTENT)
}

/// Errors that can occur during provider deletion
///
/// This enum represents all possible error conditions that can occur
/// when handling a `DELETE /providers/{name}` request, from path parsing
/// to store operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The provider name in the URL path is invalid
    ///
    /// This occurs when the path parameter cannot be extracted properly,
    /// typically due to URL encoding issues or empty names.
    #[error("invalid provider name: {err}")]
    InvalidName {
        /// The rejection details from Axum's path extractor
        err: PathRejection,
    },

    /// The requested provider was not found in the store
    ///
    /// This occurs when the provider name is valid but no provider
    /// configuration exists with that name in the dataset store.
    #[error("provider '{name}' not found")]
    NotFound {
        /// The provider name that was not found
        name: String,
    },

    /// Failed to delete the provider configuration from the store
    ///
    /// This occurs when the underlying storage operation fails,
    /// such as filesystem errors, permission issues, or other
    /// store-level problems during deletion.
    #[error("failed to delete provider configuration: {0}")]
    StoreError(#[from] DeleteError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidName { .. } => "INVALID_PROVIDER_NAME",
            Error::NotFound { .. } => "PROVIDER_NOT_FOUND",
            Error::StoreError(_) => "STORE_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidName { .. } => StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::StoreError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
