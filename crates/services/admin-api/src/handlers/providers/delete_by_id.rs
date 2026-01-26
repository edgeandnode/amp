//! Providers delete handler

use amp_providers_registry::DeleteError;
use axum::{
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use monitoring::logging;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for the `DELETE /providers/{name}` endpoint
///
/// Deletes a specific provider configuration by its name from the dataset store.
///
/// This operation is idempotent - deleting a non-existent provider returns success.
///
/// ## Path Parameters
/// - `name`: The unique name/identifier of the provider to delete
///
/// ## Response
/// - **204 No Content**: Provider successfully deleted (or did not exist)
/// - **400 Bad Request**: Invalid provider name format
/// - **500 Internal Server Error**: Store error occurred during deletion
///
/// ## Error Codes
/// - `INVALID_PROVIDER_NAME`: The provided name is invalid or malformed
/// - `STORE_ERROR`: Failed to delete provider configuration from store
///
/// This handler:
/// - Validates and extracts the provider name from the URL path
/// - Attempts to delete the provider configuration from both store and cache
/// - Returns 204 even if the provider does not exist (idempotent behavior)
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
            (status = 204, description = "Provider successfully deleted (or did not exist)"),
            (status = 400, description = "Invalid provider name", body = crate::handlers::error::ErrorResponse),
            (status = 500, description = "Internal server error", body = crate::handlers::error::ErrorResponse)
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
            tracing::debug!(error = %err, error_source = logging::error_source(&err), "invalid provider name in path");
            return Err(Error::InvalidName { err }.into());
        }
    };

    ctx.providers_registry.delete(&name).await.map_err(|err| {
        tracing::error!(
            provider_name = %name,
            error = %err, error_source = logging::error_source(&err),
            "failed to delete provider"
        );
        Error::StoreError(err)
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

    /// Failed to delete the provider configuration from the store
    ///
    /// This occurs when the underlying storage operation fails,
    /// such as filesystem errors, permission issues, or other
    /// store-level problems during deletion.
    #[error("failed to delete provider configuration: {0}")]
    StoreError(#[source] DeleteError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidName { .. } => "INVALID_PROVIDER_NAME",
            Error::StoreError(_) => "STORE_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidName { .. } => StatusCode::BAD_REQUEST,
            Error::StoreError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
