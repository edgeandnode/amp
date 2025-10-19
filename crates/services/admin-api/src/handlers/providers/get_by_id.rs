//! Providers get by ID handler

use axum::{
    Json,
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};

use super::provider_info::ProviderInfo;
use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for the `GET /providers/{name}` endpoint
///
/// Retrieves and returns complete information for a specific provider configuration by its name from the dataset store.
///
/// ## Security Note
///
/// This endpoint returns the **complete provider configuration** including all configuration
/// details stored in the provider files. Ensure that sensitive information such as API keys,
/// connection strings, and credentials are not stored in provider configuration files or
/// are properly filtered before storage.
///
/// ## Path Parameters
/// - `name`: The unique name/identifier of the provider to retrieve
///
/// ## Response
/// - **200 OK**: Returns the provider metadata as JSON
/// - **400 Bad Request**: Invalid provider name format
/// - **404 Not Found**: Provider with the given name does not exist
///
/// ## Error Codes
/// - `INVALID_PROVIDER_NAME`: The provided name is invalid or malformed
/// - `PROVIDER_NOT_FOUND`: No provider exists with the given name
///
/// This handler:
/// - Validates and extracts the provider name from the URL path
/// - Accesses cached provider configurations from the dataset store
/// - Returns 404 if provider not found in cache; store/parsing errors are logged during cache loading
/// - Converts provider configuration to API response format including full configuration details
///
/// Note: Empty provider names (e.g., `GET /providers/`) are handled by Axum's routing layer
/// and return 404 before reaching this handler, ensuring no conflict with the get_all endpoint.
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        get,
        path = "/providers/{name}",
        tag = "providers",
        operation_id = "providers_get",
        params(
            ("name" = String, Path, description = "Provider name")
        ),
        responses(
            (status = 200, description = "Successfully retrieved provider information", body = ProviderInfo),
            (status = 400, description = "Invalid provider name", body = crate::handlers::error::ErrorResponse),
            (status = 404, description = "Provider not found", body = crate::handlers::error::ErrorResponse),
            (status = 500, description = "Internal server error", body = crate::handlers::error::ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<String>, PathRejection>,
) -> Result<Json<ProviderInfo>, ErrorResponse> {
    let name = match path {
        Ok(Path(name)) => name,
        Err(err) => {
            tracing::debug!(error=?err, "invalid provider name in path");
            return Err(Error::InvalidName { err }.into());
        }
    };

    let Some(config) = ctx.dataset_store.providers().get_by_name(&name).await else {
        tracing::debug!(provider_name = %name, "provider not found");
        return Err(Error::NotFound { name }.into());
    };

    let provider_info = ProviderInfo::try_from((name.clone(), config)).map_err(|err| {
        tracing::error!(
            provider_name = %name,
            error = %err,
            "failed to convert provider config to info"
        );
        Error::ConversionError { name, err }
    })?;

    Ok(Json(provider_info))
}

/// Errors that can occur during provider retrieval
///
/// This enum represents all possible error conditions that can occur
/// when handling a `GET /providers/{name}` request, from path parsing
/// to cache access.
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

    /// The requested provider was not found in the cached store
    ///
    /// This occurs when the provider name is valid but no provider
    /// configuration exists with that name in the cached dataset store.
    ///
    /// Store/parsing errors are logged during cache loading.
    #[error("provider '{name}' not found")]
    NotFound {
        /// The provider name that was not found
        name: String,
    },

    /// Failed to convert provider configuration to info format
    ///
    /// This occurs when the provider configuration cannot be converted
    /// to the API response format, typically due to invalid JSON data
    /// in the provider's configuration.
    #[error("failed to convert provider '{name}' configuration: {err}")]
    ConversionError {
        /// The provider name that failed conversion
        name: String,
        /// The conversion error details
        err: serde_json::Error,
    },
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidName { .. } => "INVALID_PROVIDER_NAME",
            Error::NotFound { .. } => "PROVIDER_NOT_FOUND",
            Error::ConversionError { .. } => "PROVIDER_CONVERSION_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidName { .. } => StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::ConversionError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
