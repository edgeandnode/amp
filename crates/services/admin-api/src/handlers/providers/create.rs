//! Provider create handler

use axum::{
    Json,
    extract::{State, rejection::JsonRejection},
    http::StatusCode,
};
use dataset_store::providers::{ProviderConfig, RegisterError};

use super::{convert, provider_info::ProviderInfo};
use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for the `POST /providers` endpoint
///
/// Creates a new provider configuration and stores it in the dataset store.
///
/// ## Request Body
/// - JSON object containing provider configuration with required fields:
///   - `name`: The unique identifier for the provider
///   - `kind`: The type of provider (e.g., "evm-rpc", "firehose", "substreams")
///   - `network`: The blockchain network (e.g., "mainnet", "goerli", "polygon")
///   - Additional provider-specific configuration fields as needed
///
/// ## Response
/// - **201 Created**: Provider created successfully
/// - **400 Bad Request**: Invalid request body or provider configuration
/// - **409 Conflict**: Provider with the same name already exists
/// - **500 Internal Server Error**: Store error
///
/// ## Error Codes
/// - `INVALID_REQUEST_BODY`: Malformed JSON request body
/// - `DATA_CONVERSION_ERROR`: Failed to convert JSON to TOML format
/// - `PROVIDER_CONFLICT`: Provider name already exists
/// - `STORE_ERROR`: Failed to save provider configuration
///
/// This handler:
/// - Validates and extracts the provider data from the JSON request body
/// - Converts additional JSON configuration fields to TOML format
/// - Registers the provider configuration in the dataset store
/// - Returns HTTP 201 on successful creation
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        post,
        path = "/providers",
        tag = "providers",
        operation_id = "providers_create",
        request_body = ProviderInfo,
        responses(
            (status = 201, description = "Provider created successfully"),
            (status = 400, description = "Invalid request body or provider configuration"),
            (status = 409, description = "Provider with the same name already exists"),
            (status = 500, description = "Internal server error")
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    body: Result<Json<ProviderInfo>, JsonRejection>,
) -> Result<StatusCode, ErrorResponse> {
    let provider_info = match body {
        Ok(Json(provider_info)) => provider_info,
        Err(err) => {
            tracing::debug!(error=?err, "invalid JSON in request body");
            return Err(Error::InvalidRequestBody { err }.into());
        }
    };

    let provider_name = provider_info.name.to_string();
    let provider_rest_table =
        convert::json_map_to_toml_table(provider_info.rest).map_err(Error::ConversionError)?;
    let provider_config = ProviderConfig {
        name: provider_info.name.to_string(),
        kind: provider_info.kind,
        network: provider_info.network.to_string(),
        rest: provider_rest_table,
    };

    ctx.dataset_store
        .providers()
        .register(provider_config)
        .await
        .map_err(|err| match err {
            RegisterError::Conflict { name } => {
                tracing::debug!(
                    provider_name = %name,
                    "provider already exists"
                );
                Error::Conflict { name }
            }
            other => {
                tracing::error!(
                    %provider_name,
                    error = %other,
                    "failed to register provider"
                );
                Error::StoreError(other)
            }
        })?;

    tracing::info!(
        %provider_name,
        "successfully created provider configuration"
    );

    Ok(StatusCode::CREATED)
}

/// Errors that can occur during provider creation
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The JSON request body is malformed or invalid
    ///
    /// This occurs when:
    /// - The request body is not valid JSON
    /// - Required fields are missing (name, kind, network)
    /// - Field values have incorrect types
    /// - The JSON structure doesn't match the expected schema
    #[error("invalid request body: {err}")]
    InvalidRequestBody {
        /// The rejection details from Axum's JSON extractor
        err: JsonRejection,
    },

    /// Failed to convert JSON to TOML format
    ///
    /// This occurs when the JSON data in the request body cannot be
    /// converted to TOML format, specifically due to:
    /// - **Null values**: TOML doesn't support null/None values
    /// - **Mixed-type arrays**: TOML arrays must contain homogeneous types
    /// - **Complex nested structures**: Some deeply nested JSON structures may not map to TOML
    /// - **Invalid TOML table keys**: Keys that are not valid TOML identifiers
    #[error("failed to convert JSON map to TOML table: {0}")]
    ConversionError(serde_json::Error),

    /// A provider with the same name already exists
    ///
    /// This occurs when attempting to create a provider configuration
    /// with a name that is already in use. Provider names must be unique
    /// within the system.
    #[error("provider '{name}' already exists")]
    Conflict {
        /// The name of the conflicting provider
        name: String,
    },

    /// Failed to store the provider configuration
    ///
    /// This occurs when the underlying storage operation fails,
    /// such as filesystem errors, serialization failures, or
    /// other store-level issues.
    #[error("failed to store provider configuration: {0}")]
    StoreError(#[from] RegisterError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidRequestBody { .. } => "INVALID_REQUEST_BODY",
            Error::ConversionError(_) => "DATA_CONVERSION_ERROR",
            Error::Conflict { .. } => "PROVIDER_CONFLICT",
            Error::StoreError(_) => "STORE_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidRequestBody { .. } => StatusCode::BAD_REQUEST,
            Error::ConversionError(_) => StatusCode::BAD_REQUEST,
            Error::Conflict { .. } => StatusCode::CONFLICT,
            Error::StoreError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
