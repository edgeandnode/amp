//! Providers get all handler

use axum::{Json, extract::State};

use super::provider_info::ProviderInfo;
use crate::ctx::Ctx;

/// Handler for the `GET /providers` endpoint
///
/// Retrieves and returns complete information for all provider configurations from the dataset store.
///
/// ## Security Note
///
/// This endpoint returns the **complete provider configuration** including all configuration
/// details stored in the provider files. Ensure that sensitive information such as API keys,
/// connection strings, and credentials are not stored in provider configuration files or
/// are properly filtered before storage.
///
/// ## Response
/// - **200 OK**: Returns provider metadata as JSON
///
/// This handler:
/// - Accesses cached provider configurations from the dataset store
/// - Transforms available provider configurations to API response format including full configuration
/// - Cannot fail as it returns cached data; any store/parsing errors are logged during cache loading (explicit via `load_into_cache()` or lazy-loaded on first access)
/// - Filters out providers that cannot be converted to valid API format (conversion errors are logged)
#[tracing::instrument(skip_all)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        get,
        path = "/providers",
        tag = "providers",
        operation_id = "providers_list",
        responses(
            (status = 200, description = "Successfully retrieved all providers", body = ProvidersResponse)
        )
    )
)]
pub async fn handler(State(ctx): State<Ctx>) -> Json<ProvidersResponse> {
    let providers = ctx
        .dataset_store
        .providers()
        .get_all()
        .await
        .iter()
        .filter_map(
            |(name, config)| match ProviderInfo::try_from((name.clone(), config.clone())) {
                Ok(provider_info) => Some(provider_info),
                Err(err) => {
                    tracing::warn!(
                        provider_name = %name,
                        error = %err,
                        "failed to convert provider config to info, skipping"
                    );
                    None
                }
            },
        )
        .collect();

    Json(ProvidersResponse { providers })
}

/// API response containing complete provider information
///
/// This response structure provides all provider configurations
/// available in the system, including their full configuration details.
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct ProvidersResponse {
    /// List of all provider configurations with complete configuration details
    pub providers: Vec<ProviderInfo>,
}
