use axum::{
    Json,
    extract::{State, rejection::JsonRejection},
    http::StatusCode,
};
use common::BoxError;
use dataset_store::RegisterManifestError;
use datasets_common::{manifest::Manifest as CommonManifest, name::Name, version::Version};
use datasets_derived::{DATASET_KIND as DERIVED_DATASET_KIND, Manifest as DerivedDatasetManifest};
use evm_rpc_datasets::{DATASET_KIND as EVM_RPC_DATASET_KIND, Manifest as EvmRpcManifest};

use crate::{
    ctx::Ctx,
    handlers::{
        common::NonEmptyString,
        error::{ErrorResponse, IntoErrorResponse},
    },
};

/// Handler for the `POST /datasets` endpoint
///
/// Registers a new dataset configuration in the server's local registry. Accepts a JSON payload
/// containing the dataset registration configuration.
///
/// **Note**: This endpoint only registers datasets and does NOT schedule data extraction.
/// To extract data after registration, make a separate call to:
/// - `POST /datasets/{name}/dump` - for latest version
/// - `POST /datasets/{name}/versions/{version}/dump` - for specific version
///
/// ## Request Body
/// - `dataset_name`: Name of the dataset to be registered (must be valid dataset name)
/// - `version`: Version of the dataset to register (must be valid version string)
/// - `manifest`: JSON string representation of the dataset manifest
///
/// ## Response
/// - **201 Created**: Dataset successfully registered
/// - **400 Bad Request**: Invalid dataset name, version, or manifest format
/// - **409 Conflict**: Dataset already exists with provided manifest, or manifest required but not provided
/// - **500 Internal Server Error**: Database or object store error
///
/// ## Error Codes
/// - `INVALID_PAYLOAD_FORMAT`: Request JSON is malformed or invalid
/// - `INVALID_MANIFEST`: Manifest JSON parsing or structure error
/// - `MANIFEST_VALIDATION_ERROR`: Manifest name/version doesn't match request parameters
/// - `MANIFEST_REGISTRATION_ERROR`: Failed to register manifest in system
/// - `DATASET_ALREADY_EXISTS`: Dataset with same name and version already exists
/// - `UNSUPPORTED_DATASET_KIND`: Dataset kind is not "manifest" or "evm-rpc" (only derived and evm-rpc datasets supported)
/// - `STORE_ERROR`: Failed to load or access dataset store
///
/// ## Behavior
/// This handler supports derived and evm-rpc dataset registration:
/// - **Derived dataset** (kind="manifest"): Registers a derived dataset manifest that transforms data from other datasets using SQL queries
/// - **EVM-RPC dataset** (kind="evm-rpc"): Registers a raw dataset that extracts blockchain data directly from Ethereum-compatible JSON-RPC endpoints
/// - **Other raw datasets** (firehose, substreams, etc.) are **not supported** and will return an error
/// - **Legacy SQL datasets** are **not supported** and will return an error
///
/// Both dataset types are registered using the same underlying `register_manifest` method to ensure consistency.
///
/// The handler:
/// - Validates dataset name and version format
/// - Checks that dataset kind is "manifest" or "evm-rpc"
/// - Attempts to load existing dataset from store
/// - Handles manifest registration in the server's local registry
/// - Returns appropriate status codes and error messages
///
/// ## Typical Workflow
/// For users wanting both registration and data extraction:
/// 1. `POST /datasets` - Register the dataset (this endpoint)
/// 2. `POST /datasets/{name}/dump` or `POST /datasets/{name}/versions/{version}/dump` - Schedule data extraction
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        post,
        path = "/datasets",
        tag = "datasets",
        operation_id = "datasets_register",
        request_body = RegisterRequest,
        responses(
            (status = 201, description = "Dataset successfully registered"),
            (status = 400, description = "Invalid request format or manifest"),
            (status = 409, description = "Dataset already exists"),
            (status = 500, description = "Internal server error")
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    payload: Result<Json<RegisterRequest>, JsonRejection>,
) -> Result<StatusCode, ErrorResponse> {
    let payload = match payload {
        Ok(Json(payload)) => payload,
        Err(err) => {
            tracing::error!("Failed to parse request JSON: {}", err);
            return Err(Error::InvalidPayloadFormat.into());
        }
    };

    // Early check if dataset already exists in the store to avoid unnecessary processing
    let dataset_exists = ctx
        .dataset_store
        .is_registered(&payload.name, &payload.version)
        .await
        .map_err(|err| {
            tracing::error!(
                name = %payload.name,
                version = %payload.version,
                error = ?err,
                "Failed to check dataset existence in store"
            );
            Error::StoreError(err.into())
        })?;

    // Check if dataset already exists with this name and version
    if dataset_exists {
        return Err(Error::DatasetAlreadyExists(
            payload.name.to_string(),
            payload.version.to_string(),
        )
        .into());
    }

    let manifest =
        serde_json::from_str::<CommonManifest>(payload.manifest.as_str()).map_err(|err| {
            tracing::error!(
                name = %payload.name,
                version = %payload.version,
                error = ?err,
                "Failed to parse common manifest JSON"
            );
            Error::InvalidManifest(err)
        })?;

    // Validate that the manifest name and version match the request parameters
    if manifest.name != payload.name || manifest.version != payload.version {
        return Err(Error::ManifestValidationError(
            manifest.name.to_string(),
            manifest.version.to_string(),
        )
        .into());
    }

    match manifest.kind.as_str() {
        DERIVED_DATASET_KIND => {
            let manifest: DerivedDatasetManifest = serde_json::from_str(payload.manifest.as_str())
                .map_err(|err| {
                    tracing::error!(
                        name = %payload.name,
                        version = %payload.version,
                        kind = DERIVED_DATASET_KIND,
                        error = ?err,
                        "Failed to parse derived dataset manifest JSON"
                    );
                    Error::InvalidManifest(err)
                })?;

            ctx.dataset_store
                .register_manifest(&manifest.name, &manifest.version, &manifest)
                .await
                .map_err(|err| {
                    tracing::error!(
                        name = %manifest.name,
                        version = %manifest.version,
                        kind = DERIVED_DATASET_KIND,
                        error = ?err,
                        "Failed to register derived dataset manifest"
                    );
                    Error::ManifestRegistrationError(err)
                })?;

            tracing::info!(
                "Registered manifest for derived dataset '{}' version '{}'",
                manifest.name,
                manifest.version
            );
        }
        EVM_RPC_DATASET_KIND => {
            // Validate evm-rpc dataset definition structure
            let manifest: EvmRpcManifest = serde_json::from_str(payload.manifest.as_str())
                .map_err(|err| {
                    tracing::error!(
                        name = %payload.name,
                        version = %payload.version,
                        kind = EVM_RPC_DATASET_KIND,
                        error = ?err,
                        "Failed to parse evm-rpc dataset manifest JSON"
                    );
                    Error::InvalidManifest(err)
                })?;

            ctx.dataset_store
                .register_manifest(&manifest.name, &manifest.version, &manifest)
                .await
                .map_err(|err| {
                    tracing::error!(
                        name = %manifest.name,
                        version = %manifest.version,
                        kind = EVM_RPC_DATASET_KIND,
                        error = ?err,
                        "Failed to register evm-rpc dataset manifest"
                    );
                    Error::ManifestRegistrationError(err)
                })?;

            tracing::info!(
                "Registered manifest for evm-rpc dataset '{}' version '{}'",
                manifest.name,
                manifest.version
            );
        }
        _ => {
            return Err(Error::UnsupportedDatasetKind(manifest.kind.clone()).into());
        }
    }

    Ok(StatusCode::CREATED)
}

/// Request payload for dataset registration
///
/// Contains the dataset name, version, and manifest.
/// The manifest will be registered in the local registry.
#[derive(serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct RegisterRequest {
    /// Name of the dataset to be registered (validated identifier format)
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub name: Name,
    /// Version of the dataset to register using semantic versioning (e.g., "1.0.0")
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub version: Version,
    /// JSON string representation of the dataset manifest (required)
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub manifest: NonEmptyString,
}

/// Errors that can occur during dataset registration
///
/// This enum represents all possible error conditions when handling
/// a request to register a dataset in the local registry.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid request format
    ///
    /// This occurs when:
    /// - Request JSON is malformed or invalid
    /// - Required fields are missing or have wrong types
    /// - Dataset name or version format is invalid
    #[error("invalid request format")]
    InvalidPayloadFormat,

    /// Invalid derived dataset manifest content or structure
    ///
    /// This occurs when:
    /// - Manifest JSON is malformed or invalid
    /// - Manifest structure doesn't match expected schema
    /// - Required manifest fields are missing or invalid
    #[error("invalid manifest: {0}")]
    InvalidManifest(#[from] serde_json::Error),

    /// Manifest validation error - name/version mismatch
    ///
    /// This occurs when:
    /// - Manifest name doesn't match the request dataset_name
    /// - Manifest version doesn't match the request version
    /// - Manifest and request parameters are inconsistent
    #[error("Manifest name '{0}' and version '{1}' do not match with manifest")]
    ManifestValidationError(String, String),

    /// Failed to register manifest in the system
    ///
    /// This occurs when:
    /// - Error during manifest processing or storage
    /// - Registry information extraction failed
    /// - System-level registration errors
    #[error("Failed to register manifest: {0}")]
    ManifestRegistrationError(#[from] RegisterManifestError),

    /// Unsupported dataset kind
    ///
    /// This occurs when:
    /// - Dataset kind is not "manifest" or "evm-rpc" (only derived and evm-rpc datasets are supported)
    #[error(
        "unsupported kind '{0}' - only derived datasets (kind='manifest') and evm-rpc datasets (kind='evm-rpc') are supported for registration"
    )]
    UnsupportedDatasetKind(String),

    /// Dataset already exists with the given configuration
    ///
    /// This occurs when:
    /// - Dataset with same name/version already exists
    /// - Attempt to register existing dataset with new manifest
    /// - Conflicting registration attempts
    #[error("Dataset '{0}' version '{1}' already exists")]
    DatasetAlreadyExists(String, String),

    /// Dataset store error
    ///
    /// This occurs when:
    /// - Failed to load dataset from store
    /// - Dataset store configuration errors
    /// - Dataset store connectivity issues
    #[error("dataset store error: {0}")]
    StoreError(#[source] BoxError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidPayloadFormat => "INVALID_PAYLOAD_FORMAT",
            Error::InvalidManifest(_) => "INVALID_MANIFEST",
            Error::ManifestValidationError(_, _) => "MANIFEST_VALIDATION_ERROR",
            Error::ManifestRegistrationError(_) => "MANIFEST_REGISTRATION_ERROR",
            Error::DatasetAlreadyExists(_, _) => "DATASET_ALREADY_EXISTS",
            Error::StoreError(_) => "STORE_ERROR",
            Error::UnsupportedDatasetKind(_) => "UNSUPPORTED_DATASET_KIND",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidPayloadFormat => StatusCode::BAD_REQUEST,
            Error::InvalidManifest(_) => StatusCode::BAD_REQUEST,
            Error::ManifestValidationError(_, _) => StatusCode::BAD_REQUEST,
            Error::ManifestRegistrationError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::DatasetAlreadyExists(_, _) => StatusCode::CONFLICT,
            Error::StoreError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::UnsupportedDatasetKind(_) => StatusCode::BAD_REQUEST,
        }
    }
}
