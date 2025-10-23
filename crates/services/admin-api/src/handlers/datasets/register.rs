use axum::{
    Json,
    extract::{State, rejection::JsonRejection},
    http::StatusCode,
};
use common::BoxError;
use dataset_store::{DatasetKind, RegisterManifestError, SetVersionTagError};
use datasets_common::{
    hash::hash, manifest::Manifest as CommonManifest, name::Name, namespace::Namespace,
    version::Version,
};
use datasets_derived::{Manifest as DerivedDatasetManifest, manifest::DependencyValidationError};
use eth_beacon_datasets::Manifest as EthBeaconManifest;
use evm_rpc_datasets::Manifest as EvmRpcManifest;
use firehose_datasets::dataset::Manifest as FirehoseManifest;

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
/// - **201 Created**: Dataset successfully registered (or updated if version tag already exists)
/// - **400 Bad Request**: Invalid dataset name, version, or manifest format
/// - **500 Internal Server Error**: Database or object store error
///
/// ## Error Codes
/// - `INVALID_PAYLOAD_FORMAT`: Request JSON is malformed or invalid
/// - `INVALID_MANIFEST`: Manifest JSON parsing or structure error
/// - `MANIFEST_REGISTRATION_ERROR`: Failed to register manifest in system
/// - `VERSION_TAGGING_ERROR`: Failed to tag the manifest with the version
/// - `UNSUPPORTED_DATASET_KIND`: Dataset kind is not supported
/// - `STORE_ERROR`: Failed to load or access dataset store
///
/// ## Behavior
/// This handler supports multiple dataset kinds for registration:
/// - **Derived dataset** (kind="manifest"): Registers a derived dataset manifest that transforms data from other datasets using SQL queries
/// - **EVM-RPC dataset** (kind="evm-rpc"): Registers a raw dataset that extracts blockchain data directly from Ethereum-compatible JSON-RPC endpoints
/// - **Firehose dataset** (kind="firehose"): Registers a raw dataset that streams blockchain data from StreamingFast Firehose protocol
/// - **Eth Beacon dataset** (kind="eth-beacon"): Registers a raw dataset that extracts Ethereum Beacon Chain data
/// - **Legacy SQL datasets** are **not supported** and will return an error
///
/// ## Registration Process
/// The registration process involves two steps:
/// 1. **Register manifest**: Stores the manifest file in hash-based storage and creates a metadata database entry
/// 2. **Tag version**: Associates the version identifier with the manifest hash (upsert operation)
///
/// This two-step approach enables:
/// - Content-addressable storage by manifest hash
/// - Deduplication of identical manifests
/// - Separation of manifest storage from version management
///
/// The tag operation is idempotent:
/// - If the version tag doesn't exist, it is created
/// - If the version tag exists with the same manifest hash, the operation succeeds (no changes)
/// - If the version tag exists with a different manifest hash, it is updated to point to the new hash
///
/// The handler:
/// - Validates dataset name and version format
/// - Checks that dataset kind is supported
/// - Stores the manifest and upserts the version tag in the server's local registry
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
            (status = 201, description = "Dataset successfully registered or updated"),
            (status = 400, description = "Invalid request format or manifest", body = crate::handlers::error::ErrorResponse),
            (status = 500, description = "Internal server error", body = crate::handlers::error::ErrorResponse)
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

    let manifest =
        serde_json::from_str::<CommonManifest>(payload.manifest.as_str()).map_err(|err| {
            tracing::error!(
                namespace = %payload.namespace,
                name = %payload.name,
                version = %payload.version,
                error = ?err,
                "Failed to parse common manifest JSON"
            );
            Error::InvalidManifest(err)
        })?;

    let dataset_kind = manifest
        .kind
        .parse()
        .map_err(|_| Error::UnsupportedDatasetKind(manifest.kind.clone()))?;

    // Validate and serialize manifest based on dataset kind
    let manifest_str = match dataset_kind {
        DatasetKind::Derived => {
            parse_validate_and_canonicalize_derived_dataset_manifest(&payload.manifest)
                .map_err(Error::from)?
        }
        DatasetKind::EvmRpc => {
            parse_and_canonicalize_raw_dataset_manifest::<EvmRpcManifest>(&payload.manifest)
                .map_err(Error::from)?
        }
        DatasetKind::Firehose => {
            parse_and_canonicalize_raw_dataset_manifest::<FirehoseManifest>(&payload.manifest)
                .map_err(Error::from)?
        }
        DatasetKind::EthBeacon => {
            parse_and_canonicalize_raw_dataset_manifest::<EthBeaconManifest>(&payload.manifest)
                .map_err(Error::from)?
        }
    };

    // Compute manifest hash from canonical serialization
    let manifest_hash = hash(&manifest_str);

    // Register the manifest
    ctx.dataset_store
        .register_manifest(
            &payload.namespace,
            &payload.name,
            &manifest_hash,
            manifest_str,
        )
        .await
        .map_err(|err| {
            tracing::error!(
                namespace = %payload.namespace,
                name = %payload.name,
                manifest_hash = %manifest_hash,
                kind = %dataset_kind,
                error = ?err,
                "Failed to register manifest"
            );
            Error::ManifestRegistrationError(err)
        })?;

    // Tag the manifest with the version
    ctx.dataset_store
        .set_dataset_version_tag(
            &payload.namespace,
            &payload.name,
            &payload.version,
            &manifest_hash,
        )
        .await
        .map_err(|err| {
            tracing::error!(
                namespace = %payload.namespace,
                name = %payload.name,
                version = %payload.version,
                manifest_hash = %manifest_hash,
                kind = %dataset_kind,
                error = ?err,
                "Failed to set version tag"
            );
            Error::VersionTaggingError(err)
        })?;

    tracing::info!(
        "Registered manifest for dataset '{}/{}' version '{}' (hash: {})",
        payload.namespace,
        payload.name,
        payload.version,
        manifest_hash
    );

    Ok(StatusCode::CREATED)
}

/// Request payload for dataset registration
///
/// Contains the dataset namespace, name, version, and manifest.
/// The manifest will be registered in the local registry.
#[derive(serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct RegisterRequest {
    /// Namespace for the dataset (validated identifier format)
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub namespace: Namespace,
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

    /// Dependency validation error
    ///
    /// This occurs when:
    /// - SQL queries are invalid
    /// - SQL queries reference datasets not declared in dependencies
    #[error("Manifest dependency error: {0}")]
    DependencyValidationError(#[from] DependencyValidationError),

    /// Failed to register manifest in the system
    ///
    /// This occurs when:
    /// - Error during manifest processing or storage
    /// - Registry information extraction failed
    /// - System-level registration errors
    #[error("Failed to register manifest: {0}")]
    ManifestRegistrationError(#[from] RegisterManifestError),

    /// Failed to tag version for the dataset
    ///
    /// This occurs when:
    /// - Error during version tagging in metadata database
    /// - Invalid semantic version format
    /// - Error updating latest tag
    #[error("Failed to set version tag: {0}")]
    VersionTaggingError(#[from] SetVersionTagError),

    /// Unsupported dataset kind
    ///
    /// This occurs when:
    /// - Dataset kind is not one of the supported types (manifest, evm-rpc, firehose, eth-beacon)
    #[error(
        "unsupported kind '{0}' - supported kinds: 'manifest' (derived), 'evm-rpc', 'firehose', 'eth-beacon'"
    )]
    UnsupportedDatasetKind(String),

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
            Error::DependencyValidationError(_) => "DEPENDENCY_VALIDATION_ERROR",
            Error::ManifestRegistrationError(_) => "MANIFEST_REGISTRATION_ERROR",
            Error::VersionTaggingError(_) => "VERSION_TAGGING_ERROR",
            Error::StoreError(_) => "STORE_ERROR",
            Error::UnsupportedDatasetKind(_) => "UNSUPPORTED_DATASET_KIND",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidPayloadFormat => StatusCode::BAD_REQUEST,
            Error::InvalidManifest(_) => StatusCode::BAD_REQUEST,
            Error::DependencyValidationError(_) => StatusCode::BAD_REQUEST,
            Error::ManifestRegistrationError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::VersionTaggingError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::StoreError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::UnsupportedDatasetKind(_) => StatusCode::BAD_REQUEST,
        }
    }
}

impl From<ParseDerivedManifestError> for Error {
    fn from(err: ParseDerivedManifestError) -> Self {
        match err {
            ParseDerivedManifestError::Deserialization(e) => Error::InvalidManifest(e),
            ParseDerivedManifestError::DependencyValidation(e) => {
                Error::DependencyValidationError(e)
            }
            ParseDerivedManifestError::Serialization(e) => Error::InvalidManifest(e),
        }
    }
}

impl From<ParseRawManifestError> for Error {
    fn from(err: ParseRawManifestError) -> Self {
        match err {
            ParseRawManifestError::Deserialization(e) => Error::InvalidManifest(e),
            ParseRawManifestError::Serialization(e) => Error::InvalidManifest(e),
        }
    }
}

/// Parse, validate, and re-serialize a derived dataset manifest to canonical JSON format
///
/// This function handles derived datasets which require dependency validation:
/// 1. Deserialize from JSON string
/// 2. Validate dependencies using `manifest.validate_dependencies()`
/// 3. Re-serialize to canonical JSON
///
/// Returns canonical JSON string on success
fn parse_validate_and_canonicalize_derived_dataset_manifest(
    manifest_str: impl AsRef<str>,
) -> Result<String, ParseDerivedManifestError> {
    let manifest: DerivedDatasetManifest = serde_json::from_str(manifest_str.as_ref())
        .map_err(ParseDerivedManifestError::Deserialization)?;

    manifest
        .validate_dependencies()
        .map_err(ParseDerivedManifestError::DependencyValidation)?;

    serde_json::to_string(&manifest).map_err(ParseDerivedManifestError::Serialization)
}

/// Error type for derived dataset manifest parsing and validation
///
/// Represents the different failure points when processing a derived dataset manifest
/// through the parse → validate → canonicalize pipeline.
#[derive(Debug, thiserror::Error)]
enum ParseDerivedManifestError {
    /// Failed to deserialize the JSON string into a `DerivedDatasetManifest` struct
    ///
    /// This occurs when:
    /// - JSON syntax is invalid
    /// - JSON structure doesn't match the manifest schema
    /// - Required fields are missing or have wrong types
    #[error("failed to deserialize manifest: {0}")]
    Deserialization(#[source] serde_json::Error),

    /// Failed dependency validation after successful deserialization
    ///
    /// This occurs when:
    /// - SQL queries reference datasets not declared in the dependencies list
    /// - Circular dependencies are detected
    /// - Other domain-specific validation rules are violated
    #[error("dependency validation failed: {0}")]
    DependencyValidation(#[source] DependencyValidationError),

    /// Failed to serialize the validated manifest back to canonical JSON
    ///
    /// This occurs when:
    /// - The manifest structure cannot be serialized (rare, indicates a bug)
    /// - Memory allocation fails during serialization
    ///
    /// Note: This should rarely happen since we already deserialized successfully
    #[error("failed to serialize manifest: {0}")]
    Serialization(#[source] serde_json::Error),
}

/// Parse and re-serialize a raw dataset manifest to canonical JSON format
///
/// This function handles the common pattern for raw datasets (EvmRpc, Firehose, EthBeacon):
/// 1. Deserialize from JSON string
/// 2. Re-serialize to canonical JSON
///
/// Returns canonical JSON string on success
fn parse_and_canonicalize_raw_dataset_manifest<T>(
    manifest_str: impl AsRef<str>,
) -> Result<String, ParseRawManifestError>
where
    T: serde::de::DeserializeOwned + serde::Serialize,
{
    let manifest: T = serde_json::from_str(manifest_str.as_ref())
        .map_err(ParseRawManifestError::Deserialization)?;
    serde_json::to_string(&manifest).map_err(ParseRawManifestError::Serialization)
}

/// Error type for raw dataset manifest parsing and canonicalization
///
/// Represents the different failure points when processing raw dataset manifests
/// (EvmRpc, Firehose, EthBeacon) through the parse → canonicalize pipeline.
#[derive(Debug, thiserror::Error)]
enum ParseRawManifestError {
    /// Failed to deserialize the JSON string into the manifest struct
    ///
    /// This occurs when:
    /// - JSON syntax is invalid
    /// - JSON structure doesn't match the manifest schema
    /// - Required fields are missing or have wrong types
    #[error("failed to deserialize manifest: {0}")]
    Deserialization(#[source] serde_json::Error),

    /// Failed to serialize the manifest back to canonical JSON
    ///
    /// This occurs when:
    /// - The manifest structure cannot be serialized (rare, indicates a bug)
    /// - Memory allocation fails during serialization
    ///
    /// Note: This should rarely happen since we already deserialized successfully
    #[error("failed to serialize manifest: {0}")]
    Serialization(#[source] serde_json::Error),
}
