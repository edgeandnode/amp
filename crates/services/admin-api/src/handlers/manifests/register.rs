//! Manifests register handler

use amp_datasets_raw::manifest::{EvmRpcManifest, FirehoseManifest, SolanaManifest};
use amp_datasets_registry::{error::RegisterManifestError, manifests::StoreError};
use axum::{
    Json,
    extract::{State, rejection::JsonRejection},
    http::StatusCode,
};
use datasets_common::{
    dataset_kind_str::DatasetKindStr,
    hash::{Hash, hash},
};
use monitoring::logging;

use crate::{
    ctx::Ctx,
    handlers::{
        common::{
            DatasetKind, ManifestHeader, ManifestValidationError, ParseDerivedManifestError,
            ParseRawManifestError, parse_and_canonicalize_derived_dataset_manifest,
            parse_and_canonicalize_raw_dataset_manifest,
        },
        error::{ErrorResponse, IntoErrorResponse},
    },
};

/// Handler for the `POST /manifests` endpoint
///
/// Registers a new manifest in content-addressable storage without linking to any dataset or creating version tags.
/// This endpoint is useful for pre-registering manifests before associating them with specific datasets.
///
/// ## Request Body
/// The request body should contain a complete manifest JSON object. The manifest kind determines
/// the validation rules:
/// - `kind="manifest"` (Derived): Validates SQL dependencies
/// - `kind="evm-rpc"`, `kind="firehose"`, `kind="solana"` (Raw): Validates structure only
///
/// ## Response
/// - **201 Created**: Manifest successfully registered, returns the computed hash
/// - **400 Bad Request**: Invalid JSON format, unsupported kind, or validation failure
/// - **500 Internal Server Error**: Manifest store error
///
/// ## Error Codes
/// - `INVALID_PAYLOAD_FORMAT`: Request JSON is malformed or invalid
/// - `INVALID_MANIFEST`: Manifest JSON parsing or structure error
/// - `MANIFEST_VALIDATION_ERROR`: Manifest validation failed (derived datasets only)
/// - `MANIFEST_STORAGE_ERROR`: Failed to store manifest in object store
/// - `MANIFEST_REGISTRATION_ERROR`: Failed to register manifest in metadata database
///
/// ## Registration Process
/// Unlike `POST /datasets`, this endpoint performs minimal registration:
/// 1. **Parse and validate**: Validates manifest structure and dependencies (for derived datasets)
/// 2. **Canonicalize**: Re-serializes manifest to canonical JSON format
/// 3. **Compute hash**: Generates content hash from canonical JSON
/// 4. **Store manifest**: Writes to object store and registers in metadata database
///
/// This handler:
/// - Validates and extracts the manifest JSON from the request body
/// - Parses and validates based on dataset kind
/// - Stores the manifest in content-addressable storage
/// - Returns the computed manifest hash
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        post,
        path = "/manifests",
        tag = "manifests",
        operation_id = "manifests_register",
        request_body = serde_json::Value,
        responses(
            (status = 201, description = "Manifest successfully registered", body = RegisterManifestResponse),
            (status = 400, description = "Invalid request format or manifest", body = crate::handlers::error::ErrorResponse),
            (status = 500, description = "Internal server error", body = crate::handlers::error::ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    payload: Result<Json<serde_json::Value>, JsonRejection>,
) -> Result<(StatusCode, Json<RegisterManifestResponse>), ErrorResponse> {
    let payload = match payload {
        Ok(Json(payload)) => payload,
        Err(err) => {
            tracing::error!(error = %err, error_source = logging::error_source(&err), "failed to parse request JSON");
            return Err(Error::InvalidPayloadFormat.into());
        }
    };

    // Convert payload to JSON string for processing
    let manifest_str = serde_json::to_string(&payload).map_err(|err| {
        tracing::error!(error = %err, error_source = logging::error_source(&err), "failed to serialize payload to JSON string");
        Error::InvalidManifest(err)
    })?;

    // Parse ManifestHeader to extract validated kind
    let header = serde_json::from_str::<ManifestHeader>(&manifest_str).map_err(|err| {
        tracing::error!(error = %err, error_source = logging::error_source(&err), "failed to parse manifest");
        Error::InvalidManifest(err)
    })?;

    let kind_str = DatasetKindStr::from(header.kind);

    // Validate and serialize manifest based on dataset kind
    let canonical_manifest_str = match header.kind {
        DatasetKind::Derived => parse_and_canonicalize_derived_dataset_manifest(
            &ctx.datasets_cache,
            &ctx.ethcall_udfs_cache,
            &manifest_str,
        )
        .await
        .map_err(Error::from)?,
        DatasetKind::EvmRpc => {
            parse_and_canonicalize_raw_dataset_manifest::<EvmRpcManifest>(&manifest_str)
                .map_err(Error::from)?
        }
        DatasetKind::Solana => {
            parse_and_canonicalize_raw_dataset_manifest::<SolanaManifest>(&manifest_str)
                .map_err(Error::from)?
        }
        DatasetKind::Firehose => {
            parse_and_canonicalize_raw_dataset_manifest::<FirehoseManifest>(&manifest_str)
                .map_err(Error::from)?
        }
    };

    // Compute manifest hash from canonical serialization
    let hash = hash(&canonical_manifest_str);

    // Store manifest in object store and register in metadata database
    // This does NOT link to any dataset or create version tags
    if let Err(err) = ctx
        .datasets_registry
        .register_manifest(&hash, &kind_str, canonical_manifest_str)
        .await
    {
        tracing::error!(
            manifest_hash = %hash,
            kind = %kind_str,
            error = %err, error_source = logging::error_source(&err),
            "failed to register manifest"
        );
        return Err(match err {
            RegisterManifestError::ManifestStorage(e) => Error::ObjectStoreWriteError(e),
            RegisterManifestError::MetadataRegistration(e) => Error::MetadataDbError(e),
        }
        .into());
    }

    tracing::info!(
        manifest_hash = %hash,
        kind = %kind_str,
        "manifest registered"
    );

    Ok((StatusCode::CREATED, Json(RegisterManifestResponse { hash })))
}

/// Response payload for manifest registration
///
/// Contains the computed hash of the registered manifest.
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct RegisterManifestResponse {
    /// The computed content hash of the manifest (used as unique identifier)
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub hash: Hash,
}

/// Errors that can occur during manifest registration
///
/// This enum represents all possible error conditions when handling
/// a request to register a manifest without dataset association.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid request format
    ///
    /// This occurs when:
    /// - Request JSON is malformed or invalid
    /// - Request body cannot be parsed as valid JSON
    /// - Required fields are missing or have wrong types
    #[error("invalid request format")]
    InvalidPayloadFormat,

    /// Invalid manifest content or structure
    ///
    /// This occurs when:
    /// - Manifest JSON is malformed or invalid
    /// - Manifest structure doesn't match expected schema for the given kind
    /// - Required manifest fields (name, kind, version, etc.) are missing or invalid
    /// - JSON serialization/deserialization fails during canonicalization
    #[error("invalid manifest")]
    InvalidManifest(#[source] serde_json::Error),

    /// Manifest validation error for derived datasets
    #[error("manifest validation error")]
    ManifestValidationError(#[source] ManifestValidationError),

    /// Failed to write manifest to object store
    ///
    /// This occurs when:
    /// - Object store is not accessible or connection fails
    /// - Write permissions are insufficient
    /// - Storage quota is exceeded
    /// - Network errors prevent writing to remote storage
    #[error("failed to write manifest to object store")]
    ObjectStoreWriteError(#[source] StoreError),

    /// Failed to register manifest in metadata database
    ///
    /// This occurs when:
    /// - Database connection is lost
    /// - SQL insertion or update query fails
    /// - Database constraints are violated
    /// - Schema inconsistencies prevent registration
    #[error("failed to register manifest in metadata database")]
    MetadataDbError(#[source] metadata_db::Error),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidPayloadFormat => "INVALID_PAYLOAD_FORMAT",
            Error::InvalidManifest(_) => "INVALID_MANIFEST",
            Error::ManifestValidationError(_) => "MANIFEST_VALIDATION_ERROR",
            Error::ObjectStoreWriteError(_) => "MANIFEST_STORAGE_ERROR",
            Error::MetadataDbError(_) => "MANIFEST_REGISTRATION_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidPayloadFormat => StatusCode::BAD_REQUEST,
            Error::InvalidManifest(_) => StatusCode::BAD_REQUEST,
            Error::ManifestValidationError(_) => StatusCode::BAD_REQUEST,
            Error::ObjectStoreWriteError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::MetadataDbError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl From<ParseDerivedManifestError> for Error {
    fn from(err: ParseDerivedManifestError) -> Self {
        match err {
            ParseDerivedManifestError::Deserialization(e) => Error::InvalidManifest(e),
            ParseDerivedManifestError::ManifestValidation(e) => Error::ManifestValidationError(*e),
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
