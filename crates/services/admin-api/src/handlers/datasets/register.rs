use amp_datasets_raw::manifest::{EvmRpcManifest, FirehoseManifest, SolanaManifest};
use amp_datasets_registry::error::{LinkManifestError, RegisterManifestError, SetVersionTagError};
use axum::{
    Json,
    extract::{State, rejection::JsonRejection},
    http::StatusCode,
};
use common::datasets_cache::GetDatasetError;
use datasets_common::{
    dataset_kind_str::DatasetKindStr,
    hash::{Hash, hash},
    hash_reference::HashReference,
    name::Name,
    namespace::Namespace,
    table_name::TableName,
    version::Version,
};
use monitoring::logging;
use serde_json::value::RawValue;

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

/// Handler for the `POST /datasets` endpoint
///
/// Registers a new dataset configuration in the server's local registry. Accepts a JSON payload
/// containing the dataset registration configuration.
///
/// **Note**: This endpoint only registers datasets and does NOT schedule data extraction.
/// To extract data after registration, make a separate call to:
/// - `POST /datasets/{namespace}/{name}/versions/dev/deploy` - for dev tag
/// - `POST /datasets/{namespace}/{name}/versions/latest/deploy` - for latest tag
/// - `POST /datasets/{namespace}/{name}/versions/{version}/deploy` - for specific version
///
/// ## Request Body
/// - `dataset_name`: Name of the dataset to be registered (must be valid dataset name)
/// - `version`: Optional version of the dataset to register. If omitted, only the "dev" tag is updated.
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
/// - `MANIFEST_VALIDATION_ERROR`: Manifest validation failed (SQL queries invalid, undeclared dependencies, etc.)
/// - `MANIFEST_REGISTRATION_ERROR`: Failed to register manifest in system
/// - `MANIFEST_LINKING_ERROR`: Failed to link manifest to dataset
/// - `MANIFEST_NOT_FOUND`: Manifest hash provided but manifest doesn't exist
/// - `VERSION_TAGGING_ERROR`: Failed to tag the manifest with the version
///
/// ## Behavior
/// This handler supports multiple dataset kinds for registration:
/// - **Derived dataset** (kind="manifest"): Registers a derived dataset manifest that transforms data from other datasets using SQL queries
/// - **EVM-RPC dataset** (kind="evm-rpc"): Registers a raw dataset that extracts blockchain data directly from Ethereum-compatible JSON-RPC endpoints
/// - **Firehose dataset** (kind="firehose"): Registers a raw dataset that streams blockchain data from StreamingFast Firehose protocol
/// - **Legacy SQL datasets** are **not supported** and will return an error
///
/// ## Registration Process
/// The registration process involves two or three steps depending on whether a version is provided:
/// 1. **Register or validate manifest**: Either stores a new manifest in hash-based storage and creates
///    a metadata database entry, or validates that a provided manifest hash exists in the system
/// 2. **Link manifest to dataset**: Links the manifest to the dataset namespace/name and automatically
///    updates the "dev" tag to point to this manifest (performed in a transaction for atomicity)
/// 3. **Tag version** (optional): If a version is provided, associates the version identifier with the
///    manifest hash, and updates the "latest" tag if this version is higher than the current latest
///
/// This approach enables:
/// - Content-addressable storage by manifest hash
/// - Deduplication of identical manifests
/// - Separation of manifest storage, dataset linking, and version management
/// - Development workflow: register without version to only update "dev" tag via linking
/// - Release workflow: register with version to create semantic version tags and update "latest"
/// - Reuse workflow: provide manifest hash to link existing manifest without re-registering it
///
/// All operations are idempotent:
/// - **Manifest registration**: If the manifest already exists (same hash), the operation succeeds without changes
/// - **Manifest linking**: If the manifest is already linked to the dataset, the operation succeeds without changes
/// - **Dev tag update**: The dev tag is always updated to point to the linked manifest (last-write-wins)
/// - **Version tag**: If the version tag doesn't exist, it is created; if it exists with the same hash, no changes;
///   if it exists with a different hash, it is updated to point to the new hash
/// - **Latest tag**: Automatically updated only if the new version is higher than the current latest version
///
/// The handler:
/// - Validates dataset name and version format
/// - Checks that dataset kind is supported
/// - Registers/validates the manifest, links it to the dataset, and optionally tags it with a version
/// - Returns appropriate status codes and error messages
///
/// ## Typical Workflow
/// For users wanting both registration and data extraction:
/// 1. `POST /datasets` - Register the dataset (this endpoint)
/// 2. `POST /datasets/{namespace}/{name}/versions/{version}/deploy` - Schedule data extraction
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
            (status = 201, description = "Dataset successfully registered or updated", body = RegisterResponse),
            (status = 400, description = "Invalid request format or manifest", body = crate::handlers::error::ErrorResponse),
            (status = 500, description = "Internal server error", body = crate::handlers::error::ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    payload: Result<Json<RegisterRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<RegisterResponse>), ErrorResponse> {
    let RegisterRequest {
        namespace,
        name,
        version,
        manifest,
    } = match payload {
        Ok(Json(payload)) => payload,
        Err(err) => {
            tracing::debug!(error = %err, error_source = logging::error_source(&err), "invalid request body");
            return Err(Error::InvalidPayloadFormat(err).into());
        }
    };

    // Step 1: Register manifest or use provided hash
    let manifest_hash = match manifest {
        // Hash variant: use existing manifest hash
        HashOrManifestJson::Hash(hash) => {
            tracing::debug!(
                namespace = %namespace,
                name = %name,
                version = ?version,
                manifest_hash = %hash,
                "Received manifest hash, will link to dataset"
            );

            hash
        }

        // Content variant: validate and register new manifest
        HashOrManifestJson::ManifestJson(manifest_content) => {
            tracing::debug!(
                namespace = %namespace,
                name = %name,
                version = ?version,
                "Received manifest content, validating and storing"
            );

            let header =
                serde_json::from_str::<ManifestHeader>(manifest_content.get()).map_err(|err| {
                    tracing::error!(
                        namespace = %namespace,
                        name = %name,
                        version = ?version,
                        error = %err, error_source = logging::error_source(&err),
                        "Failed to parse manifest JSON"
                    );
                    Error::InvalidManifest(err)
                })?;

            let kind_str = DatasetKindStr::from(header.kind);

            // Validate and serialize manifest based on dataset kind
            let manifest_canonical =
                match header.kind {
                    DatasetKind::Derived => parse_and_canonicalize_derived_dataset_manifest(
                        &ctx.datasets_cache,
                        &ctx.ethcall_udfs_cache,
                        manifest_content.get(),
                    )
                    .await
                    .map_err(Error::from)?,
                    DatasetKind::EvmRpc => parse_and_canonicalize_raw_dataset_manifest::<
                        EvmRpcManifest,
                    >(manifest_content.get())
                    .map_err(Error::from)?,
                    DatasetKind::Firehose => parse_and_canonicalize_raw_dataset_manifest::<
                        FirehoseManifest,
                    >(manifest_content.get())
                    .map_err(Error::from)?,
                    DatasetKind::Solana => parse_and_canonicalize_raw_dataset_manifest::<
                        SolanaManifest,
                    >(manifest_content.get())
                    .map_err(Error::from)?,
                };

            // Compute manifest hash from canonical serialization
            let manifest_hash = hash(&manifest_canonical);

            // Register manifest (store in object store + metadata DB)
            ctx.datasets_registry
                .register_manifest(&manifest_hash, &kind_str, manifest_canonical)
                .await
                .map_err(|err| {
                    tracing::error!(
                        namespace = %namespace,
                        name = %name,
                        manifest_hash = %manifest_hash,
                        kind = %kind_str,
                        error = %err, error_source = logging::error_source(&err),
                        "Failed to register manifest"
                    );
                    Error::ManifestRegistrationError(err)
                })?;

            tracing::debug!(
                namespace = %namespace,
                name = %name,
                manifest_hash = %manifest_hash,
                kind = %kind_str,
                "Manifest registered, will link to dataset"
            );

            manifest_hash
        }
    };

    // Step 2: Link manifest to dataset
    ctx.datasets_registry
        .link_manifest(&namespace, &name, &manifest_hash)
        .await
        .map_err(|err| match err {
            LinkManifestError::ManifestNotFound(hash) => {
                tracing::error!(
                    namespace = %namespace,
                    name = %name,
                    manifest_hash = %hash,
                    "Manifest not found"
                );
                Error::ManifestNotFound(hash)
            }
            err => {
                tracing::error!(
                    namespace = %namespace,
                    name = %name,
                    manifest_hash = %manifest_hash,
                    error = %err, error_source = logging::error_source(&err),
                    "Failed to link manifest to dataset"
                );
                Error::ManifestLinkingError(err)
            }
        })?;

    tracing::info!(
        "Linked manifest to dataset '{}/{}' (hash: {})",
        namespace,
        name,
        manifest_hash
    );

    // Step 3: Tag the manifest with version, if provided
    if let Some(ref version) = version {
        ctx.datasets_registry
            .set_dataset_version_tag(&namespace, &name, version, &manifest_hash)
            .await
            .map_err(|err| {
                tracing::error!(
                    namespace = %namespace,
                    name = %name,
                    version = %version,
                    manifest_hash = %manifest_hash,
                    error = %err, error_source = logging::error_source(&err),
                    "Failed to set version tag"
                );
                Error::VersionTaggingError(err)
            })?;

        tracing::info!(
            "Tagged version '{}' for dataset '{}/{}' (hash: {})",
            version,
            namespace,
            name,
            manifest_hash
        );
    }

    // Fetch dataset info from cache to populate response
    let hash_ref = HashReference::new(namespace.clone(), name.clone(), manifest_hash.clone());
    let dataset = ctx
        .datasets_cache
        .get_dataset(&hash_ref)
        .await
        .map_err(Error::GetDataset)?;

    let tables = dataset.table_names();
    let finalized_blocks_only = dataset.finalized_blocks_only();
    let kind = dataset.kind();
    let start_block = dataset.start_block().unwrap_or(0);

    Ok((
        StatusCode::CREATED,
        Json(RegisterResponse {
            namespace,
            name,
            version,
            manifest_hash,
            kind,
            start_block,
            finalized_blocks_only,
            tables,
        }),
    ))
}

/// Response payload for dataset registration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct RegisterResponse {
    /// Dataset namespace
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub namespace: Namespace,
    /// Dataset name
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub name: Name,
    /// Version of the dataset, if provided during registration
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "utoipa", schema(value_type = Option<String>))]
    pub version: Option<Version>,
    /// Manifest hash
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub manifest_hash: Hash,
    /// Dataset kind
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub kind: DatasetKindStr,
    /// Starting block
    pub start_block: u64,
    /// Finalized blocks only
    pub finalized_blocks_only: bool,
    /// Tables
    #[cfg_attr(feature = "utoipa", schema(value_type = Vec<String>))]
    pub tables: Vec<TableName>,
}

/// Request payload for dataset registration
///
/// Contains the dataset namespace, name, version, and manifest.
/// The manifest will be registered (or validated if hash provided), linked to the dataset,
/// and optionally tagged with a semantic version.
#[derive(serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct RegisterRequest {
    /// Namespace for the dataset (validated identifier format)
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub namespace: Namespace,
    /// Name of the dataset to be registered (validated identifier format)
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub name: Name,
    /// Optional version of the dataset to register using semantic versioning (e.g., "1.0.0").
    ///
    /// If omitted, only the manifest linking and "dev" tag update are performed.
    /// If provided, the manifest is also tagged with this semantic version, and "latest" tag is
    /// updated if this version is higher than the current latest.
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<Version>,
    /// Manifest input: either a manifest hash (64-char hex string) to link to an existing manifest,
    /// or a full manifest JSON object to register a new manifest
    #[cfg_attr(feature = "utoipa", schema(schema_with = hash_or_manifest_utoipa_schema))]
    pub manifest: HashOrManifestJson,
}

#[cfg(feature = "utoipa")]
fn hash_or_manifest_utoipa_schema() -> utoipa::openapi::schema::Schema {
    use utoipa::openapi::schema::{ObjectBuilder, OneOfBuilder, SchemaType, Type};

    utoipa::openapi::schema::Schema::OneOf(
        OneOfBuilder::new()
            .item(
                ObjectBuilder::new()
                    .schema_type(SchemaType::Type(Type::String))
                    .description(Some(
                        "A manifest hash (64-character SHA-256 hex string)".to_string(),
                    ))
                    .min_length(Some(64))
                    .max_length(Some(64))
                    .pattern(Some("[0-9a-fA-F]{64}"))
                    .build(),
            )
            .item(
                ObjectBuilder::new()
                    .schema_type(SchemaType::Type(Type::Object))
                    .description(Some("Full manifest JSON content".to_string()))
                    .build(),
            )
            .description(Some(
                "Either a manifest hash (64-char hex string) or full manifest JSON content"
                    .to_string(),
            ))
            .build(),
    )
}

/// Input type for manifest field in dataset registration requests
///
/// This enum allows callers to provide either:
/// - A manifest hash (64-character SHA-256 hex string) to link to an existing manifest
/// - A full manifest JSON content to register a new manifest
///
/// ## Deserialization Behavior
/// The deserializer attempts to parse the input in the following order:
/// 1. **Hash**: If the input is a string of exactly 64 hexadecimal characters, it's treated as a hash
/// 2. **ManifestJson**: Otherwise, treat the input as raw JSON manifest content
#[derive(Debug, Clone)]
pub enum HashOrManifestJson {
    /// A reference to an existing manifest by its SHA-256 hash
    ///
    /// The hash must be exactly 64 hexadecimal characters.
    /// When this variant is used, the manifest must already exist in the system.
    Hash(Hash),

    /// Full manifest content as unparsed JSON
    ///
    /// This preserves the JSON structure without parsing until needed.
    /// The manifest will be validated, canonicalized, and stored during registration.
    ManifestJson(Box<RawValue>),
}

impl<'de> serde::Deserialize<'de> for HashOrManifestJson {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = Box::<RawValue>::deserialize(deserializer)?;

        // Try to deserialize the RawValue as Hash - if it works, return early
        if let Ok(hash) = serde_json::from_str::<Hash>(raw.get()) {
            return Ok(HashOrManifestJson::Hash(hash));
        }

        // Otherwise, use the RawValue as manifest content
        Ok(HashOrManifestJson::ManifestJson(raw))
    }
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
    #[error("invalid request body")]
    InvalidPayloadFormat(#[source] JsonRejection),

    /// Invalid derived dataset manifest content or structure
    ///
    /// This occurs when:
    /// - Manifest JSON is malformed or invalid
    /// - Manifest structure doesn't match expected schema
    /// - Required manifest fields are missing or invalid
    #[error("invalid manifest")]
    InvalidManifest(#[source] serde_json::Error),

    /// Manifest validation error
    #[error("Manifest validation error")]
    ManifestValidationError(#[source] ManifestValidationError),

    /// Failed to register manifest in the system
    ///
    /// This occurs when:
    /// - Error during manifest processing or storage
    /// - Registry information extraction failed
    /// - System-level registration errors
    #[error("Failed to register manifest")]
    ManifestRegistrationError(#[source] RegisterManifestError),

    /// Failed to link manifest to dataset
    ///
    /// This occurs when:
    /// - Error during manifest linking in metadata database
    /// - Error updating dev tag
    #[error("Failed to link manifest to dataset")]
    ManifestLinkingError(#[source] LinkManifestError),

    /// Failed to tag version for the dataset
    ///
    /// This occurs when:
    /// - Error during version tagging in metadata database
    /// - Invalid semantic version format
    /// - Error updating latest tag
    #[error("Failed to set version tag")]
    VersionTaggingError(#[source] SetVersionTagError),

    /// Manifest not found
    ///
    /// This occurs when:
    /// - A manifest hash was provided but the manifest doesn't exist in the system
    /// - The hash is valid format but no manifest is stored with that hash
    #[error("manifest with hash '{0}' not found")]
    ManifestNotFound(Hash),

    /// Failed to get dataset info after registration
    #[error("Failed to get dataset info")]
    GetDataset(#[source] GetDatasetError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidPayloadFormat(_) => "INVALID_PAYLOAD_FORMAT",
            Error::InvalidManifest(_) => "INVALID_MANIFEST",
            Error::ManifestValidationError(_) => "MANIFEST_VALIDATION_ERROR",
            Error::ManifestRegistrationError(_) => "MANIFEST_REGISTRATION_ERROR",
            Error::ManifestLinkingError(_) => "MANIFEST_LINKING_ERROR",
            Error::VersionTaggingError(_) => "VERSION_TAGGING_ERROR",
            Error::ManifestNotFound(_) => "MANIFEST_NOT_FOUND",
            Error::GetDataset(_) => "GET_DATASET_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidPayloadFormat(_) => StatusCode::BAD_REQUEST,
            Error::InvalidManifest(_) => StatusCode::BAD_REQUEST,
            Error::ManifestValidationError(_) => StatusCode::BAD_REQUEST,
            Error::ManifestRegistrationError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::ManifestLinkingError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::VersionTaggingError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::ManifestNotFound(_) => StatusCode::NOT_FOUND,
            Error::GetDataset(_) => StatusCode::INTERNAL_SERVER_ERROR,
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
