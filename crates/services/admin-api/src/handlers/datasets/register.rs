use axum::{
    Json,
    extract::{State, rejection::JsonRejection},
    http::StatusCode,
};
use common::manifest::derived::Manifest;
use dataset_store::DatasetStore;
use datasets_common::{manifest::Manifest as CommonManifest, name::Name, version::Version};
use http_common::{BoxRequestError, RequestError};
use metadata_db::MetadataDb;
use object_store::path::Path;
use tracing::instrument;

use crate::{ctx::Ctx, handlers::common::NonEmptyString};

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
/// - **200 OK**: Dataset successfully registered
/// - **400 Bad Request**: Invalid dataset name, version, or manifest format
/// - **409 Conflict**: Dataset already exists with provided manifest, or manifest required but not provided
/// - **500 Internal Server Error**: Database or object store error
///
/// ## Error Codes
/// - `INVALID_DATASET_NAME`: Dataset name contains invalid characters or format
/// - `INVALID_DATASET_VERSION`: Version string is not a valid semantic version
/// - `INVALID_MANIFEST`: Manifest JSON parsing or structure error
/// - `MANIFEST_VALIDATION_ERROR`: Manifest name/version doesn't match request parameters
/// - `MANIFEST_REGISTRATION_ERROR`: Failed to register manifest in system
/// - `DATASET_ALREADY_EXISTS`: Dataset with same name and version already exists
/// - `DATASET_DEF_STORE_ERROR`: Failed to store dataset definition
/// - `STORE_ERROR`: Failed to load or access dataset store
///
/// ## Behavior
/// This handler supports two main registration scenarios:
/// 1. **Derived dataset**: Registers a derived dataset manifest (kind="manifest") in both object store and metadata database
/// 2. **SQL dataset**: Stores dataset definition JSON in object store and loads the dataset to ensure registration
///
/// The handler:
/// - Validates dataset name and version format
/// - Attempts to load existing dataset from store
/// - Handles manifest registration in the server's local registry
/// - Returns appropriate status codes and error messages
///
/// ## Typical Workflow
/// For users wanting both registration and data extraction:
/// 1. `POST /datasets` - Register the dataset (this endpoint)
/// 2. `POST /datasets/{name}/dump` or `POST /datasets/{name}/versions/{version}/dump` - Schedule data extraction
#[tracing::instrument(skip_all, err)]
pub async fn handler(
    State(ctx): State<Ctx>,
    payload: Result<Json<RegisterRequest>, JsonRejection>,
) -> Result<StatusCode, BoxRequestError> {
    let payload = match payload {
        Ok(Json(payload)) => payload,
        Err(err) => {
            tracing::error!("Failed to parse request JSON: {}", err);
            return Err(Error::InvalidPayloadFormat.into());
        }
    };

    let dataset = ctx
        .store
        .try_load_dataset(&payload.name, Some(&payload.version))
        .await
        .map_err(Error::StoreError)?;

    // Check if dataset already exists with this name and version
    if dataset.is_some() {
        return Err(Error::DatasetAlreadyExists(
            payload.name.to_string(),
            payload.version.to_string(),
        )
        .into());
    }

    let common = serde_json::from_str::<CommonManifest>(payload.manifest.as_str())
        .map_err(|err| Error::InvalidManifest(err.to_string()))?;

    match common.kind.as_str() {
        "manifest" => {
            let manifest: Manifest = serde_json::from_str(payload.manifest.as_str())
                .map_err(|err| Error::InvalidManifest(err.to_string()))?;
            if manifest.name != payload.name || manifest.version != payload.version {
                return Err(Error::ManifestValidationError(
                    manifest.name.to_string(),
                    manifest.version.to_string(),
                )
                .into());
            }
            register_manifest(&ctx.store, &ctx.metadata_db, &manifest)
                .await
                .map_err(Error::ManifestRegistrationError)?;
            tracing::info!(
                "Registered manifest for dataset '{}' version '{}'",
                payload.name,
                payload.version
            );
        }
        // SQL datasets
        _ => {
            // Validate manifest JSON body
            let _: serde_json::Value = serde_json::from_str(payload.manifest.as_str())
                .map_err(|err| Error::InvalidManifest(err.to_string()))?;

            // Store dataset definition in the dataset definitions store
            ctx.config
                .dataset_defs_store
                .prefixed_store()
                .put(
                    &Path::from(format!("{}.json", payload.name)),
                    payload.manifest.into_inner().into(),
                )
                .await
                .map_err(Error::DatasetDefStoreError)?;

            // Attempt to load the dataset to ensure it's registered
            let _ = ctx
                .store
                .load_dataset(&payload.name, None)
                .await
                .map_err(Error::StoreError)?;
        }
    }

    Ok(StatusCode::CREATED)
}

/// Request payload for dataset registration
///
/// Contains the dataset name, version, and manifest.
/// The manifest will be registered in the local registry.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct RegisterRequest {
    /// Name of the dataset to be registered (automatically validated)
    pub name: Name,
    /// Version of the dataset to register
    pub version: Version,
    /// JSON string representation of the dataset manifest (required)
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

    /// Invalid manifest content or structure
    ///
    /// This occurs when:
    /// - Manifest JSON is malformed or invalid
    /// - Manifest structure doesn't match expected schema
    /// - Required manifest fields are missing or invalid
    #[error("invalid manifest: {0}")]
    InvalidManifest(String),

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
    ManifestRegistrationError(#[from] RegisterError),

    /// Dataset already exists with the given configuration
    ///
    /// This occurs when:
    /// - Dataset with same name/version already exists
    /// - Attempt to register existing dataset with new manifest
    /// - Conflicting registration attempts
    #[error("Dataset '{0}' version '{1}' already exists")]
    DatasetAlreadyExists(String, String),

    /// Dataset definition store error
    ///
    /// This occurs when:
    /// - Failed to write dataset definition to object store
    /// - Object store connectivity or permissions issues
    /// - Storage backend errors
    #[error("dataset definition store error: {0}")]
    DatasetDefStoreError(#[from] object_store::Error),

    /// Dataset store error
    ///
    /// This occurs when:
    /// - Failed to load dataset from store
    /// - Dataset store configuration errors
    /// - Dataset store connectivity issues
    #[error("dataset store error: {0}")]
    StoreError(#[from] dataset_store::DatasetError),

    /// Metadata database error
    ///
    /// This occurs when:
    /// - Database connection issues
    /// - SQL query execution errors
    /// - Database schema inconsistencies
    #[error("metadata db error: {0}")]
    MetadataDbError(#[from] metadata_db::Error),
}

impl RequestError for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidPayloadFormat => "INVALID_PAYLOAD_FORMAT",
            Error::InvalidManifest(_) => "INVALID_MANIFEST",
            Error::ManifestValidationError(_, _) => "MANIFEST_VALIDATION_ERROR",
            Error::ManifestRegistrationError(_) => "MANIFEST_REGISTRATION_ERROR",
            Error::DatasetAlreadyExists(_, _) => "DATASET_ALREADY_EXISTS",
            Error::DatasetDefStoreError(_) => "DATASET_DEF_STORE_ERROR",
            Error::StoreError(_) => "STORE_ERROR",
            Error::MetadataDbError(_) => "METADATA_DB_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidPayloadFormat => StatusCode::BAD_REQUEST,
            Error::InvalidManifest(_) => StatusCode::BAD_REQUEST,
            Error::ManifestValidationError(_, _) => StatusCode::BAD_REQUEST,
            Error::ManifestRegistrationError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::DatasetAlreadyExists(_, _) => StatusCode::CONFLICT,
            Error::DatasetDefStoreError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::StoreError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::MetadataDbError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

/// Register a manifest in the dataset store and metadata database
///
/// This function validates and registers a dataset manifest by:
/// 1. Checking if the dataset already exists with the given name and version
/// 2. Extracting registry information from the manifest
/// 3. Storing the manifest JSON in the dataset definitions store
/// 4. Registering the dataset metadata in the database
#[instrument(skip_all, err)]
async fn register_manifest(
    dataset_store: &DatasetStore,
    metadata_db: &MetadataDb,
    manifest: &Manifest,
) -> Result<(), RegisterError> {
    // Check if the dataset with the given name and version already exists in the registry.
    if metadata_db
        .dataset_exists(&manifest.name, &manifest.version)
        .await
        .map_err(RegisterError::ExistenceCheck)?
    {
        return Err(RegisterError::DatasetExists {
            name: manifest.name.clone(),
            version: manifest.version.clone(),
        });
    }

    // Extract dataset owner from dependencies
    let dataset_owner = manifest
        .dependencies
        .first_key_value()
        .unwrap()
        .1
        .owner
        .clone();

    // Prepare manifest data for storage
    let dataset_name_str = manifest.name.to_string();
    let manifest_filename = manifest.to_filename();
    let manifest_path_str = object_store::path::Path::from(manifest_filename).to_string();
    let manifest_json = serde_json::to_string(&manifest)
        .map_err(|err| RegisterError::ManifestSerialization(err.to_string()))?;

    // Store manifest in dataset definitions store
    let dataset_defs_store = dataset_store.dataset_defs_store();
    let manifest_path = object_store::path::Path::from(manifest_path_str.clone());

    dataset_defs_store
        .prefixed_store()
        .put(&manifest_path, manifest_json.into())
        .await
        .map_err(RegisterError::ManifestStorage)?;

    // Register dataset metadata in database
    metadata_db
        .register_dataset(
            &dataset_owner,
            &dataset_name_str,
            &manifest.version,
            &manifest_path_str,
        )
        .await
        .map_err(RegisterError::MetadataRegistration)?;

    Ok(())
}

/// Errors specific to manifest registration operations
#[derive(Debug, thiserror::Error)]
pub enum RegisterError {
    /// Dataset already exists in the registry
    #[error("Dataset '{name}' version '{version}' already registered")]
    DatasetExists { name: Name, version: Version },

    /// Failed to serialize manifest to JSON
    #[error("Failed to serialize manifest to JSON: {0}")]
    ManifestSerialization(String),

    /// Failed to store manifest in dataset definitions store
    #[error("Failed to store manifest in dataset definitions store: {0}")]
    ManifestStorage(object_store::Error),

    /// Failed to register dataset in metadata database
    #[error("Failed to register dataset in metadata database: {0}")]
    MetadataRegistration(metadata_db::Error),

    /// Failed to check if dataset exists in metadata database
    #[error("Failed to check dataset existence in metadata database: {0}")]
    ExistenceCheck(metadata_db::Error),
}
