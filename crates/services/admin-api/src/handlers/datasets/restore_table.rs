use amp_datasets_registry::error::ResolveRevisionError;
use axum::{
    body::Bytes,
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use datasets_common::{
    name::Name, namespace::Namespace, reference::Reference, revision::Revision,
    table_name::TableName,
};
use metadata_db::physical_table_revision::LocationId;
use monitoring::logging;

use crate::{
    ctx::Ctx,
    handlers::{
        common::{RegisterRevisionFilesError, register_revision_files},
        error::{ErrorResponse, IntoErrorResponse},
    },
};

/// Handler for the `POST /datasets/{namespace}/{name}/versions/{revision}/tables/{table_name}/restore` endpoint
///
/// Restores a single physical table, either by activating a known revision or
/// by discovering the latest revision from object storage.
///
/// ## Path Parameters
/// - `namespace`: Dataset namespace
/// - `name`: Dataset name
/// - `revision`: Revision (version, hash, latest, or dev)
/// - `table_name`: Table name
///
/// ## Request Body (optional JSON)
/// - `location_id` (optional, i64): If provided, activates this existing revision
///   for the table. If omitted, discovers the latest revision from object storage
///   via UUID heuristic.
///
/// ## Response
/// - **200 OK**: Table successfully restored
/// - **400 Bad Request**: Invalid path parameters or request body
/// - **404 Not Found**: Dataset, revision, or table not found
/// - **500 Internal Server Error**: Database or storage error
///
/// ## Error Codes
/// - `INVALID_PATH`: Invalid path parameters (namespace, name, revision, or table_name)
/// - `INVALID_BODY`: Invalid request body
/// - `DATASET_NOT_FOUND`: The specified dataset or revision does not exist
/// - `RESOLVE_REVISION_ERROR`: Failed to resolve revision to manifest hash
/// - `GET_DATASET_ERROR`: Failed to load dataset from store
/// - `TABLE_NOT_IN_MANIFEST`: Table not found in the dataset manifest
/// - `TABLE_NOT_FOUND`: Table data not found in object storage
/// - `GET_REVISION_ERROR`: Failed to look up revision by location ID
/// - `REVISION_NOT_FOUND`: No revision exists for the given location ID
/// - `REGISTER_AND_ACTIVATE_ERROR`: Failed to register and activate physical table
/// - `RESTORE_TABLE_REVISION_ERROR`: Failed to restore a table from storage
/// - `REGISTER_FILES_ERROR`: Failed to register files for a table
///
/// ## Behavior
///
/// **With `location_id`:**
/// 1. Resolves the revision and loads the dataset
/// 2. Validates the table exists in the dataset manifest
/// 3. Upserts the `physical_tables` entry
/// 4. Marks all existing revisions for the table as inactive
/// 5. Marks the specified `location_id` as active
///
/// **Without `location_id`:**
/// 1. Resolves the revision and loads the dataset
/// 2. Validates the table exists in the dataset manifest
/// 3. Scans object storage for the latest revision (by UUID ordering)
/// 4. Registers and activates the discovered revision
/// 5. Re-indexes all Parquet file metadata from storage
///
/// This is useful for:
/// - Recovering from metadata database loss
/// - Activating a specific known revision for a table
/// - Setting up a new system with pre-existing data
/// - Re-syncing metadata after storage restoration
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        post,
        path = "/datasets/{namespace}/{name}/versions/{revision}/tables/{table_name}/restore",
        tag = "datasets",
        operation_id = "restore_table",
        params(
            ("namespace" = String, Path, description = "Dataset namespace"),
            ("name" = String, Path, description = "Dataset name"),
            ("revision" = String, Path, description = "Revision (version, hash, latest, or dev)"),
            ("table_name" = String, Path, description = "Table name")
        ),
        request_body(content = Option<RestoreTablePayload>, content_type = "application/json"),
        responses(
            (status = 200, description = "Table successfully restored"),
            (status = 400, description = "Bad request (invalid parameters)", body = ErrorResponse),
            (status = 404, description = "Dataset, revision, or table not found", body = ErrorResponse),
            (status = 500, description = "Internal server error", body = ErrorResponse),
        )
    )
)]
pub async fn handler(
    path: Result<Path<(Namespace, Name, Revision, TableName)>, PathRejection>,
    State(ctx): State<Ctx>,
    body: Bytes,
) -> Result<StatusCode, ErrorResponse> {
    let (reference, table_name) = match path {
        Ok(Path((namespace, name, revision, table_name))) => {
            (Reference::new(namespace, name, revision), table_name)
        }
        Err(err) => {
            tracing::debug!(error = %err, error_source = logging::error_source(&err), "invalid path parameters");
            return Err(Error::InvalidPath(err).into());
        }
    };

    let location_id = if body.is_empty() {
        None
    } else {
        let payload: RestoreTablePayload = serde_json::from_slice(&body).map_err(|err| {
            tracing::debug!(error = %err, error_source = logging::error_source(&err), "invalid request body");
            Error::InvalidBody(err)
        })?;
        Some(payload.location_id)
    };

    tracing::debug!(
        dataset_reference = %reference,
        table_name = %table_name,
        ?location_id,
        "restoring table"
    );

    let namespace = reference.namespace().clone();
    let name = reference.name().clone();
    let revision = reference.revision().clone();

    // Resolve reference to hash reference
    let dataset_ref = ctx
        .datasets_registry
        .resolve_revision(&reference)
        .await
        .map_err(Error::ResolveRevision)?
        .ok_or_else(|| Error::NotFound {
            namespace: namespace.clone(),
            name: name.clone(),
            revision: revision.clone(),
        })?;

    // Load the full dataset object using the resolved hash reference
    let dataset = ctx
        .dataset_store
        .get_dataset(&dataset_ref)
        .await
        .map_err(Error::GetDataset)?;

    // Validate the table exists in the dataset manifest
    dataset
        .tables()
        .iter()
        .find(|t| *t.name() == table_name)
        .ok_or_else(|| Error::TableNotInManifest {
            table: table_name.clone(),
        })?;

    if let Some(location_id) = location_id {
        // Check if the revision with the given location id exists
        let revision = ctx
            .data_store
            .get_revision_by_location_id(location_id)
            .await
            .map_err(|err| Error::GetRevisionByLocationId {
                location_id,
                source: err,
            })?;
        if revision.is_none() {
            return Err(Error::RevisionNotFound { location_id }.into());
        }

        // With location_id: UPSERT physical_tables → mark inactive → mark active
        tracing::debug!(%dataset_ref, %table_name, %location_id, "activating existing revision");

        ctx.data_store
            .register_and_activate_physical_table(&dataset_ref, &table_name, location_id)
            .await
            .map_err(Error::RegisterAndActivatePhysicalTable)?;

        tracing::info!(%dataset_ref, %table_name, %location_id, "table revision activated");
    } else {
        // Without location_id: full restore via UUID heuristic
        tracing::debug!(%dataset_ref, %table_name, "restoring from object storage");

        let data_store = ctx.data_store.clone();

        let info = data_store
            .restore_latest_table_revision(&dataset_ref, &table_name)
            .await
            .map_err(|err| {
                tracing::error!(
                    table_name = %table_name,
                    error = %err,
                    error_source = logging::error_source(&err),
                    "failed to restore table revision from storage"
                );
                Error::RestoreTableRevision {
                    table: table_name.clone(),
                    source: err,
                }
            })?
            .ok_or_else(|| Error::TableNotFound {
                table: table_name.clone(),
            })?;

        // Register all files in the revision
        register_revision_files(&data_store, &info)
            .await
            .map_err(|err| {
                tracing::error!(
                    table_name = %table_name,
                    error = %err,
                    error_source = logging::error_source(&err),
                    "failed to register files for table"
                );
                Error::RegisterFiles {
                    table: table_name.clone(),
                    source: err,
                }
            })?;

        tracing::info!(
            %dataset_ref,
            %table_name,
            location_id = %info.location_id,
            path = %info.path,
            "table restored from storage"
        );
    }

    Ok(StatusCode::OK)
}

/// Optional request body for restore table operation
#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct RestoreTablePayload {
    /// Location ID of the revision to activate. If omitted, the latest
    /// revision is discovered from object storage.
    #[cfg_attr(feature = "utoipa", schema(value_type = i64))]
    pub location_id: LocationId,
}

/// Errors that can occur when restoring a table
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid path parameters
    ///
    /// One or more path segments (namespace, name, revision, or table_name)
    /// could not be parsed into their expected types.
    #[error("Invalid path parameters: {0}")]
    InvalidPath(#[source] PathRejection),

    /// Invalid request body
    ///
    /// This occurs when:
    /// - The request body is not valid JSON
    /// - The JSON structure doesn't match the expected schema
    /// - Required fields are missing or have invalid types
    #[error("Invalid request body: {0}")]
    InvalidBody(#[source] serde_json::Error),

    /// Dataset or revision not found
    ///
    /// The combination of namespace, name, and revision does not match
    /// any registered dataset in the registry.
    #[error("Dataset '{namespace}/{name}' at revision '{revision}' not found")]
    NotFound {
        namespace: Namespace,
        name: Name,
        revision: Revision,
    },

    /// Failed to resolve revision to a manifest hash
    ///
    /// The datasets registry could not resolve the given revision reference
    /// (version string, hash, "latest", or "dev") to a concrete manifest hash.
    #[error("Failed to resolve revision: {0}")]
    ResolveRevision(#[source] ResolveRevisionError),

    /// Failed to load dataset from the dataset store
    ///
    /// The resolved hash reference exists but the dataset object could not
    /// be loaded from the backing store.
    #[error("Failed to load dataset: {0}")]
    GetDataset(#[source] common::dataset_store::GetDatasetError),

    /// Table not found in the dataset manifest
    ///
    /// The dataset was loaded successfully but does not contain a table
    /// definition matching the requested table name.
    #[error("Table '{table}' not found in dataset manifest")]
    TableNotInManifest { table: TableName },

    /// Failed to look up a revision by its location ID
    ///
    /// The metadata database query for the given `location_id` failed.
    #[error("Failed to get revision by location ID: {location_id}")]
    GetRevisionByLocationId {
        location_id: LocationId,
        #[source]
        source: amp_data_store::GetRevisionByLocationIdError,
    },

    /// No revision exists for the given location ID
    ///
    /// The provided `location_id` does not correspond to any registered
    /// physical table revision in the metadata database.
    #[error("No revision found for location_id {location_id}")]
    RevisionNotFound { location_id: LocationId },

    /// Failed to upsert physical table and activate revision
    ///
    /// The transactional upsert-and-activate operation failed. This covers
    /// registering the physical table row, deactivating existing revisions,
    /// and activating the specified `location_id`.
    #[error("Failed to register and activate physical table: {0}")]
    RegisterAndActivatePhysicalTable(
        #[source] amp_data_store::RegisterAndActivatePhysicalTableError,
    ),

    /// Failed to restore table revision from object storage
    ///
    /// The UUID heuristic scan of object storage failed, or no revision
    /// directories were found for the table.
    #[error("Failed to restore table revision for '{table}'")]
    RestoreTableRevision {
        table: TableName,
        #[source]
        source: amp_data_store::RestoreLatestTableRevisionError,
    },

    /// Failed to register parquet files for a restored table revision
    ///
    /// The revision was discovered and activated, but re-indexing the
    /// parquet file metadata from storage into the database failed.
    #[error("Failed to register files for table '{table}'")]
    RegisterFiles {
        table: TableName,
        #[source]
        source: RegisterRevisionFilesError,
    },

    /// Table data not found in object storage
    ///
    /// The UUID heuristic scan completed but found no revision directories
    /// for the table in object storage.
    #[error("Table '{table}' not found in object storage")]
    TableNotFound { table: TableName },
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidPath(_) => "INVALID_PATH",
            Error::InvalidBody(_) => "INVALID_BODY",
            Error::NotFound { .. } => "DATASET_NOT_FOUND",
            Error::ResolveRevision(_) => "RESOLVE_REVISION_ERROR",
            Error::GetDataset(_) => "GET_DATASET_ERROR",
            Error::TableNotInManifest { .. } => "TABLE_NOT_IN_MANIFEST",
            Error::GetRevisionByLocationId { .. } => "GET_REVISION_ERROR",
            Error::RevisionNotFound { .. } => "REVISION_NOT_FOUND",
            Error::RegisterAndActivatePhysicalTable(_) => "REGISTER_AND_ACTIVATE_ERROR",
            Error::RestoreTableRevision { .. } => "RESTORE_TABLE_REVISION_ERROR",
            Error::RegisterFiles { .. } => "REGISTER_FILES_ERROR",
            Error::TableNotFound { .. } => "TABLE_NOT_FOUND",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidPath(_) => StatusCode::BAD_REQUEST,
            Error::InvalidBody(_) => StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::ResolveRevision(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::GetDataset(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::TableNotInManifest { .. } => StatusCode::NOT_FOUND,
            Error::GetRevisionByLocationId { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Error::RevisionNotFound { .. } => StatusCode::NOT_FOUND,
            Error::RegisterAndActivatePhysicalTable(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::RestoreTableRevision { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Error::RegisterFiles { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Error::TableNotFound { .. } => StatusCode::NOT_FOUND,
        }
    }
}
