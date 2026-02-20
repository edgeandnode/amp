use amp_data_store::{
    GetRevisionByLocationIdError, PhyTableRevision,
    physical_table::{PhyTableRevisionPath, PhyTableUrl},
};
use axum::{
    Json,
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
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

/// Handler for the `POST /revisions/{id}/restore` endpoint
///
/// Restores a revision by re-registering its files from object storage.
///
/// ## Path Parameters
/// - `id`: The unique identifier of the revision (must be a valid LocationId)
///
/// ## Response
/// - **200 OK**: Revision restored successfully
/// - **400 Bad Request**: Invalid path parameters
/// - **404 Not Found**: Revision not found
/// - **500 Internal Server Error**: Database or file registration error
///
/// ## Error Codes
/// - `INVALID_PATH_PARAMETERS`: Invalid path parameters
/// - `REVISION_NOT_FOUND`: No revision exists with the specified location ID
/// - `GET_REVISION_BY_LOCATION_ID_ERROR`: Failed to retrieve revision from database
/// - `REGISTER_FILES_ERROR`: Failed to register revision files from object storage
///
/// ## Behavior
/// This endpoint looks up the physical table revision by its location ID, then
/// lists all files in the revision's object storage directory. For each file,
/// it reads Amp-specific Parquet metadata and registers it in the metadata database.
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        post,
        path = "/revisions/{id}/restore",
        tag = "revisions",
        operation_id = "restore_revision",
        params(
            ("id" = i64, Path, description = "Revision ID")
        ),
        responses(
            (status = 200, description = "Revision restored successfully", body = RestoreResponse),
            (status = 400, description = "Invalid path parameters", body = ErrorResponse),
            (status = 404, description = "Revision not found", body = ErrorResponse),
            (status = 500, description = "Internal server error", body = ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<LocationId>, PathRejection>,
) -> Result<Json<RestoreResponse>, ErrorResponse> {
    let location_id = match path {
        Ok(Path(location_id)) => location_id,
        Err(err) => {
            tracing::debug!(error = %err, error_source = logging::error_source(&err), "invalid path parameters");
            return Err(Error::InvalidPath(err).into());
        }
    };

    tracing::debug!(location_id = %location_id, "restoring revision");

    let revision = ctx
        .data_store
        .get_revision_by_location_id(location_id)
        .await
        .map_err(Error::GetRevisionByLocationId)?
        .ok_or_else(|| {
            tracing::debug!(location_id = %location_id, "revision not found");
            Error::NotFound { location_id }
        })?;
    let path: PhyTableRevisionPath = revision.path.into();
    let url = PhyTableUrl::new(ctx.data_store.url(), &path);
    let phy_table_revision = PhyTableRevision {
        location_id: revision.id,
        path,
        url,
    };

    let total_files = register_revision_files(&ctx.data_store, &phy_table_revision)
        .await
        .map_err(Error::RegisterRevisionFiles)?;

    if total_files == 0 {
        tracing::warn!(location_id = %location_id, "no files found to restore");
    }

    tracing::info!(location_id = %location_id, "revision restored");

    Ok(Json(RestoreResponse { total_files }))
}

/// Response for restore operation                                                                                                                          
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct RestoreResponse {
    /// Total number of files restored                                                                                                             
    pub total_files: i32,
}

/// Errors that can occur when restoring a revision
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid path parameters
    ///
    /// This occurs when:
    /// - The location ID in the URL path is not a valid integer
    /// - Path parameter parsing fails
    #[error("Invalid path parameters: {0}")]
    InvalidPath(#[source] PathRejection),
    /// Revision not found
    ///
    /// This occurs when:
    /// - The specified location ID doesn't exist in the metadata database
    #[error("Revision with location ID '{location_id}' not found")]
    NotFound { location_id: LocationId },
    /// Failed to get revision by location ID
    ///
    /// This occurs when:
    /// - The database query to get revision by location ID fails
    /// - Database connection issues
    /// - Internal database errors during get revision by location ID
    #[error("Failed to get revision by location ID")]
    GetRevisionByLocationId(#[source] GetRevisionByLocationIdError),

    /// Failed to register files for revision
    ///
    /// This occurs when:
    /// - Error listing files in object storage
    /// - Error reading Parquet file metadata
    /// - Error registering file metadata in database
    #[error("Failed to register files for revision")]
    RegisterRevisionFiles(#[source] RegisterRevisionFilesError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidPath(_) => "INVALID_PATH_PARAMETERS",
            Error::NotFound { .. } => "REVISION_NOT_FOUND",
            Error::GetRevisionByLocationId(_) => "GET_REVISION_BY_LOCATION_ID_ERROR",
            Error::RegisterRevisionFiles(_) => "REGISTER_FILES_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidPath(_) => StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::GetRevisionByLocationId(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::RegisterRevisionFiles(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
