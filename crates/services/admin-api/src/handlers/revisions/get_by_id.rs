use amp_data_store::GetRevisionByLocationIdError;
use axum::{
    Json,
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use metadata_db::{
    jobs::JobId,
    physical_table_revision::{LocationId, PhysicalTableRevision},
};
use monitoring::logging;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for the `GET /revisions/{id}` endpoint
///
/// Returns a specific revision by location ID.
///
/// ## Path Parameters
/// - `id`: The unique identifier of the revision (must be a valid LocationId)
///
/// ## Response
/// - **200 OK**: Successfully retrieved revision
/// - **400 Bad Request**: Invalid path parameters
/// - **404 Not Found**: Revision not found
/// - **500 Internal Server Error**: Database error during retrieval
///
/// ## Error Codes
/// - `INVALID_PATH_PARAMETERS`: Invalid path parameters
/// - `REVISION_NOT_FOUND`: No revision exists with the specified location ID
/// - `GET_REVISION_BY_LOCATION_ID_ERROR`: Failed to retrieve revision from database
///
/// ## Behavior
/// This endpoint looks up a single physical table revision by its location ID
/// and returns its details including path, active status, writer, and metadata.
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        get,
        path = "/revisions/{id}",
        tag = "revisions",
        operation_id = "get_revision_by_location_id",
        params(
            ("id" = i64, Path, description = "Revision ID")
        ),
        responses(
            (status = 200, description = "Successfully retrieved revision", body = RevisionInfo),
            (status = 400, description = "Invalid path parameters", body = ErrorResponse),
            (status = 404, description = "Revision not found", body = ErrorResponse),
            (status = 500, description = "Internal server error", body = ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<LocationId>, PathRejection>,
) -> Result<Json<RevisionInfo>, ErrorResponse> {
    let location_id = match path {
        Ok(Path(location_id)) => location_id,
        Err(err) => {
            tracing::debug!(error = %err, error_source = logging::error_source(&err), "invalid path parameters");
            return Err(Error::InvalidPath(err).into());
        }
    };

    tracing::debug!(location_id = %location_id, "retrieving revision");

    let revision = ctx
        .data_store
        .get_revision_by_location_id(location_id)
        .await
        .map_err(Error::GetRevisionByLocationId)?
        .ok_or_else(|| {
            tracing::debug!(location_id = %location_id, "revision not found");
            Error::NotFound { location_id }
        })?;
    let revision_info: RevisionInfo = revision.into();

    tracing::info!(location_id = %location_id, "revision retrieved successfully");

    Ok(Json(revision_info))
}

/// Revision information returned by the API                                                                                                                          
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct RevisionInfo {
    /// Unique identifier for this revision (location ID)                                                                                                             
    #[cfg_attr(feature = "utoipa", schema(value_type = i64))]
    pub id: LocationId,
    /// Relative path to the storage location                                                                                                                         
    pub path: String,
    /// Whether this revision is currently active                                                                                                                     
    pub active: bool,
    /// Writer job ID responsible for populating this revision, if one exists                                                                                         
    #[cfg_attr(feature = "utoipa", schema(value_type = Option<i64>))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub writer: Option<JobId>,
    /// Metadata about the revision                                                                                                                                   
    pub metadata: RevisionMetadataInfo,
}

/// Revision metadata returned by the API
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct RevisionMetadataInfo {
    /// Dataset namespace
    #[serde(default)]
    pub dataset_namespace: String,
    /// Dataset name
    #[serde(default)]
    pub dataset_name: String,
    /// Manifest hash
    #[serde(default)]
    pub manifest_hash: String,
    /// Table name
    #[serde(default)]
    pub table_name: String,
}

impl From<PhysicalTableRevision> for RevisionInfo {
    fn from(value: PhysicalTableRevision) -> Self {
        let metadata: RevisionMetadataInfo = match serde_json::from_str(value.metadata.as_str()) {
            Ok(m) => m,
            Err(err) => {
                tracing::warn!(error = %err, "failed to deserialize revision metadata, using defaults");
                RevisionMetadataInfo::default()
            }
        };
        Self {
            id: value.id,
            path: value.path.into_inner(),
            active: value.active,
            writer: value.writer,
            metadata,
        }
    }
}

/// Errors that can occur when getting a revision by location ID
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
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidPath(_) => "INVALID_PATH_PARAMETERS",
            Error::NotFound { .. } => "REVISION_NOT_FOUND",
            Error::GetRevisionByLocationId(_) => "GET_REVISION_BY_LOCATION_ID_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidPath(_) => StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::GetRevisionByLocationId(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
