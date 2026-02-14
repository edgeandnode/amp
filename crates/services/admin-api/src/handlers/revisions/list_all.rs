use axum::{
    Json,
    extract::{Query, State, rejection::QueryRejection},
    http::StatusCode,
};
use metadata_db::{JobId, LocationId, physical_table::PhysicalTableRevision};
use monitoring::logging;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for the `GET /revisions` endpoint
///
/// Returns all physical table revisions, with an optional active status filter.
///
/// ## Query Parameters
/// - `active` (optional): Filter by active status (`true` or `false`)
///
/// ## Response
/// - **200 OK**: Successfully retrieved revisions
/// - **400 Bad Request**: Invalid query parameters
/// - **500 Internal Server Error**: Database error during listing
///
/// ## Error Codes
/// - `INVALID_QUERY_PARAMETERS`: Invalid query parameters
/// - `METADATA_DB_ERROR`: Internal database error occurred
///
/// ## Behavior
/// This endpoint lists all physical table revisions. When the `active` query
/// parameter is provided, only revisions matching that active status are returned.
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        get,
        path = "/revisions",
        tag = "revisions",
        operation_id = "list_revisions",
        params(
            ("active" = Option<bool>, Query, description = "Filter by active status")
        ),
        responses(
            (status = 200, description = "Successfully retrieved revisions", body = Vec<RevisionInfo>),
            (status = 400, description = "Invalid query parameters", body = ErrorResponse),
            (status = 500, description = "Internal server error", body = ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    query: Result<Query<QueryParams>, QueryRejection>,
) -> Result<Json<Vec<RevisionInfo>>, ErrorResponse> {
    let query = match query {
        Ok(Query(query)) => query,
        Err(err) => {
            tracing::debug!(error = %err, error_source = logging::error_source(&err), "invalid query parameters");
            return Err(Error::InvalidQueryParams(err).into());
        }
    };

    let revisions = metadata_db::physical_table::list_all(&ctx.metadata_db, query.active)
        .await
        .map_err(|err| {
            tracing::debug!(error = %err, error_source = logging::error_source(&err), "failed to list revisions");
            Error::MetadataDbError(err)
        })?;

    tracing::info!(
        revision_count = revisions.len(),
        "revisions listed successfully"
    );

    let revisions: Vec<RevisionInfo> = revisions.into_iter().map(Into::into).collect();
    Ok(Json(revisions))
}

/// Query parameters for the list revisions endpoint
#[derive(Debug, serde::Deserialize)]
pub struct QueryParams {
    /// Filter by active status
    pub active: Option<bool>,
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
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct RevisionMetadataInfo {
    /// Dataset namespace
    pub dataset_namespace: String,
    /// Dataset name
    pub dataset_name: String,
    /// Manifest hash
    pub manifest_hash: String,
    /// Table name
    pub table_name: String,
}

impl From<PhysicalTableRevision> for RevisionInfo {
    fn from(value: PhysicalTableRevision) -> Self {
        Self {
            id: value.id,
            path: value.path.into_inner(),
            active: value.active,
            writer: value.writer,
            metadata: RevisionMetadataInfo {
                dataset_namespace: value.metadata.dataset_namespace.clone(),
                dataset_name: value.metadata.dataset_name.clone(),
                manifest_hash: value.metadata.manifest_hash.clone(),
                table_name: value.metadata.table_name.clone(),
            },
        }
    }
}

/// Errors that can occur when listing revisions
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid query parameters
    ///
    /// This occurs when the query string cannot be parsed.
    #[error("Invalid query parameters: {0}")]
    InvalidQueryParams(#[source] QueryRejection),
    /// An error occurred while querying the metadata database
    ///
    /// This covers database connection issues, query failures,
    /// and other internal database errors.
    #[error("metadata db error: {0}")]
    MetadataDbError(#[source] metadata_db::Error),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidQueryParams(_) => "INVALID_QUERY_PARAMETERS",
            Error::MetadataDbError(_) => "METADATA_DB_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidQueryParams(_) => StatusCode::BAD_REQUEST,
            Error::MetadataDbError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
