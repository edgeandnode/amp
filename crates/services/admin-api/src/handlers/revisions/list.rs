use amp_data_store::ListAllTableRevisionsError;
use axum::{
    Json,
    extract::{Query, State, rejection::QueryRejection},
    http::StatusCode,
};
use metadata_db::physical_table_revision::LocationId;
use monitoring::logging;

use crate::{
    ctx::Ctx,
    handlers::{
        error::{ErrorResponse, IntoErrorResponse},
        revisions::get_by_id::RevisionInfo,
    },
};

/// Handler for the `GET /revisions` endpoint
///
/// Returns physical table revisions with cursor-based pagination and optional active
/// status filtering.
///
/// ## Query Parameters
/// - `active` (optional): Filter by active status (`true` or `false`)
/// - `limit` (optional): Maximum number of revisions to return (default: 100)
/// - `last_id` (optional): Cursor for pagination — ID of the last revision from the
///   previous page. Omit for the first page.
///
/// ## Response
/// - **200 OK**: Successfully retrieved revisions
/// - **400 Bad Request**: Invalid query parameters
/// - **500 Internal Server Error**: Database error during listing
///
/// ## Error Codes
/// - `INVALID_QUERY_PARAMETERS`: Invalid query parameters
/// - `LIST_ALL_TABLE_REVISIONS_ERROR`: Failed to list table revisions
///
/// This handler:
/// - Validates and extracts optional `active`, `limit`, and `last_id` query parameters
/// - Calls the data store to list table revisions with the given filters and cursor
/// - Returns revision information as a JSON array
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        get,
        path = "/revisions",
        tag = "revisions",
        operation_id = "list_revisions",
        params(
            ("active" = Option<bool>, Query, description = "Filter by active status"),
            ("limit" = Option<i64>, Query, description = "Maximum number of revisions to return (default: 100)"),
            ("last_id" = Option<i64>, Query, description = "Cursor for pagination: ID of the last revision from the previous page")
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

    if query.limit < 0 {
        tracing::debug!(limit = query.limit, "negative limit");
        return Err(Error::NegativeLimit(query.limit).into());
    }

    let revisions =
        ctx.data_store.list_all_table_revisions(query.active, query.limit, query.last_id)
            .await
            .map_err(|err| {
                tracing::debug!(error = %err, error_source = logging::error_source(&err), "failed to list revisions");
                Error::ListAllTableRevisions(err)
            })?;

    tracing::info!(revision_count = revisions.len(), "revisions listed");

    let revisions: Vec<RevisionInfo> = revisions.into_iter().map(Into::into).collect();
    Ok(Json(revisions))
}

/// Query parameters for the list revisions endpoint
#[derive(Debug, serde::Deserialize)]
pub struct QueryParams {
    /// Filter by active status
    pub active: Option<bool>,
    /// Maximum number of revisions to return (default: 100)
    #[serde(default = "default_limit")]
    pub limit: i64,
    /// Cursor for pagination: ID of the last revision from the previous page
    pub last_id: Option<LocationId>,
}

fn default_limit() -> i64 {
    100
}

/// Errors that can occur when listing revisions
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid query parameters
    ///
    /// This occurs when the query string cannot be parsed.
    #[error("Invalid query parameters")]
    InvalidQueryParams(#[source] QueryRejection),
    /// Negative limit value
    ///
    /// The `limit` query parameter must be non-negative.
    #[error("limit must be non-negative, got {0}")]
    NegativeLimit(i64),
    /// An error occurred while listing table revisions
    ///
    /// This covers data store errors when retrieving revisions,
    /// including underlying database connection issues and query failures.
    #[error("failed to list table revisions")]
    ListAllTableRevisions(#[source] ListAllTableRevisionsError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidQueryParams(_) | Error::NegativeLimit(_) => "INVALID_QUERY_PARAMETERS",
            Error::ListAllTableRevisions(_) => "LIST_ALL_TABLE_REVISIONS_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidQueryParams(_) | Error::NegativeLimit(_) => StatusCode::BAD_REQUEST,
            Error::ListAllTableRevisions(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
