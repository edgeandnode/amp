use amp_data_store::{RegisterTableRevisionError, physical_table::PhyTableRevisionPath};
use amp_datasets_registry::error::ResolveRevisionError;
use axum::{Json, extract::State, http::StatusCode};
use datasets_common::{reference::Reference, table_name::TableName};
use metadata_db::LocationId;
use monitoring::logging;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for the `POST /revisions` endpoint
///
/// Creates an inactive, unlinked physical table revision record from a given path.
/// This is a low-level API that only inserts into `physical_table_revisions`.
/// It does NOT create `physical_tables` entries or activate the revision.
///
/// ## Request Body
/// - `dataset`: Dataset reference (namespace/name or with revision)
/// - `table_name`: Name of the table to create a revision for
/// - `path`: Physical table revision path in object storage
///
/// ## Response
/// - **200 OK**: Successfully created the table revision
/// - **404 Not Found**: Dataset or revision not found
/// - **500 Internal Server Error**: Database error during creation
///
/// ## Error Codes
/// - `DATASET_NOT_FOUND`: The specified dataset or revision does not exist
/// - `REGISTER_TABLE_REVISION_ERROR`: Failed to register the table revision
/// - `RESOLVE_REVISION_ERROR`: Failed to resolve revision to manifest hash
///
/// This handler:
/// 1. Resolves the dataset reference to a manifest hash
/// 2. Idempotently creates a revision record in the metadata database
/// 3. Returns the assigned location ID
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        post,
        path = "/revisions",
        tag = "revisions",
        operation_id = "create_table_revision",
        request_body = CreatePayload,
        responses(
            (status = 200, description = "Successfully created the table revision", body = CreateRevisionResponse),
            (status = 404, description = "Dataset, revision, or table not found", body = ErrorResponse),
            (status = 500, description = "Internal server error", body = ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    Json(payload): Json<CreatePayload>,
) -> Result<Json<CreateRevisionResponse>, ErrorResponse> {
    let dataset_ref = payload.dataset;

    tracing::debug!(
        dataset_reference = %dataset_ref,
        table_name = %payload.table_name,
        path = %payload.path,
        "creating table revision"
    );

    let reference = ctx
        .datasets_registry
        .resolve_revision(&dataset_ref)
        .await
        .map_err(|err| {
            tracing::error!(
                dataset_reference = %dataset_ref,
                error = %err,
                error_source = logging::error_source(&err),
                "failed to resolve revision"
            );
            Error::ResolveRevision(err)
        })?
        .ok_or_else(|| {
            tracing::debug!(dataset = %dataset_ref, "dataset not found");
            Error::NotFound {
                dataset: dataset_ref.clone(),
            }
        })?;

    let location_id = ctx
        .data_store
        .register_table_revision(&reference, &payload.table_name, &payload.path)
        .await
        .map_err(|err| {
            tracing::error!(
                table_name = %payload.table_name,
                path = %payload.path,
                error = %err,
                error_source = logging::error_source(&err),
                "failed to register table revision"
            );
            Error::RegisterTableRevision(err)
        })?;

    tracing::info!(
        table_name = %payload.table_name,
        path = %payload.path,
        location_id = %location_id,
        "table revision created"
    );

    Ok(Json(CreateRevisionResponse { location_id }))
}

/// Response for creating a table revision
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct CreateRevisionResponse {
    /// Location ID assigned to the new revision
    #[cfg_attr(feature = "utoipa", schema(value_type = i64))]
    pub location_id: LocationId,
}

/// Payload for creating a table revision
#[derive(Debug, Clone, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct CreatePayload {
    /// Table name
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub table_name: TableName,
    /// Dataset reference
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub dataset: Reference,
    /// Revision path in object storage
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub path: PhyTableRevisionPath,
}

/// Errors that can occur when creating a table revision
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Dataset or revision not found
    ///
    /// This occurs when:
    /// - The specified dataset name doesn't exist in the namespace
    /// - The specified revision doesn't exist for this dataset
    /// - The revision resolves to a manifest that doesn't exist
    #[error("Dataset '{dataset}' not found")]
    NotFound { dataset: Reference },

    /// Failed to resolve revision to manifest hash
    ///
    /// This occurs when:
    /// - Failed to query metadata database for revision information
    /// - Database connection issues
    /// - Internal database errors during revision resolution
    #[error("Failed to resolve revision: {0}")]
    ResolveRevision(#[source] ResolveRevisionError),

    /// Failed to register table revision
    ///
    /// This occurs when:
    /// - Failed to register table revision in metadata database
    /// - Database connection issues
    /// - Internal database errors during table revision registration
    #[error("Failed to register table revision: {0}")]
    RegisterTableRevision(#[source] RegisterTableRevisionError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::NotFound { .. } => "DATASET_NOT_FOUND",
            Error::ResolveRevision(_) => "RESOLVE_REVISION_ERROR",
            Error::RegisterTableRevision(_) => "REGISTER_TABLE_REVISION_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::ResolveRevision(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::RegisterTableRevision(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
