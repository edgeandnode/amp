use amp_data_store::DeactivateTableRevisionError;
use amp_datasets_registry::error::ResolveRevisionError;
use axum::{Json, extract::State, http::StatusCode};
use datasets_common::{reference::Reference, table_name::TableName};
use monitoring::logging;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for the `POST /revisions/deactivate` endpoint
///
/// Deactivates all revisions for a specific table.
///
/// ## Request Body
/// - `dataset`: Dataset reference (namespace/name or with revision)
/// - `table_name`: Name of the table whose revisions to deactivate
///
/// ## Response
/// - **200 OK**: Successfully deactivated all table revisions
/// - **404 Not Found**: Dataset or revision not found
/// - **500 Internal Server Error**: Database error during deactivation
///
/// ## Error Codes
/// - `DATASET_NOT_FOUND`: The specified dataset or revision does not exist
/// - `TABLE_NOT_FOUND`: No physical table exists for the dataset and table name
/// - `DEACTIVATE_TABLE_REVISION_ERROR`: Failed to deactivate the table revisions
/// - `RESOLVE_REVISION_ERROR`: Failed to resolve revision to manifest hash
///
/// ## Behavior
/// This endpoint resolves the dataset reference to a manifest hash, then marks all
/// revisions for the specified table as inactive.
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        post,
        path = "/revisions/deactivate",
        tag = "revisions",
        operation_id = "deactivate_table_revision",
        request_body = DeactivationPayload,
        responses(
            (status = 200, description = "Successfully deactivated the table revisions"),
            (status = 404, description = "Dataset or revision not found", body = ErrorResponse),
            (status = 500, description = "Internal server error", body = ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    Json(payload): Json<DeactivationPayload>,
) -> Result<StatusCode, ErrorResponse> {
    let reference = payload.dataset.clone();

    tracing::debug!(dataset_reference = %reference, table_name = %payload.table_name, "deactivating table revision");

    let reference = ctx
        .datasets_registry
        .resolve_revision(&reference)
        .await
        .map_err(|err| {
            tracing::error!(dataset_reference = %reference, error = %err, error_source = logging::error_source(&err), "failed to resolve revision");
            Error::ResolveRevision(err)
        })?
        .ok_or_else(|| {
            tracing::debug!(dataset = %reference, "dataset not found");
            Error::NotFound {
                dataset: reference.clone(),
            }
        })?;

    ctx.data_store
        .deactivate_table_revision(&reference, &payload.table_name)
        .await
        .map_err(|err| {
            tracing::error!(table_name = %payload.table_name, error = %err, error_source = logging::error_source(&err), "failed to deactivate table revision");
            match err {
                DeactivateTableRevisionError::TableNotFound => {
                    Error::TableNotFound {
                        table_name: payload.table_name.clone(),
                        dataset: payload.dataset,
                    }
                }
                err => Error::DeactivateTableRevision(err),
            }
        })?;

    tracing::info!(table_name = %payload.table_name, "table revision deactivated successfully");

    Ok(StatusCode::OK)
}

/// Payload for deactivating a table revision
#[derive(Debug, Clone, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct DeactivationPayload {
    /// Table name
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub table_name: TableName,
    /// Dataset reference
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub dataset: Reference,
}

/// Errors that can occur when deactivating a table revision
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
    /// No physical table found for the given dataset and table name
    ///
    /// This occurs when:
    /// - The table name does not exist in the dataset
    #[error("Table '{table_name}' not found for dataset '{dataset}'")]
    TableNotFound {
        table_name: TableName,
        dataset: Reference,
    },
    /// Failed to deactivate table revision
    ///
    /// This occurs when:
    /// - The database operation to mark revisions as inactive fails
    /// - Database connection issues during the update
    #[error("Failed to deactivate table revision: {0}")]
    DeactivateTableRevision(#[source] DeactivateTableRevisionError),
    /// Failed to resolve revision to manifest hash
    ///
    /// This occurs when:
    /// - Failed to query metadata database for revision information
    /// - Database connection issues
    /// - Internal database errors during revision resolution
    #[error("Failed to resolve revision: {0}")]
    ResolveRevision(#[source] ResolveRevisionError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::NotFound { .. } => "DATASET_NOT_FOUND",
            Error::TableNotFound { .. } => "TABLE_NOT_FOUND",
            Error::DeactivateTableRevision(_) => "DEACTIVATE_TABLE_REVISION_ERROR",
            Error::ResolveRevision(_) => "RESOLVE_REVISION_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::TableNotFound { .. } => StatusCode::NOT_FOUND,
            Error::DeactivateTableRevision(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::ResolveRevision(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
