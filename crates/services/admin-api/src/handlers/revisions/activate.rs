use amp_data_store::ActivateTableRevisionError;
use amp_datasets_registry::error::ResolveRevisionError;
use axum::{
    Json,
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use common::dataset_store::GetDatasetError;
use datasets_common::{reference::Reference, table_name::TableName};
use metadata_db::physical_table_revision::LocationId;
use monitoring::logging;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for the `POST /revisions/activate` endpoint
///
/// Activates a specific table revision by location ID.
///
/// ## Request Body
/// - `dataset`: Dataset reference (namespace/name or with revision)
/// - `table_name`: Name of the table whose revision to activate
/// - `location_id`: Location ID of the revision to activate
///
/// ## Response
/// - **200 OK**: Successfully activated the table revision
/// - **404 Not Found**: Dataset or revision not found
/// - **500 Internal Server Error**: Database error during activation
///
/// ## Error Codes
/// - `DATASET_NOT_FOUND`: The specified dataset or revision does not exist
/// - `TABLE_NOT_IN_MANIFEST`: The table name does not exist in the dataset manifest
/// - `TABLE_NOT_REGISTERED`: The table is not registered in the data store
/// - `GET_DATASET_ERROR`: Failed to load the dataset from its manifest
/// - `ACTIVATE_TABLE_REVISION_ERROR`: Failed to activate the table revision
/// - `RESOLVE_REVISION_ERROR`: Failed to resolve revision to manifest hash
///
/// ## Behavior
/// This endpoint resolves the dataset reference to a manifest hash, validates that
/// the table name exists in the dataset manifest, then atomically deactivates all
/// existing revisions for the table and marks the specified revision as active.
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        post,
        path = "/revisions/{id}/activate",
        tag = "revisions",
        operation_id = "activate_table_revision",
        request_body = ActivationPayload,
        params(
            ("id" = u64, Path, description = "Location ID of the revision to activate")
        ),
        responses(
            (status = 200, description = "Successfully activated the table revision"),
            (status = 404, description = "Dataset or revision not found", body = ErrorResponse),
            (status = 500, description = "Internal server error", body = ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<LocationId>, PathRejection>,
    Json(payload): Json<ActivationPayload>,
) -> Result<StatusCode, ErrorResponse> {
    let reference = payload.dataset.clone();
    let location_id = match path {
        Ok(Path(location_id)) => location_id,
        Err(err) => {
            tracing::debug!(error = %err, error_source = logging::error_source(&err), "invalid path parameters");
            return Err(Error::InvalidPath(err).into());
        }
    };

    tracing::debug!(dataset_reference = %reference, table_name = %payload.table_name, location_id = %location_id, "activating table revision");

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

    let dataset = ctx
        .dataset_store
        .get_dataset(&reference)
        .await
        .map_err(|err| {
            tracing::error!(
                dataset_reference = %payload.dataset,
                error = %err,
                error_source = logging::error_source(&err),
                "failed to load dataset"
            );
            Error::GetDataset(err)
        })?;

    if !dataset
        .tables()
        .iter()
        .any(|t| t.name() == &payload.table_name)
    {
        tracing::debug!(
            table_name = %payload.table_name,
            dataset_reference = %payload.dataset,
            "table not found in dataset manifest"
        );
        return Err(Error::TableNotInManifest {
            table_name: payload.table_name.clone(),
            dataset: payload.dataset.clone(),
        }
        .into());
    }

    ctx.data_store
        .activate_table_revision(&reference, &payload.table_name, location_id)
        .await
        .map_err(|err| match err {
            ActivateTableRevisionError::TableNotFound => {
                tracing::debug!(table_name = %payload.table_name, "table not registered in data store");
                Error::TableNotRegistered {
                    table_name: payload.table_name.clone(),
                    dataset: payload.dataset,
                }
            }
            err => {
                tracing::error!(table_name = %payload.table_name, location_id = %location_id, error = %err, error_source = logging::error_source(&err), "failed to activate table revision");
                Error::ActivateTableRevision(err)
            }
        })?;

    tracing::info!(table_name = %payload.table_name, location_id = %location_id, "table revision activated successfully");

    Ok(StatusCode::OK)
}

/// Payload for activating a table revision
#[derive(Debug, Clone, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct ActivationPayload {
    /// Table name
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub table_name: TableName,
    /// Dataset reference
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub dataset: Reference,
}

/// Errors that can occur when activating a table revision
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid path parameters
    ///
    /// This occurs when:
    /// - The location ID in the URL path is invalid
    /// - Path parameter parsing fails
    #[error("Invalid path parameters: {0}")]
    InvalidPath(#[source] PathRejection),
    /// Dataset or revision not found
    ///
    /// This occurs when:
    /// - The specified dataset name doesn't exist in the namespace
    /// - The specified revision doesn't exist for this dataset
    /// - The revision resolves to a manifest that doesn't exist
    #[error("Dataset '{dataset}' not found")]
    NotFound { dataset: Reference },
    /// Table name not found in dataset manifest
    ///
    /// This occurs when:
    /// - The table name does not exist in the dataset manifest definition
    #[error("Table '{table_name}' not found in manifest for dataset '{dataset}'")]
    TableNotInManifest {
        table_name: TableName,
        dataset: Reference,
    },
    /// No physical table found for the given dataset and table name
    ///
    /// This occurs when:
    /// - The table is not registered in the data store
    #[error("Table '{table_name}' not registered for dataset '{dataset}'")]
    TableNotRegistered {
        table_name: TableName,
        dataset: Reference,
    },
    /// Failed to load dataset from manifest
    ///
    /// This occurs when:
    /// - The manifest content could not be retrieved from object store
    /// - The manifest could not be parsed for the dataset kind
    /// - The dataset kind is unsupported
    #[error("Failed to load dataset: {0}")]
    GetDataset(#[source] GetDatasetError),

    /// Failed to activate table revision
    ///
    /// This occurs when:
    /// - The database transaction to activate the revision fails
    /// - Marking existing revisions as inactive fails
    /// - Committing the activation transaction fails
    #[error("Failed to activate table revision")]
    ActivateTableRevision(#[source] ActivateTableRevisionError),
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
            Error::InvalidPath(_) => "INVALID_PATH_PARAMETERS",
            Error::NotFound { .. } => "DATASET_NOT_FOUND",
            Error::TableNotInManifest { .. } => "TABLE_NOT_IN_MANIFEST",
            Error::TableNotRegistered { .. } => "TABLE_NOT_REGISTERED",
            Error::GetDataset(_) => "GET_DATASET_ERROR",
            Error::ActivateTableRevision(_) => "ACTIVATE_TABLE_REVISION_ERROR",
            Error::ResolveRevision(_) => "RESOLVE_REVISION_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidPath(_) => StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::TableNotInManifest { .. } => StatusCode::NOT_FOUND,
            Error::TableNotRegistered { .. } => StatusCode::NOT_FOUND,
            Error::GetDataset(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::ActivateTableRevision(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::ResolveRevision(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
