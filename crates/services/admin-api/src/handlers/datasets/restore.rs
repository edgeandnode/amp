use axum::{
    Json,
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use common::catalog::{
    JobLabels,
    physical::{PhysicalTable, RestoreLatestRevisionError},
};
use datasets_common::{
    name::Name, namespace::Namespace, reference::Reference, revision::Revision,
    table_name::TableName,
};
use futures::{StreamExt as _, stream::FuturesUnordered};
use monitoring::logging;
use tokio::task::JoinHandle;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for the `POST /datasets/{namespace}/{name}/versions/{revision}/restore` endpoint
///
/// Restores physical table locations from object storage into the metadata database.
///
/// ## Path Parameters
/// - `namespace`: Dataset namespace
/// - `name`: Dataset name
/// - `revision`: Revision (version, hash, latest, or dev)
///
/// ## Response
/// - **202 Accepted**: Physical tables successfully restored from storage
/// - **400 Bad Request**: Invalid path parameters
/// - **404 Not Found**: Dataset or revision not found, or no tables found in storage
/// - **500 Internal Server Error**: Database or storage error
///
/// ## Error Codes
/// - `INVALID_PATH`: Invalid path parameters (namespace, name, or revision)
/// - `DATASET_NOT_FOUND`: The specified dataset or revision does not exist
/// - `GET_DATASET_ERROR`: Failed to load dataset from store
/// - `RESTORE_TABLE_ERROR`: Failed to restore a table from storage
/// - `TABLE_NOT_FOUND`: Table data not found in object storage
///
/// ## Behavior
/// This endpoint restores dataset physical tables from object storage:
/// 1. Resolves the revision to find the corresponding dataset
/// 2. Scans object storage for existing physical table files
/// 3. Re-indexes all Parquet file metadata from storage
/// 4. Registers the physical table locations in the metadata database
/// 5. Marks the restored locations as active
///
/// This is useful for:
/// - Recovering from metadata database loss
/// - Setting up a new system with pre-existing data
/// - Re-syncing metadata after storage restoration
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        post,
        path = "/datasets/{namespace}/{name}/versions/{revision}/restore",
        tag = "datasets",
        operation_id = "restore_dataset",
        params(
            ("namespace" = String, Path, description = "Dataset namespace"),
            ("name" = String, Path, description = "Dataset name"),
            ("revision" = String, Path, description = "Revision (version, hash, latest, or dev)")
        ),
        responses(
            (status = 202, description = "Physical tables successfully restored", body = RestoreResponse),
            (status = 400, description = "Bad request (invalid parameters)", body = ErrorResponse),
            (status = 404, description = "Dataset or revision not found", body = ErrorResponse),
            (status = 500, description = "Internal server error", body = ErrorResponse)
        )
    )
)]
pub async fn handler(
    path: Result<Path<(Namespace, Name, Revision)>, PathRejection>,
    State(ctx): State<Ctx>,
) -> Result<(StatusCode, Json<RestoreResponse>), ErrorResponse> {
    let reference = match path {
        Ok(Path((namespace, name, revision))) => Reference::new(namespace, name, revision),
        Err(err) => {
            tracing::debug!(error = %err, error_source = logging::error_source(&err), "invalid path parameters");
            return Err(Error::InvalidPath(err).into());
        }
    };

    tracing::debug!(dataset_reference=%reference, "restoring dataset physical tables");

    // Load the full dataset object using the resolved version
    let dataset = ctx
        .dataset_store
        .get_dataset(&reference)
        .await
        .map_err(Error::GetDataset)?;

    let job_labels = JobLabels {
        dataset_namespace: reference.namespace().clone(),
        dataset_name: reference.name().clone(),
        manifest_hash: dataset.manifest_hash().clone(),
    };

    let mut all_tasks: FuturesUnordered<JoinHandle<Result<RestoredTableInfo, Error>>> =
        FuturesUnordered::new();

    // Restore each table in the dataset concurrently
    for table in dataset.resolved_tables(reference.clone().into()) {
        tracing::debug!(dataset_reference=%reference, table_name=%table.name(), "restoring table");

        let data_store = ctx.config.data_store.clone();
        let metadata_db = ctx.metadata_db.clone();
        let job_labels = job_labels.clone();
        let reference_clone = reference.clone();

        let task = tokio::spawn(async move {
            let physical_table = PhysicalTable::restore_latest_revision(
                &table,
                data_store,
                metadata_db,
                &job_labels,
            )
            .await
            .map_err(|err| {
                tracing::error!(
                    error = %err,
                    error_source = logging::error_source(&err),
                    table = %table.name(),
                    "failed to restore table from storage"
                );
                Error::RestoreTable {
                    table: table.name().clone(),
                    source: err,
                }
            })?
            .ok_or_else(|| Error::TableNotFound {
                table: table.name().clone(),
            })?;

            tracing::info!(
                datset_reference=%reference_clone,
                table_name = %table.name(),
                location_id = %physical_table.location_id(),
                url = %physical_table.url(),
                "table restored successfully"
            );

            Ok(RestoredTableInfo {
                table_name: table.name().to_string(),
                location_id: *physical_table.location_id(),
                url: physical_table.url().to_string(),
            })
        });

        all_tasks.push(task);
    }

    let mut restored_tables = Vec::new();
    while let Some(result) = all_tasks.next().await {
        restored_tables.push(result.map_err(|err| {
            tracing::error!(
                error = %err,
                error_source = logging::error_source(&err),
                "task join error during table restoration"
            );
            Error::TaskJoin { source: err }
        })??);
    }

    tracing::info!(
        dataset_reference = %reference,
        tables_restored = restored_tables.len(),
        "dataset restoration complete"
    );

    Ok((
        StatusCode::ACCEPTED,
        Json(RestoreResponse {
            tables: restored_tables,
        }),
    ))
}

/// Response for restore operation
#[derive(serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct RestoreResponse {
    /// List of restored physical tables
    pub tables: Vec<RestoredTableInfo>,
}

/// Information about a restored physical table
#[derive(serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct RestoredTableInfo {
    /// Name of the table within the dataset
    pub table_name: String,
    /// Unique location ID assigned in the metadata database
    #[cfg_attr(feature = "utoipa", schema(value_type = i64))]
    pub location_id: i64,
    /// Full URL to the storage location
    pub url: String,
}

/// Errors that can occur when restoring a dataset
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid path parameters
    ///
    /// This occurs when:
    /// - The namespace, name, or revision in the URL path is invalid
    /// - Path parameter parsing fails
    #[error("Invalid path parameters: {0}")]
    InvalidPath(#[source] PathRejection),

    /// Dataset store operation error when loading dataset
    ///
    /// This occurs when:
    /// - Failed to load dataset configuration from manifest
    /// - Manifest parsing errors
    /// - Invalid dataset structure
    #[error("Failed to load dataset: {0}")]
    GetDataset(#[source] dataset_store::GetDatasetError),

    /// Failed to restore table from storage
    ///
    /// This occurs when:
    /// - Error scanning object storage for table files
    /// - Error registering location in metadata database
    /// - Error re-indexing Parquet file metadata
    #[error("Failed to restore table '{table}'")]
    RestoreTable {
        table: TableName,
        #[source]
        source: RestoreLatestRevisionError,
    },

    /// Table data not found in object storage
    ///
    /// This occurs when:
    /// - No physical table files exist in storage for this table
    /// - Table has never been dumped or data was deleted
    #[error("Table '{table}' not found in object storage")]
    TableNotFound { table: TableName },

    /// Failed to join restoration task
    ///
    /// This occurs when:
    /// - A spawned task panicked during table restoration
    /// - Task was cancelled unexpectedly
    #[error("Failed to join restoration task")]
    TaskJoin {
        #[source]
        source: tokio::task::JoinError,
    },
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidPath(_) => "INVALID_PATH",
            Error::GetDataset(_) => "GET_DATASET_ERROR",
            Error::RestoreTable { .. } => "RESTORE_TABLE_ERROR",
            Error::TableNotFound { .. } => "TABLE_NOT_FOUND",
            Error::TaskJoin { .. } => "TASK_JOIN_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidPath(_) => StatusCode::BAD_REQUEST,
            Error::GetDataset(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::RestoreTable { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Error::TableNotFound { .. } => StatusCode::NOT_FOUND,
            Error::TaskJoin { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
