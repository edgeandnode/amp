use amp_datasets_registry::error::ResolveRevisionError;
use axum::{
    Json,
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use common::catalog::physical::{RestoreLatestTableRevisionError, restore_table_latest_revision};
use datasets_common::{
    name::Name, namespace::Namespace, reference::Reference, revision::Revision,
    table_name::TableName,
};
use futures::{StreamExt as _, stream::FuturesUnordered};
use metadata_db::LocationId;
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

    tracing::debug!(dataset_reference = %reference, "restoring dataset physical tables");

    let namespace = reference.namespace().clone();
    let name = reference.name().clone();
    let revision = reference.revision().clone();

    // Resolve reference to hash reference
    let dataset_ref = ctx
        .dataset_store
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

    let mut all_tasks: FuturesUnordered<JoinHandle<Result<RestoredTableInfo, Error>>> =
        FuturesUnordered::new();

    // Restore each table in the dataset concurrently
    for table_def in dataset.tables() {
        let data_store = ctx.data_store.clone();
        let dataset_ref = dataset_ref.clone();
        let table_def = table_def.clone();
        let table_name = table_def.name().clone();
        let start_block = dataset.start_block;

        tracing::debug!(%dataset_ref, %table_name, "restoring table");

        let task = tokio::spawn(async move {
            let sql_table_ref_schema = dataset_ref.to_reference().to_string();
            let physical_table = restore_table_latest_revision(
                data_store,
                &dataset_ref,
                start_block,
                &table_def,
                sql_table_ref_schema,
            )
            .await
            .map_err(|err| {
                tracing::error!(
                    error = %err,
                    error_source = logging::error_source(&err),
                    table = %table_name,
                    "failed to restore table from storage"
                );
                Error::RestoreTable {
                    table: table_name.clone(),
                    source: err,
                }
            })?
            .ok_or_else(|| Error::TableNotFound {
                table: table_name.clone(),
            })?;

            tracing::info!(
                %dataset_ref,
                %table_name,
                location_id = %physical_table.location_id(),
                path = %physical_table.path(),
                "table restored successfully"
            );

            Ok(RestoredTableInfo {
                table_name: table_name.to_string(),
                location_id: physical_table.location_id(),
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
        %dataset_ref,
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
    pub location_id: LocationId,
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

    /// Dataset or revision not found
    ///
    /// This occurs when:
    /// - The specified dataset name doesn't exist in the namespace
    /// - The specified revision doesn't exist for this dataset
    /// - The revision resolves to a manifest that doesn't exist
    #[error("Dataset '{namespace}/{name}' at revision '{revision}' not found")]
    NotFound {
        namespace: Namespace,
        name: Name,
        revision: Revision,
    },

    /// Dataset store operation error when resolving revision
    ///
    /// This occurs when:
    /// - Failed to resolve revision to manifest hash
    /// - Database connection issues
    /// - Internal database errors
    #[error("Failed to resolve revision: {0}")]
    ResolveRevision(#[source] ResolveRevisionError),

    /// Dataset store operation error when loading dataset
    ///
    /// This occurs when:
    /// - Failed to load dataset configuration from manifest
    /// - Manifest parsing errors
    /// - Invalid dataset structure
    #[error("Failed to load dataset: {0}")]
    GetDataset(#[source] amp_dataset_store::GetDatasetError),

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
        source: RestoreLatestTableRevisionError,
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
            Error::NotFound { .. } => "DATASET_NOT_FOUND",
            Error::ResolveRevision(_) => "RESOLVE_REVISION_ERROR",
            Error::GetDataset(_) => "GET_DATASET_ERROR",
            Error::RestoreTable { .. } => "RESTORE_TABLE_ERROR",
            Error::TableNotFound { .. } => "TABLE_NOT_FOUND",
            Error::TaskJoin { .. } => "TASK_JOIN_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidPath(_) => StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::ResolveRevision(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::GetDataset(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::RestoreTable { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Error::TableNotFound { .. } => StatusCode::NOT_FOUND,
            Error::TaskJoin { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
