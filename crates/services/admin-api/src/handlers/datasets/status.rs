use axum::{
    Json,
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use common::{BlockNum, metadata::parquet::ParquetMeta};
use datasets_common::{name::Name, namespace::Namespace, revision::Revision};
use futures::StreamExt;
use monitoring::logging;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for `GET /datasets/{namespace}/{name}/versions/{revision}/status`
///
/// Returns status information for all tables in a dataset including block coverage statistics.
///
/// ## Response
/// - **200 OK**: Successfully retrieved dataset status (even if tables are empty)
/// - **400 Bad Request**: Invalid path parameters
/// - **404 Not Found**: Dataset or revision not found
/// - **500 Internal Server Error**: Database or parsing errors
///
/// ## Error Codes
/// - `INVALID_PATH`: Invalid namespace, name, or revision in path
/// - `DATASET_NOT_FOUND`: Dataset or revision not found
/// - `NO_ACTIVE_TABLES`: No active tables found for dataset
/// - `RESOLVE_REVISION_ERROR`: Failed to resolve revision to manifest hash
/// - `METADATA_DB_ERROR`: Database query error
/// - `PARSE_METADATA_ERROR`: Failed to parse parquet metadata
///
/// ## Behavior
/// - Returns status for all active tables in the dataset
/// - Empty tables (no segments) return valid status with null blocks and zero counts
/// - Only returns active physical tables
/// - Computes statistics across all segments in each table
/// - Tables are ordered alphabetically by name
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        get,
        path = "/datasets/{namespace}/{name}/versions/{revision}/status",
        tag = "datasets",
        operation_id = "get_dataset_status",
        params(
            ("namespace" = String, Path, description = "Dataset namespace"),
            ("name" = String, Path, description = "Dataset name"),
            ("revision" = String, Path, description = "Revision (version, hash, latest, or dev)")
        ),
        responses(
            (status = 200, description = "Successfully retrieved dataset status", body = DatasetStatusResponse),
            (status = 400, description = "Invalid path parameters", body = ErrorResponse),
            (status = 404, description = "Dataset or revision not found", body = ErrorResponse),
            (status = 500, description = "Internal server error", body = ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<(Namespace, Name, Revision)>, PathRejection>,
) -> Result<Json<DatasetStatusResponse>, ErrorResponse> {
    // Extract and validate path parameters
    let (namespace, name, revision) = match path {
        Ok(Path((namespace, name, revision))) => (namespace, name, revision),
        Err(err) => {
            tracing::debug!(error = %err, error_source = logging::error_source(&err), "invalid path parameters");
            return Err(Error::InvalidPath(err).into());
        }
    };

    tracing::debug!(
        namespace=%namespace,
        name=%name,
        revision=%revision,
        "retrieving dataset status"
    );

    // Resolve revision to manifest hash
    let manifest_hash = ctx
        .dataset_store
        .resolve_dataset_revision(&namespace, &name, &revision)
        .await
        .map_err(Error::ResolveRevision)?
        .ok_or_else(|| Error::DatasetNotFound {
            namespace: namespace.clone(),
            name: name.clone(),
            revision: revision.clone(),
        })?;

    // Get all active physical tables for this manifest hash
    let physical_tables = metadata_db::physical_table::get_all_active_by_manifest_hash(
        &ctx.metadata_db,
        &manifest_hash,
    )
    .await
    .map_err(Error::MetadataDbError)?;

    if physical_tables.is_empty() {
        return Err(Error::NoActiveTables {
            namespace,
            name,
            revision,
        }
        .into());
    }

    tracing::debug!(
        num_tables = physical_tables.len(),
        "found active physical tables"
    );

    // Process each table to compute its status
    let mut table_statuses = Vec::with_capacity(physical_tables.len());

    for physical_table in physical_tables {
        let table_name = physical_table.table_name.to_string();

        tracing::debug!(
            table_name = %table_name,
            location_id = ?physical_table.id,
            "processing table"
        );

        // Stream file metadata and extract block ranges
        let mut segments = vec![];
        let mut file_stream = ctx
            .metadata_db
            .stream_files_by_location_id_with_details(physical_table.id);

        while let Some(file_result) = file_stream.next().await {
            let file_metadata = file_result.map_err(Error::MetadataDbError)?;

            // Parse parquet metadata
            let parquet_meta: ParquetMeta =
                serde_json::from_value(file_metadata.metadata).map_err(Error::ParseMetadata)?;

            // Extract block ranges (should be exactly 1 per file)
            for range in parquet_meta.ranges {
                segments.push(range);
            }
        }

        tracing::debug!(
            table_name = %table_name,
            num_segments = segments.len(),
            "extracted segments from file metadata"
        );

        // Compute statistics (handle empty case gracefully)
        let max_end_block = segments.iter().map(|s| s.end()).max();
        let min_start_block = segments.iter().map(|s| s.start()).min();
        let total_blocks: u64 = segments.iter().map(|s| s.end() - s.start() + 1).sum();

        table_statuses.push(TableStatus {
            table_name,
            max_end_block,
            min_start_block,
            total_blocks,
            num_files: segments.len(),
        });
    }

    Ok(Json(DatasetStatusResponse {
        tables: table_statuses,
    }))
}

/// Dataset status information with per-table metrics
///
/// Contains status for all active tables in the dataset.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct DatasetStatusResponse {
    /// Status for each table in the dataset, ordered alphabetically by table name
    pub tables: Vec<TableStatus>,
}

/// Status information for a single table
///
/// Contains metrics about block coverage for a table.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct TableStatus {
    /// Name of the table (e.g., "blocks", "transactions", "logs")
    pub table_name: String,

    /// Maximum end block across all segments
    ///
    /// Returns null if the table has no segments yet (valid for newly created tables).
    #[cfg_attr(feature = "utoipa", schema(value_type = Option<u64>))]
    pub max_end_block: Option<BlockNum>,

    /// Minimum start block across all segments
    ///
    /// Returns null if the table has no segments yet (valid for newly created tables).
    #[cfg_attr(feature = "utoipa", schema(value_type = Option<u64>))]
    pub min_start_block: Option<BlockNum>,

    /// Total number of blocks covered by all segments
    ///
    /// This sums the block count across all segments. Returns 0 if no segments exist.
    pub total_blocks: u64,

    /// Number of files/segments in this table
    ///
    /// Returns 0 if the table is empty (valid for newly created tables).
    pub num_files: usize,
}

/// Errors that can occur when getting dataset status
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid path parameters
    #[error("Invalid path parameters: {0}")]
    InvalidPath(#[source] PathRejection),

    /// Dataset or revision not found
    #[error("Dataset '{namespace}/{name}' at revision '{revision}' not found")]
    DatasetNotFound {
        namespace: Namespace,
        name: Name,
        revision: Revision,
    },

    /// No active tables found for dataset
    #[error("No active tables found for dataset '{namespace}/{name}' at revision '{revision}'")]
    NoActiveTables {
        namespace: Namespace,
        name: Name,
        revision: Revision,
    },

    /// Failed to resolve revision
    #[error("Failed to resolve revision: {0}")]
    ResolveRevision(#[source] dataset_store::ResolveRevisionError),

    /// Metadata database error
    #[error("Metadata database error: {0}")]
    MetadataDbError(#[source] metadata_db::Error),

    /// Failed to parse parquet metadata
    #[error("Failed to parse parquet metadata: {0}")]
    ParseMetadata(#[source] serde_json::Error),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidPath(_) => "INVALID_PATH",
            Error::DatasetNotFound { .. } => "DATASET_NOT_FOUND",
            Error::NoActiveTables { .. } => "NO_ACTIVE_TABLES",
            Error::ResolveRevision(_) => "RESOLVE_REVISION_ERROR",
            Error::MetadataDbError(_) => "METADATA_DB_ERROR",
            Error::ParseMetadata(_) => "PARSE_METADATA_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidPath(_) => StatusCode::BAD_REQUEST,
            Error::DatasetNotFound { .. } => StatusCode::NOT_FOUND,
            Error::NoActiveTables { .. } => StatusCode::NOT_FOUND,
            Error::ResolveRevision(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::MetadataDbError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::ParseMetadata(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
