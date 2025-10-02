use axum::{Json, extract::State, http::StatusCode};
use common::query_context::{
    Error as QueryContextError, parse_sql, prepend_special_block_num_field,
};
use datasets_derived::manifest::TableSchema;
use tracing::instrument;

use crate::{
    ctx::Ctx,
    handlers::{
        common::NonEmptyString,
        error::{ErrorResponse, IntoErrorResponse},
    },
};

/// Handler for the `/schema` endpoint that provides SQL schema analysis.
///
/// This endpoint performs comprehensive SQL validation and schema inference by:
/// 1. **Parsing SQL**: Validates syntax using DataFusion's SQL parser
/// 2. **Loading Datasets**: Retrieves actual dataset definitions from the registry
/// 3. **Schema Resolution**: Creates planning context with real table schemas from stored datasets
/// 4. **Schema Inference**: Uses DataFusion's query planner to determine output schema without execution
/// 5. **Special Fields**: Optionally prepends `SPECIAL_BLOCK_NUM` field for SQL datasets
/// 6. **Network Extraction**: Identifies which blockchain networks the query references
///
/// The validation works with real registered datasets and their actual schemas,
/// ensuring datasets exist, tables are valid, and column references are correct.
/// This enables accurate schema introspection for query builders and dataset development tools.
///
/// ## Request Body
/// - `sql_query`: The SQL query to analyze
/// - `is_sql_dataset`: (optional) Whether this is a SQL dataset (affects block number field inclusion)
///
/// ## Response
/// - **200 OK**: Returns the schema and networks used by the query
/// - **400 Bad Request**: SQL parse error
/// - **500 Internal Server Error**: Dataset store or planning error
///
/// ## Error Codes
/// - `SQL_PARSE_ERROR`: Failed to parse the SQL query
/// - `DATASET_STORE_ERROR`: Failed to load datasets from store
/// - `PLANNING_ERROR`: Failed to determine output schema
#[instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        post,
        path = "/schema",
        tag = "schema",
        operation_id = "schema_analyze",
        request_body = OutputSchemaRequest,
        responses(
            (status = 200, description = "Successfully analyzed SQL query and returned schema", body = OutputSchemaResponse),
            (status = 400, description = "SQL parse error"),
            (status = 500, description = "Dataset store or planning error")
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    Json(OutputSchemaRequest {
        sql_query,
        is_sql_dataset,
    }): Json<OutputSchemaRequest>,
) -> Result<Json<OutputSchemaResponse>, ErrorResponse> {
    let stmt = parse_sql(sql_query.as_str()).map_err(Error::SqlParseError)?;

    let query_ctx = ctx
        .dataset_store
        .planning_ctx_for_sql(&stmt)
        .await
        .map_err(Error::DatasetStoreError)?;

    let schema = query_ctx
        .sql_output_schema(stmt)
        .await
        .map_err(Error::PlanningError)?;

    let schema = if is_sql_dataset {
        // For SQL datasets, the `SPECIAL_BLOCK_NUM` field is always included in the schema.
        prepend_special_block_num_field(&schema)
    } else {
        schema
    };

    let mut networks: Vec<String> = query_ctx
        .catalog()
        .iter()
        .map(|t| t.table().network().to_string())
        .collect();
    networks.sort();
    networks.dedup();

    Ok(Json(OutputSchemaResponse {
        schema: schema.into(),
        networks,
    }))
}

/// Request payload for output schema analysis
///
/// Contains the SQL query to analyze and optional configuration flags.
#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct OutputSchemaRequest {
    /// The SQL query to analyze for output schema determination
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    sql_query: NonEmptyString,
    /// Whether this is a SQL dataset (affects block number field inclusion)
    ///
    /// When true, a special block number field is prepended to the schema.
    /// This field tracks the block number for each row in SQL datasets.
    #[serde(default)]
    is_sql_dataset: bool,
}

/// Response returned by the output schema endpoint
///
/// Contains the determined schema and list of networks referenced by the query.
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct OutputSchemaResponse {
    /// The output schema for the SQL query
    ///
    /// Describes the structure and types of columns that will be returned
    /// when executing the provided SQL query against the dataset.
    #[cfg_attr(feature = "utoipa", schema(value_type = serde_json::Value))]
    schema: TableSchema,
    /// List of networks referenced by the query
    ///
    /// Contains the network names of all datasets/tables referenced
    /// in the SQL query (e.g., "mainnet", "polygon", etc.).
    networks: Vec<String>,
}

/// Errors that can occur during output schema operations
#[derive(Debug, thiserror::Error)]
enum Error {
    /// SQL parse error
    ///
    /// This occurs when:
    /// - The provided SQL query has invalid syntax
    /// - Unsupported SQL features are used
    /// - Query parsing fails for other reasons
    #[error("SQL parse error: {0}")]
    SqlParseError(QueryContextError),
    /// Dataset store error while loading datasets
    ///
    /// This occurs when:
    /// - The dataset store is not accessible
    /// - There's a configuration error in the store
    /// - I/O errors while reading dataset definitions
    #[error("Dataset store error: {0}")]
    DatasetStoreError(#[from] dataset_store::PlanningCtxForSqlError),
    /// Planning error while determining output schema
    ///
    /// This occurs when:
    /// - Query planning fails due to invalid references
    /// - Type inference fails for the query
    /// - Schema determination encounters errors
    #[error("Planning error: {0}")]
    PlanningError(QueryContextError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::SqlParseError(_) => "SQL_PARSE_ERROR",
            Error::DatasetStoreError(_) => "DATASET_STORE_ERROR",
            Error::PlanningError(_) => "PLANNING_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::SqlParseError(_) => StatusCode::BAD_REQUEST,
            Error::DatasetStoreError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::PlanningError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
