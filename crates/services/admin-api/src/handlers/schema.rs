use axum::{
    Json,
    extract::{State, rejection::JsonRejection},
    http::StatusCode,
};
use common::{
    catalog::{errors::PlanningCtxForSqlError, sql::planning_ctx_for_sql},
    plan_visitors::prepend_special_block_num_field,
    query_context::{Error as QueryContextError, parse_sql},
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

/// Handler for the `POST /schema` endpoint
///
/// Analyzes SQL queries and returns the output schema without executing the query.
/// Performs comprehensive validation and schema inference using real registered datasets
/// and their actual schemas.
///
/// ## Request Body
/// - `sql_query`: The SQL query to analyze
///
/// ## Response
/// - **200 OK**: Returns the inferred schema and networks referenced by the query
/// - **400 Bad Request**: Invalid SQL syntax, table references, or function format
/// - **404 Not Found**: Referenced dataset does not exist
/// - **500 Internal Server Error**: Dataset store, planning, or internal errors
///
/// ## Error Codes
/// - `INVALID_PAYLOAD_FORMAT`: Request JSON is malformed or missing required fields
/// - `SQL_PARSE_ERROR`: SQL query has invalid syntax or uses unsupported features
/// - `UNQUALIFIED_TABLE`: Table reference missing dataset qualifier (must use `dataset.table`)
/// - `CATALOG_QUALIFIED_TABLE`: Catalog-qualified table not supported (use `dataset.table` not `catalog.schema.table`)
/// - `INVALID_TABLE_NAME`: Table name does not conform to SQL identifier rules
/// - `INVALID_SCHEMA_REFERENCE`: Dataset reference format is invalid (expected `namespace/name@version`)
/// - `INVALID_FUNCTION_REFERENCE`: Function's dataset qualifier cannot be parsed as valid dataset reference
/// - `INVALID_FUNCTION_FORMAT`: Function must be unqualified or dataset-qualified (expected `dataset.function` or `function`)
/// - `DATASET_NOT_FOUND`: Referenced dataset does not exist in the store
/// - `TABLE_NOT_FOUND_IN_DATASET`: Referenced table does not exist in the dataset
/// - `TABLE_REFERENCE_RESOLUTION_ERROR`: Failed to resolve table references from SQL statement
/// - `FUNCTION_NAME_EXTRACTION_ERROR`: Failed to extract function names from SQL AST
/// - `RESOLVE_HASH_ERROR`: Failed to resolve dataset reference to content hash
/// - `GET_DATASET_ERROR`: Failed to retrieve dataset from store (manifest corrupted, unsupported kind, or storage error)
/// - `ETH_CALL_UDF_CREATION_ERROR`: Failed to create eth_call UDF (invalid provider config or connection issue)
/// - `PLANNING_ERROR`: Query planning failed (type inference, schema determination, or invalid references)
///
/// ## Schema Analysis Process
/// 1. **Parse SQL**: Validates syntax using DataFusion's SQL parser
/// 2. **Load Datasets**: Retrieves dataset definitions from the registry for all referenced datasets
/// 3. **Create Planning Context**: Builds planning context with real table schemas from stored datasets
/// 4. **Infer Schema**: Uses DataFusion's query planner to determine output schema without executing the query
/// 5. **Prepend Special Fields**: Adds `SPECIAL_BLOCK_NUM` field to the output schema
/// 6. **Extract Networks**: Identifies which blockchain networks are referenced by the query
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
            (status = 400, description = "Client error: Invalid SQL, table references, or function syntax", body = crate::handlers::error::ErrorResponse),
            (status = 404, description = "Dataset not found", body = crate::handlers::error::ErrorResponse),
            (status = 500, description = "Server error: Dataset store, planning, or internal failures", body = crate::handlers::error::ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    payload: Result<Json<OutputSchemaRequest>, JsonRejection>,
) -> Result<Json<OutputSchemaResponse>, ErrorResponse> {
    let OutputSchemaRequest { sql_query } = match payload {
        Ok(Json(payload)) => payload,
        Err(err) => {
            tracing::error!("Failed to parse request JSON: {}", err);
            return Err(Error::InvalidPayloadFormat.into());
        }
    };

    let stmt = parse_sql(sql_query.as_str()).map_err(Error::SqlParse)?;

    let query_ctx = planning_ctx_for_sql(ctx.dataset_store.as_ref(), &stmt)
        .await
        .map_err(|err| match &err {
            PlanningCtxForSqlError::TableReferenceResolution(_) => {
                Error::TableReferenceResolution(err)
            }
            PlanningCtxForSqlError::FunctionNameExtraction(_) => Error::FunctionNameExtraction(err),
            PlanningCtxForSqlError::CatalogQualifiedTable { .. } => {
                Error::CatalogQualifiedTable(err)
            }
            PlanningCtxForSqlError::UnqualifiedTable { .. } => Error::UnqualifiedTable(err),
            PlanningCtxForSqlError::InvalidTableName { .. } => Error::InvalidTableName(err),
            PlanningCtxForSqlError::InvalidSchemaReference { .. } => {
                Error::InvalidSchemaReference(err)
            }
            PlanningCtxForSqlError::ResolveHash { .. } => Error::ResolveHash(err),
            PlanningCtxForSqlError::DatasetNotFound { .. } => Error::DatasetNotFound(err),
            PlanningCtxForSqlError::TableNotFoundInDataset { .. } => {
                Error::TableNotFoundInDataset(err)
            }
            PlanningCtxForSqlError::GetDataset { .. } => Error::GetDataset(err),
            PlanningCtxForSqlError::EthCallUdfCreation { .. } => Error::EthCallUdfCreation(err),
            PlanningCtxForSqlError::InvalidFunctionReference { .. } => {
                Error::InvalidFunctionReference(err)
            }
            PlanningCtxForSqlError::InvalidFunctionFormat { .. } => {
                Error::InvalidFunctionFormat(err)
            }
        })?;

    let schema = query_ctx
        .sql_output_schema(stmt)
        .await
        .map_err(Error::Planning)?;

    // Always prepend the `SPECIAL_BLOCK_NUM` field to the schema.
    let schema = prepend_special_block_num_field(&schema);

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
/// Contains the SQL query to analyze.
#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct OutputSchemaRequest {
    /// The SQL query to analyze for output schema determination
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    sql_query: NonEmptyString,
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
    /// Invalid request payload format
    ///
    /// This occurs when:
    /// - Request JSON is malformed or missing required fields
    /// - JSON deserialization fails
    /// - Request body cannot be parsed
    #[error("Invalid payload format")]
    InvalidPayloadFormat,

    /// SQL parse error
    ///
    /// This occurs when:
    /// - The provided SQL query has invalid syntax
    /// - Unsupported SQL features are used
    /// - Query parsing fails for other reasons
    #[error("SQL parse error: {0}")]
    SqlParse(#[source] QueryContextError),

    /// Unqualified table reference
    ///
    /// All tables must be qualified with a dataset reference.
    #[error(transparent)]
    UnqualifiedTable(PlanningCtxForSqlError),

    /// Catalog-qualified table reference
    ///
    /// Only dataset-qualified tables are supported.
    #[error(transparent)]
    CatalogQualifiedTable(PlanningCtxForSqlError),

    /// Invalid table name
    ///
    /// Table name does not conform to SQL identifier rules.
    #[error(transparent)]
    InvalidTableName(PlanningCtxForSqlError),

    /// Invalid schema reference
    ///
    /// Schema portion of table reference is not a valid dataset reference.
    #[error(transparent)]
    InvalidSchemaReference(PlanningCtxForSqlError),

    /// Dataset not found
    ///
    /// The referenced dataset does not exist in the store.
    #[error(transparent)]
    DatasetNotFound(PlanningCtxForSqlError),

    /// Table not found in dataset
    ///
    /// The referenced table does not exist in the dataset.
    #[error(transparent)]
    TableNotFoundInDataset(PlanningCtxForSqlError),

    /// Failed to resolve table references from SQL
    ///
    /// This occurs when DataFusion's table reference resolver encounters issues
    /// parsing or extracting table names from the SQL statement.
    #[error(transparent)]
    TableReferenceResolution(PlanningCtxForSqlError),

    /// Failed to extract function names from SQL
    ///
    /// This occurs when analyzing the SQL AST to find function calls fails.
    #[error(transparent)]
    FunctionNameExtraction(PlanningCtxForSqlError),

    /// Failed to resolve dataset reference to hash
    ///
    /// This occurs when the dataset store cannot resolve a reference to its
    /// corresponding content hash.
    #[error(transparent)]
    ResolveHash(PlanningCtxForSqlError),

    /// Failed to retrieve dataset from store
    ///
    /// This occurs when loading a dataset definition fails due to:
    /// - Invalid or corrupted manifest
    /// - Unsupported dataset kind
    /// - Storage backend errors
    #[error(transparent)]
    GetDataset(PlanningCtxForSqlError),

    /// Failed to create ETH call UDF
    ///
    /// This occurs when creating the eth_call user-defined function fails.
    #[error(transparent)]
    EthCallUdfCreation(PlanningCtxForSqlError),

    /// Invalid function reference
    ///
    /// Function's dataset qualifier cannot be parsed as a valid dataset reference.
    #[error(transparent)]
    InvalidFunctionReference(PlanningCtxForSqlError),

    /// Invalid function format
    ///
    /// Function names must be either unqualified or dataset-qualified.
    #[error(transparent)]
    InvalidFunctionFormat(PlanningCtxForSqlError),

    /// Planning error while determining output schema
    ///
    /// This occurs when:
    /// - Query planning fails due to invalid references
    /// - Type inference fails for the query
    /// - Schema determination encounters errors
    #[error("Planning error: {0}")]
    Planning(QueryContextError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidPayloadFormat => "INVALID_PAYLOAD_FORMAT",
            Error::SqlParse(_) => "SQL_PARSE_ERROR",
            Error::UnqualifiedTable(_) => "UNQUALIFIED_TABLE",
            Error::CatalogQualifiedTable(_) => "CATALOG_QUALIFIED_TABLE",
            Error::InvalidTableName(_) => "INVALID_TABLE_NAME",
            Error::InvalidSchemaReference(_) => "INVALID_SCHEMA_REFERENCE",
            Error::DatasetNotFound(_) => "DATASET_NOT_FOUND",
            Error::TableNotFoundInDataset(_) => "TABLE_NOT_FOUND_IN_DATASET",
            Error::TableReferenceResolution(_) => "TABLE_REFERENCE_RESOLUTION_ERROR",
            Error::FunctionNameExtraction(_) => "FUNCTION_NAME_EXTRACTION_ERROR",
            Error::ResolveHash(_) => "RESOLVE_HASH_ERROR",
            Error::GetDataset(_) => "GET_DATASET_ERROR",
            Error::EthCallUdfCreation(_) => "ETH_CALL_UDF_CREATION_ERROR",
            Error::InvalidFunctionReference(_) => "INVALID_FUNCTION_REFERENCE",
            Error::InvalidFunctionFormat(_) => "INVALID_FUNCTION_FORMAT",
            Error::Planning(_) => "PLANNING_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidPayloadFormat => StatusCode::BAD_REQUEST,
            Error::SqlParse(_) => StatusCode::BAD_REQUEST,
            Error::UnqualifiedTable(_) => StatusCode::BAD_REQUEST,
            Error::CatalogQualifiedTable(_) => StatusCode::BAD_REQUEST,
            Error::InvalidTableName(_) => StatusCode::BAD_REQUEST,
            Error::InvalidSchemaReference(_) => StatusCode::BAD_REQUEST,
            Error::InvalidFunctionReference(_) => StatusCode::BAD_REQUEST,
            Error::InvalidFunctionFormat(_) => StatusCode::BAD_REQUEST,
            Error::DatasetNotFound(_) => StatusCode::NOT_FOUND,
            Error::TableNotFoundInDataset(_) => StatusCode::NOT_FOUND,
            Error::TableReferenceResolution(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::FunctionNameExtraction(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::ResolveHash(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::GetDataset(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::EthCallUdfCreation(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::Planning(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
