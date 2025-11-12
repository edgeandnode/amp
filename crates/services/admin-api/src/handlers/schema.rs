use std::collections::BTreeMap;

use axum::{
    Json,
    extract::{State, rejection::JsonRejection},
    http::StatusCode,
};
use common::{
    BoxError,
    catalog::{
        errors::PlanningCtxForSqlTablesWithDepsError,
        sql::{
            FunctionReference, ResolveFunctionReferencesError,
            planning_ctx_for_sql_tables_with_deps, resolve_function_references,
        },
    },
    plan_visitors::prepend_special_block_num_field,
    query_context::Error as QueryContextError,
};
use datafusion::sql::{TableReference, parser::Statement, resolve::resolve_table_references};
use datasets_common::{fqn::FullyQualifiedName, hash::Hash, table_name::TableName};
use datasets_derived::{
    dep_alias::DepAlias,
    dep_reference::{DepReference, HashOrVersion},
    manifest::TableSchema,
    sql_str::SqlStr,
};
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
/// - `dependencies`: External dataset dependencies mapped by alias
/// - `tables`: Table definitions mapped by table name (optional if functions provided)
/// - `functions`: Function names defined in dataset config (optional if tables provided)
///
/// ## Response
/// - **200 OK**: Returns the inferred schema and networks referenced by the query
/// - **400 Bad Request**: Invalid SQL syntax, table references, or function format
/// - **404 Not Found**: Referenced dataset does not exist
/// - **500 Internal Server Error**: Dataset store, planning, or internal errors
///
/// ## Error Codes
/// - `INVALID_PAYLOAD_FORMAT`: Request JSON is malformed or missing required fields
/// - `EMPTY_TABLES_AND_FUNCTIONS`: No tables or functions provided (at least one is required)
/// - `INVALID_TABLE_SQL`: SQL syntax error in table definition
/// - `TABLE_REFERENCE_RESOLUTION`: Failed to extract table references from SQL
/// - `FUNCTION_REFERENCE_RESOLUTION`: Failed to extract function references from SQL
/// - `DEPENDENCY_NOT_FOUND`: Referenced dependency does not exist
/// - `DEPENDENCY_RESOLUTION`: Failed to resolve dependency
/// - `CATALOG_QUALIFIED_TABLE`: Table uses unsupported catalog qualification
/// - `UNQUALIFIED_TABLE`: Table missing required dataset qualification
/// - `INVALID_TABLE_NAME`: Table name violates SQL identifier rules
/// - `INVALID_DEPENDENCY_ALIAS_FOR_TABLE_REF`: Dependency alias in table reference is invalid
/// - `INVALID_DEPENDENCY_ALIAS_FOR_FUNCTION_REF`: Dependency alias in function reference is invalid
/// - `DEPENDENCY_ALIAS_NOT_FOUND`: Referenced alias not in dependencies
/// - `DATASET_NOT_FOUND`: Referenced dataset does not exist
/// - `GET_DATASET_ERROR`: Failed to retrieve dataset from store
/// - `ETH_CALL_UDF_CREATION_ERROR`: Failed to create eth_call UDF
/// - `TABLE_NOT_FOUND_IN_DATASET`: Table not found in referenced dataset
/// - `FUNCTION_NOT_FOUND_IN_DATASET`: Function not found in referenced dataset
/// - `ETH_CALL_NOT_AVAILABLE`: eth_call function not available for dataset
/// - `SCHEMA_INFERENCE`: Failed to infer output schema from query
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
        request_body = SchemaRequest,
        responses(
            (status = 200, description = "Successfully analyzed SQL query and returned schema", body = SchemaResponse),
            (status = 400, description = "Client error: Invalid SQL, table references, or function syntax", body = crate::handlers::error::ErrorResponse),
            (status = 404, description = "Dataset not found", body = crate::handlers::error::ErrorResponse),
            (status = 500, description = "Server error: Dataset store, planning, or internal failures", body = crate::handlers::error::ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    payload: Result<Json<SchemaRequest>, JsonRejection>,
) -> Result<Json<SchemaResponse>, ErrorResponse> {
    let SchemaRequest {
        tables,
        dependencies,
        functions,
    } = match payload {
        Ok(Json(request)) => request,
        Err(err) => {
            tracing::error!("Failed to parse request JSON: {}", err);
            return Err(Error::InvalidPayloadFormat { source: err }.into());
        }
    };

    // Check if at least one of tables or functions is provided
    if tables.is_empty() && functions.is_empty() {
        tracing::error!("No tables or functions provided in schema request");
        return Err(Error::EmptyTablesAndFunctions.into());
    }

    // Early return if only functions are provided (no SQL tables to validate)
    if tables.is_empty() {
        tracing::info!(
            "Only functions provided ({}), no SQL validation needed",
            functions.len()
        );
        // This would require:
        // 1. Extracting all function calls from SQL using all_function_names()
        // 2. Checking if the provided function names match what's used in SQL
        // 3. Warning or erroring if functions are defined but never used
        return Ok(Json(SchemaResponse {
            schemas: BTreeMap::new(),
        }));
    }

    // Resolve all dependencies to their manifest hashes
    // This must happen before parsing SQL to ensure all dependencies exist
    let dependencies = {
        let mut resolved: BTreeMap<DepAlias, (FullyQualifiedName, Hash)> = BTreeMap::new();
        for (alias, dep_ref) in dependencies {
            let (fqn, hash_or_version) = dep_ref.clone().into_fqn_and_hash_or_version();

            // Resolve the dependency to its manifest hash based on whether it's a hash or version
            let hash = match hash_or_version {
                HashOrVersion::Hash(hash) => {
                    // Verify the hash is linked to the dataset (namespace/name)
                    let is_linked = ctx
                        .dataset_store
                        .is_manifest_linked(fqn.namespace(), fqn.name(), &hash)
                        .await
                        .map_err(|err| Error::DependencyResolution {
                            alias: alias.clone(),
                            reference: dep_ref.clone(),
                            source: err.into(),
                        })?;

                    if !is_linked {
                        return Err(Error::DependencyNotFound {
                            alias,
                            reference: dep_ref,
                        }
                        .into());
                    }

                    hash
                }
                HashOrVersion::Version(version) => {
                    // Resolve version tag to hash for this specific dataset
                    ctx.dataset_store
                        .resolve_version_hash(fqn.namespace(), fqn.name(), &version)
                        .await
                        .map_err(|err| Error::DependencyResolution {
                            alias: alias.clone(),
                            reference: dep_ref.clone(),
                            source: err.into(),
                        })?
                        .ok_or_else(|| Error::DependencyNotFound {
                            alias: alias.clone(),
                            reference: dep_ref.clone(),
                        })?
                }
            };

            resolved.insert(alias, (fqn, hash));
        }
        resolved
    };

    // Parse all SQL queries from tables and extract table references and function names
    let (statements, references) = {
        let mut statements: BTreeMap<TableName, Statement> = BTreeMap::new();
        let mut references: BTreeMap<TableName, (Vec<TableReference>, Vec<FunctionReference>)> =
            BTreeMap::new();

        for (table_name, sql_query) in tables {
            let stmt = common::sql::parse(&sql_query).map_err(|err| Error::InvalidTableSql {
                table_name: table_name.clone(),
                source: err,
            })?;

            // Extract table references from the statement
            let (table_refs, _) = resolve_table_references(&stmt, true).map_err(|err| {
                Error::TableReferenceResolution {
                    table_name: table_name.clone(),
                    source: err.into(),
                }
            })?;

            // Extract function references from the statement
            let func_refs = resolve_function_references(&stmt).map_err(|err| {
                Error::FunctionReferenceResolution {
                    table_name: table_name.clone(),
                    source: err,
                }
            })?;

            statements.insert(table_name.clone(), stmt);
            references.insert(table_name, (table_refs, func_refs));
        }

        (statements, references)
    };

    // Create planning context using resolved dependencies
    // TODO: Verify that functions defined in config are actually used in table SQL statements
    let planning_ctx =
        planning_ctx_for_sql_tables_with_deps(ctx.dataset_store.as_ref(), references, dependencies)
            .await
            .map_err(|err| match &err {
                PlanningCtxForSqlTablesWithDepsError::CatalogQualifiedTable { .. } => {
                    Error::CatalogQualifiedTable(err)
                }
                PlanningCtxForSqlTablesWithDepsError::UnqualifiedTable { .. } => {
                    Error::UnqualifiedTable(err)
                }
                PlanningCtxForSqlTablesWithDepsError::InvalidTableName { .. } => {
                    Error::InvalidTableName(err)
                }
                PlanningCtxForSqlTablesWithDepsError::InvalidDependencyAliasForTableRef {
                    ..
                } => Error::InvalidDependencyAliasForTableRef(err),
                PlanningCtxForSqlTablesWithDepsError::InvalidDependencyAliasForFunctionRef {
                    ..
                } => Error::InvalidDependencyAliasForFunctionRef(err),
                PlanningCtxForSqlTablesWithDepsError::DatasetNotFoundForTableRef { .. } => {
                    Error::DatasetNotFound(err)
                }
                PlanningCtxForSqlTablesWithDepsError::DatasetNotFoundForFunction { .. } => {
                    Error::DatasetNotFound(err)
                }
                PlanningCtxForSqlTablesWithDepsError::GetDatasetForTableRef { .. } => {
                    Error::GetDataset(err)
                }
                PlanningCtxForSqlTablesWithDepsError::GetDatasetForFunction { .. } => {
                    Error::GetDataset(err)
                }
                PlanningCtxForSqlTablesWithDepsError::EthCallUdfCreationForFunction { .. } => {
                    Error::EthCallUdfCreation(err)
                }
                PlanningCtxForSqlTablesWithDepsError::DependencyAliasNotFoundForTableRef {
                    ..
                } => Error::DependencyAliasNotFound(err),
                PlanningCtxForSqlTablesWithDepsError::DependencyAliasNotFoundForFunctionRef {
                    ..
                } => Error::DependencyAliasNotFound(err),
                PlanningCtxForSqlTablesWithDepsError::TableNotFoundInDataset { .. } => {
                    Error::TableNotFoundInDataset(err)
                }
                PlanningCtxForSqlTablesWithDepsError::FunctionNotFoundInDataset { .. } => {
                    Error::FunctionNotFoundInDataset(err)
                }
                PlanningCtxForSqlTablesWithDepsError::EthCallNotAvailable { .. } => {
                    Error::EthCallNotAvailable(err)
                }
            })?;

    // Infer schema for each table and extract networks
    let mut schemas = BTreeMap::new();
    for (table_name, stmt) in statements {
        // Infer schema using the planning context
        let schema =
            planning_ctx
                .sql_output_schema(stmt)
                .await
                .map_err(|err| Error::SchemaInference {
                    table_name: table_name.clone(),
                    source: err,
                })?;

        // Prepend the special block number field
        let schema = prepend_special_block_num_field(&schema);

        // Extract networks from all tables in the catalog
        let mut networks: Vec<String> = planning_ctx
            .catalog()
            .iter()
            .map(|t| t.table().network().to_string())
            .collect();
        networks.sort();
        networks.dedup();

        schemas.insert(
            table_name,
            TableSchemaWithNetworks {
                schema: schema.into(),
                networks,
            },
        );
    }

    Ok(Json(SchemaResponse { schemas }))
}

/// Request payload for schema analysis
///
/// Contains dependencies, table definitions, and function names for schema analysis.
#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(
    feature = "utoipa",
    schema(description = "Request for schema analysis with dependencies, tables, and functions")
)]
pub struct SchemaRequest {
    /// Table definitions mapped by table name
    ///
    /// Each table is defined by a SQL query that may reference
    /// tables from dependencies using the alias names.
    #[serde(default)]
    #[cfg_attr(feature = "utoipa", schema(value_type = std::collections::BTreeMap<String, String>))]
    pub tables: BTreeMap<TableName, SqlStr>,

    /// External dataset dependencies mapped by alias
    ///
    /// Maps alias names to dataset references (namespace/name@version or namespace/name@hash).
    /// These aliases are used in SQL queries to reference external datasets.
    /// Symbolic references like "latest" or "dev" are not allowed.
    #[serde(default)]
    #[cfg_attr(feature = "utoipa", schema(value_type = std::collections::BTreeMap<String, String>))]
    pub dependencies: BTreeMap<DepAlias, DepReference>,

    /// Function names defined in the dataset configuration
    ///
    /// Used to validate that functions are properly defined and available.
    /// At least one of `tables` or `functions` must be provided.
    #[serde(default)]
    #[cfg_attr(feature = "utoipa", schema(value_type = Vec<String>))]
    pub functions: Vec<NonEmptyString>,
}

/// Response returned by the schema endpoint
///
/// Contains schemas and networks for one or more tables.
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct SchemaResponse {
    /// Schemas for each table
    ///
    /// Maps table names to their schemas and networks.
    /// Contains one entry per table definition.
    #[cfg_attr(feature = "utoipa", schema(value_type = std::collections::BTreeMap<String, TableSchemaWithNetworks>))]
    schemas: BTreeMap<TableName, TableSchemaWithNetworks>,
}

/// Errors that can occur during schema operations
#[derive(Debug, thiserror::Error)]
enum Error {
    /// Invalid request payload format
    ///
    /// This occurs when:
    /// - Request JSON is malformed or missing required fields
    /// - JSON deserialization fails
    /// - Request body cannot be parsed
    #[error("Invalid payload format: {source}")]
    InvalidPayloadFormat {
        /// The rejection details from Axum's JSON extractor
        source: JsonRejection,
    },

    /// No tables or functions provided in request
    ///
    /// This occurs when both the `tables` and `functions` fields are empty.
    /// At least one table or function must be provided for schema analysis.
    #[error("At least one table or function must be provided")]
    EmptyTablesAndFunctions,

    /// Invalid SQL query in table definition
    ///
    /// This occurs when:
    /// - The provided SQL query has invalid syntax
    /// - Unsupported SQL features are used
    /// - Query parsing fails for other reasons
    #[error("Invalid SQL query for table '{table_name}': {source}")]
    InvalidTableSql {
        /// The table name that contains the invalid SQL
        table_name: TableName,
        /// The underlying parse error
        #[source]
        source: common::sql::ParseSqlError,
    },

    /// Failed to resolve table references in SQL query
    ///
    /// This occurs when:
    /// - Table references cannot be extracted from the parsed SQL statement
    /// - Invalid table reference format is encountered
    #[error("Failed to resolve table references for table '{table_name}': {source}")]
    TableReferenceResolution {
        /// The table name that contains the invalid references
        table_name: TableName,
        /// The underlying resolution error
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Failed to resolve function references from SQL query
    ///
    /// This occurs when:
    /// - Function references cannot be extracted from the parsed SQL statement
    /// - Unsupported DML statements are encountered
    #[error("Failed to resolve function references for table '{table_name}': {source}")]
    FunctionReferenceResolution {
        /// The table name that contains the invalid functions
        table_name: TableName,
        /// The underlying extraction error
        #[source]
        source: ResolveFunctionReferencesError,
    },

    /// Dependency not found in dataset store
    ///
    /// This occurs when:
    /// - A referenced dependency does not exist in the dataset store
    /// - The specified version or hash cannot be found
    #[error("Dependency '{alias}' ({reference}) not found in dataset store")]
    DependencyNotFound {
        /// The alias name used in the request
        alias: DepAlias,
        /// The dependency reference (namespace/name@version or namespace/name@hash)
        reference: DepReference,
    },

    /// Failed to resolve dependency
    ///
    /// This occurs when:
    /// - Dependency resolution encounters an error
    /// - Database query fails during resolution
    #[error("Failed to resolve dependency '{alias}' ({reference}): {source}")]
    DependencyResolution {
        /// The alias name used in the request
        alias: DepAlias,
        /// The dependency reference
        reference: DepReference,
        /// The underlying resolution error
        #[source]
        source: BoxError,
    },

    /// Catalog-qualified table reference not supported
    ///
    /// Only dataset-qualified tables are supported (e.g., `dataset.table`).
    /// Catalog-qualified tables (e.g., `catalog.schema.table`) are not supported.
    #[error(transparent)]
    CatalogQualifiedTable(PlanningCtxForSqlTablesWithDepsError),

    /// Unqualified table reference
    ///
    /// All tables must be qualified with a dataset reference in the schema portion.
    /// Unqualified tables (e.g., just `table_name`) are not allowed.
    #[error(transparent)]
    UnqualifiedTable(PlanningCtxForSqlTablesWithDepsError),

    /// Invalid table name
    ///
    /// Table name does not conform to SQL identifier rules (must start with letter/underscore,
    /// contain only alphanumeric/underscore/dollar, and be <= 63 bytes).
    #[error(transparent)]
    InvalidTableName(PlanningCtxForSqlTablesWithDepsError),

    /// Dataset reference not found
    ///
    /// The referenced dataset does not exist in the store.
    #[error(transparent)]
    DatasetNotFound(PlanningCtxForSqlTablesWithDepsError),

    /// Failed to retrieve dataset from store
    ///
    /// This occurs when loading a dataset definition fails due to:
    /// - Invalid or corrupted manifest
    /// - Unsupported dataset kind
    /// - Storage backend errors
    #[error(transparent)]
    GetDataset(PlanningCtxForSqlTablesWithDepsError),

    /// Failed to create ETH call UDF
    ///
    /// This occurs when creating the eth_call user-defined function fails.
    #[error(transparent)]
    EthCallUdfCreation(PlanningCtxForSqlTablesWithDepsError),

    /// Table not found in dataset
    ///
    /// The referenced table does not exist in the dataset.
    #[error(transparent)]
    TableNotFoundInDataset(PlanningCtxForSqlTablesWithDepsError),

    /// Function not found in dataset
    ///
    /// The referenced function does not exist in the dataset.
    #[error(transparent)]
    FunctionNotFoundInDataset(PlanningCtxForSqlTablesWithDepsError),

    /// eth_call function not available
    ///
    /// The eth_call function is not available for the referenced dataset.
    #[error(transparent)]
    EthCallNotAvailable(PlanningCtxForSqlTablesWithDepsError),

    /// Invalid dependency alias in table reference
    ///
    /// The dependency alias in a table reference does not conform to alias rules
    /// (must start with letter, contain only alphanumeric/underscore, and be <= 63 bytes).
    #[error(transparent)]
    InvalidDependencyAliasForTableRef(PlanningCtxForSqlTablesWithDepsError),

    /// Invalid dependency alias in function reference
    ///
    /// The dependency alias in a function reference does not conform to alias rules
    /// (must start with letter, contain only alphanumeric/underscore, and be <= 63 bytes).
    #[error(transparent)]
    InvalidDependencyAliasForFunctionRef(PlanningCtxForSqlTablesWithDepsError),

    /// Dependency alias not found
    ///
    /// A table or function reference uses an alias that was not provided in the dependencies map.
    #[error(transparent)]
    DependencyAliasNotFound(PlanningCtxForSqlTablesWithDepsError),

    /// Failed to infer schema for table
    ///
    /// This occurs when:
    /// - Query planning fails due to invalid references
    /// - Type inference fails for the query
    /// - Schema determination encounters errors
    #[error("Failed to infer schema for table '{table_name}': {source}")]
    SchemaInference {
        /// The table name that failed schema inference
        table_name: TableName,
        /// The underlying query context error
        #[source]
        source: QueryContextError,
    },
}

/// Table schema with associated networks
///
/// Contains the output schema for a table and the list of networks referenced by its query.
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct TableSchemaWithNetworks {
    /// The output schema for the table
    ///
    /// Describes the structure and types of columns that will be returned
    /// when executing the SQL query for this table.
    #[cfg_attr(feature = "utoipa", schema(value_type = serde_json::Value))]
    schema: TableSchema,
    /// List of networks referenced by this table's query
    ///
    /// Contains the network names of all datasets/tables referenced
    /// in this specific table's SQL query (e.g., "mainnet", "polygon", etc.).
    networks: Vec<String>,
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidPayloadFormat { .. } => "INVALID_PAYLOAD_FORMAT",
            Error::EmptyTablesAndFunctions => "EMPTY_TABLES_AND_FUNCTIONS",
            Error::InvalidTableSql { .. } => "INVALID_TABLE_SQL",
            Error::TableReferenceResolution { .. } => "TABLE_REFERENCE_RESOLUTION",
            Error::FunctionReferenceResolution { .. } => "FUNCTION_REFERENCE_RESOLUTION",
            Error::DependencyNotFound { .. } => "DEPENDENCY_NOT_FOUND",
            Error::DependencyResolution { .. } => "DEPENDENCY_RESOLUTION",
            Error::CatalogQualifiedTable(_) => "CATALOG_QUALIFIED_TABLE",
            Error::UnqualifiedTable(_) => "UNQUALIFIED_TABLE",
            Error::InvalidTableName(_) => "INVALID_TABLE_NAME",
            Error::InvalidDependencyAliasForTableRef(_) => "INVALID_DEPENDENCY_ALIAS_FOR_TABLE_REF",
            Error::InvalidDependencyAliasForFunctionRef(_) => {
                "INVALID_DEPENDENCY_ALIAS_FOR_FUNCTION_REF"
            }
            Error::DatasetNotFound(_) => "DATASET_NOT_FOUND",
            Error::GetDataset(_) => "GET_DATASET_ERROR",
            Error::EthCallUdfCreation(_) => "ETH_CALL_UDF_CREATION_ERROR",
            Error::TableNotFoundInDataset(_) => "TABLE_NOT_FOUND_IN_DATASET",
            Error::FunctionNotFoundInDataset(_) => "FUNCTION_NOT_FOUND_IN_DATASET",
            Error::EthCallNotAvailable(_) => "ETH_CALL_NOT_AVAILABLE",
            Error::DependencyAliasNotFound(_) => "DEPENDENCY_ALIAS_NOT_FOUND",
            Error::SchemaInference { .. } => "SCHEMA_INFERENCE",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidPayloadFormat { .. } => StatusCode::BAD_REQUEST,
            Error::EmptyTablesAndFunctions => StatusCode::BAD_REQUEST,
            Error::InvalidTableSql { .. } => StatusCode::BAD_REQUEST,
            Error::TableReferenceResolution { .. } => StatusCode::BAD_REQUEST,
            Error::FunctionReferenceResolution { .. } => StatusCode::BAD_REQUEST,
            Error::DependencyNotFound { .. } => StatusCode::NOT_FOUND,
            Error::DependencyResolution { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Error::CatalogQualifiedTable(_) => StatusCode::BAD_REQUEST,
            Error::UnqualifiedTable(_) => StatusCode::BAD_REQUEST,
            Error::InvalidTableName(_) => StatusCode::BAD_REQUEST,
            Error::InvalidDependencyAliasForTableRef(_) => StatusCode::BAD_REQUEST,
            Error::InvalidDependencyAliasForFunctionRef(_) => StatusCode::BAD_REQUEST,
            Error::DatasetNotFound(_) => StatusCode::NOT_FOUND,
            Error::GetDataset(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::EthCallUdfCreation(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::TableNotFoundInDataset(_) => StatusCode::NOT_FOUND,
            Error::FunctionNotFoundInDataset(_) => StatusCode::NOT_FOUND,
            Error::EthCallNotAvailable(_) => StatusCode::NOT_FOUND,
            Error::DependencyAliasNotFound(_) => StatusCode::BAD_REQUEST,
            Error::SchemaInference { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
