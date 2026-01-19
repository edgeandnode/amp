use std::collections::BTreeMap;

use amp_dataset_store::GetDatasetError;
use axum::{
    Json,
    extract::{State, rejection::JsonRejection},
    http::StatusCode,
};
use common::{
    BoxError,
    catalog::logical::for_admin_api::{
        self as catalog, CreateLogicalCatalogError, ResolveTablesError, ResolveUdfsError,
        TableReferencesMap,
    },
    plan_visitors::prepend_special_block_num_field,
    planning_context::PlanningContext,
    query_context::Error as QueryContextError,
    sql::{
        ResolveFunctionReferencesError, ResolveTableReferencesError, resolve_function_references,
        resolve_table_references,
    },
    sql_str::SqlStr,
};
use datafusion::sql::parser::Statement;
use datasets_common::{
    deps::{
        alias::{DepAlias, DepAliasOrSelfRef},
        reference::{DepReference, HashOrVersion},
    },
    func_name::FuncName,
    hash_reference::HashReference,
    table_name::TableName,
};
use datasets_derived::manifest::{Function, TableSchema};
use js_runtime::isolate_pool::IsolatePool;
use tracing::instrument;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
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
/// - `CATALOG_QUALIFIED_FUNCTION`: Function uses unsupported catalog qualification
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
        let mut resolved: BTreeMap<DepAlias, HashReference> = BTreeMap::new();
        for (alias, dep_ref) in dependencies {
            let (fqn, hash_or_version) = dep_ref.clone().into_fqn_and_hash_or_version();

            // Resolve the dependency to its manifest hash based on whether it's a hash or version
            let hash = match hash_or_version {
                HashOrVersion::Hash(hash) => {
                    // Verify the hash is linked to the dataset (namespace/name)
                    let is_linked = ctx
                        .datasets_registry
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
                    ctx.datasets_registry
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

            resolved.insert(alias, (fqn, hash).into());
        }
        resolved
    };

    // Parse all SQL queries from tables and extract table references and function names
    let (statements, references) = {
        let mut statements: BTreeMap<TableName, Statement> = BTreeMap::new();
        let mut references = TableReferencesMap::new();

        for (table_name, sql_query) in tables {
            let stmt = common::sql::parse(&sql_query).map_err(|err| Error::InvalidTableSql {
                table_name: table_name.clone(),
                source: err,
            })?;

            // Extract table references from the statement
            let table_refs =
                resolve_table_references::<DepAlias>(&stmt).map_err(|err| match &err {
                    ResolveTableReferencesError::InvalidTableName { .. } => {
                        Error::InvalidTableName(err)
                    }
                    ResolveTableReferencesError::CatalogQualifiedTable { .. } => {
                        Error::CatalogQualifiedTable {
                            table_name: table_name.clone(),
                            source: err,
                        }
                    }
                    ResolveTableReferencesError::InvalidSchemaFormat { .. } => {
                        Error::InvalidDependencyAliasForTableRef {
                            table_name: table_name.clone(),
                            source: err,
                        }
                    }
                    _ => Error::TableReferenceResolution {
                        table_name: table_name.clone(),
                        source: err,
                    },
                })?;

            // Extract function references from the statement (supports both external deps and self-references)
            let func_refs = resolve_function_references::<DepAliasOrSelfRef>(&stmt).map_err(
                |err| match &err {
                    ResolveFunctionReferencesError::InvalidSchemaFormat { .. } => {
                        Error::InvalidDependencyAliasForFunctionRef {
                            table_name: table_name.clone(),
                            source: err,
                        }
                    }
                    ResolveFunctionReferencesError::CatalogQualifiedFunction { .. } => {
                        Error::CatalogQualifiedFunction {
                            table_name: table_name.clone(),
                            source: err,
                        }
                    }
                    _ => Error::FunctionReferenceResolution {
                        table_name: table_name.clone(),
                        source: err,
                    },
                },
            )?;

            statements.insert(table_name.clone(), stmt);
            references.insert(table_name, (table_refs, func_refs));
        }

        (statements, references)
    };

    // Create logical catalog using resolved dependencies
    let catalog = catalog::create(
        &ctx.dataset_store,
        IsolatePool::dummy(), // For schema validation only (no JS execution)
        dependencies,
        functions,
        references,
    )
    .await
    .map_err(|err| match &err {
        CreateLogicalCatalogError::ResolveTables(inner) => match inner {
            ResolveTablesError::UnqualifiedTable { .. } => Error::UnqualifiedTable(err),
            ResolveTablesError::DependencyAliasNotFound { .. } => {
                Error::DependencyAliasNotFound(err)
            }
            ResolveTablesError::GetDataset { source, .. } => {
                // NOTE: The source error is a BoxError from DatasetAccess::get_dataset().
                // When the underlying error is GetDatasetError::DatasetNotFound, we map to
                // Error::DatasetNotFound to return HTTP 404. All other GetDatasetError variants
                // (and other error types) map to Error::GetDataset for HTTP 500.
                if source
                    .downcast_ref::<GetDatasetError>()
                    .is_some_and(|e| matches!(e, GetDatasetError::DatasetNotFound(_)))
                {
                    Error::DatasetNotFound(err)
                } else {
                    Error::GetDataset(err)
                }
            }
            ResolveTablesError::TableNotFoundInDataset { .. } => Error::TableNotFoundInDataset(err),
        },
        CreateLogicalCatalogError::ResolveUdfs(inner) => match inner {
            ResolveUdfsError::DependencyAliasNotFound { .. } => Error::DependencyAliasNotFound(err),
            ResolveUdfsError::GetDataset { source, .. } => {
                // NOTE: The source error is a BoxError from DatasetAccess::get_dataset().
                // When the underlying error is GetDatasetError::DatasetNotFound, we map to
                // Error::DatasetNotFound to return HTTP 404. All other GetDatasetError variants
                // (and other error types) map to Error::GetDataset for HTTP 500.
                if source
                    .downcast_ref::<GetDatasetError>()
                    .is_some_and(|e| matches!(e, GetDatasetError::DatasetNotFound(_)))
                {
                    Error::DatasetNotFound(err)
                } else {
                    Error::GetDataset(err)
                }
            }
            ResolveUdfsError::EthCallUdfCreation { .. } => Error::EthCallUdfCreation(err),
            ResolveUdfsError::EthCallNotAvailable { .. } => Error::EthCallNotAvailable(err),
            ResolveUdfsError::FunctionNotFoundInDataset { .. } => {
                Error::FunctionNotFoundInDataset(err)
            }
            ResolveUdfsError::SelfReferencedFunctionNotFound { .. } => {
                Error::FunctionNotFoundInDataset(err)
            }
        },
    })?;

    // Create planning context from catalog
    let planning_ctx = PlanningContext::new(catalog);

    // Infer schema for each table and extract networks
    let mut schemas = BTreeMap::new();
    for (table_name, stmt) in statements {
        let plan =
            planning_ctx
                .plan_sql(stmt.clone())
                .await
                .map_err(|err| Error::SchemaInference {
                    table_name: table_name.clone(),
                    source: err,
                })?;
        // Return error if query is non-incremental
        plan.is_incremental()
            .map_err(|err| Error::NonIncrementalQuery {
                table_name: table_name.clone(),
                source: err,
            })?;
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

    /// User-defined function definitions mapped by function name
    ///
    /// Maps function names to their complete definitions including input/output types
    /// and implementation source code. These functions can be referenced in SQL queries
    /// as bare function calls (e.g., `my_function(args)` without dataset qualification).
    ///
    /// At least one of `tables` or `functions` must be provided.
    ///
    /// Function names must follow DataFusion UDF identifier rules:
    /// - Start with a letter (a-z, A-Z) or underscore (_)
    /// - Contain only letters, digits (0-9), underscores (_), and dollar signs ($)
    /// - Maximum length of 255 bytes
    #[serde(default)]
    #[cfg_attr(feature = "utoipa", schema(value_type = std::collections::BTreeMap<String, serde_json::Value>))]
    pub functions: BTreeMap<FuncName, Function>,
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

    #[error("Table '{table_name}' contains non-incremental SQL: {source}")]
    NonIncrementalQuery {
        table_name: TableName,
        #[source]
        source: BoxError,
    },

    /// Failed to resolve table references in SQL query
    ///
    /// This occurs when:
    /// - Table references contain invalid identifiers
    /// - Table references have unsupported format (not 1-3 parts)
    /// - Table names don't conform to identifier rules
    #[error("Failed to resolve table references for table '{table_name}': {source}")]
    TableReferenceResolution {
        /// The table name that contains the invalid references
        table_name: TableName,
        /// The underlying resolution error
        #[source]
        source: ResolveTableReferencesError<datasets_common::deps::alias::DepAliasError>,
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
        source:
            ResolveFunctionReferencesError<datasets_common::deps::alias::DepAliasOrSelfRefError>,
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
    ///
    /// This error is detected during table reference resolution from SQL.
    #[error("Catalog-qualified table in '{table_name}': {source}")]
    CatalogQualifiedTable {
        /// The table name that contains the catalog-qualified reference
        table_name: TableName,
        /// The underlying resolution error
        #[source]
        source: ResolveTableReferencesError<datasets_common::deps::alias::DepAliasError>,
    },

    /// Unqualified table reference
    ///
    /// All tables must be qualified with a dataset reference in the schema portion.
    /// Unqualified tables (e.g., just `table_name`) are not allowed.
    #[error(transparent)]
    UnqualifiedTable(CreateLogicalCatalogError),

    /// Invalid table name
    ///
    /// Table name does not conform to SQL identifier rules (must start with letter/underscore,
    /// contain only alphanumeric/underscore/dollar, and be <= 63 bytes).
    #[error("Invalid table name in SQL query: {0}")]
    InvalidTableName(
        #[source] ResolveTableReferencesError<datasets_common::deps::alias::DepAliasError>,
    ),

    /// Dataset reference not found
    ///
    /// The referenced dataset does not exist in the store.
    #[error(transparent)]
    DatasetNotFound(CreateLogicalCatalogError),

    /// Failed to retrieve dataset from store
    ///
    /// This occurs when loading a dataset definition fails due to:
    /// - Invalid or corrupted manifest
    /// - Unsupported dataset kind
    /// - Storage backend errors
    #[error(transparent)]
    GetDataset(CreateLogicalCatalogError),

    /// Failed to create ETH call UDF
    ///
    /// This occurs when creating the eth_call user-defined function fails.
    #[error(transparent)]
    EthCallUdfCreation(CreateLogicalCatalogError),

    /// Table not found in dataset
    ///
    /// The referenced table does not exist in the dataset.
    #[error(transparent)]
    TableNotFoundInDataset(CreateLogicalCatalogError),

    /// Function not found in dataset
    ///
    /// The referenced function does not exist in the dataset.
    #[error(transparent)]
    FunctionNotFoundInDataset(CreateLogicalCatalogError),

    /// eth_call function not available
    ///
    /// The eth_call function is not available for the referenced dataset.
    #[error(transparent)]
    EthCallNotAvailable(CreateLogicalCatalogError),

    /// Invalid dependency alias in table reference
    ///
    /// The dependency alias in a table reference does not conform to alias rules
    /// (must start with letter, contain only alphanumeric/underscore, and be <= 63 bytes).
    ///
    /// This error is detected during table reference resolution from SQL.
    #[error("Invalid dependency alias in table reference in '{table_name}': {source}")]
    InvalidDependencyAliasForTableRef {
        /// The table name that contains the invalid alias
        table_name: TableName,
        /// The underlying resolution error
        #[source]
        source: ResolveTableReferencesError<datasets_common::deps::alias::DepAliasError>,
    },

    /// Invalid dependency alias in function reference
    ///
    /// The dependency alias in a function reference does not conform to alias rules
    /// (must start with letter, contain only alphanumeric/underscore, and be <= 63 bytes).
    ///
    /// This error is detected during function reference resolution from SQL.
    #[error("Invalid dependency alias in function reference in '{table_name}': {source}")]
    InvalidDependencyAliasForFunctionRef {
        /// The table name that contains the invalid alias
        table_name: TableName,
        /// The underlying resolution error
        #[source]
        source:
            ResolveFunctionReferencesError<datasets_common::deps::alias::DepAliasOrSelfRefError>,
    },

    /// Catalog-qualified function reference not supported
    ///
    /// Only dataset-qualified functions are supported (e.g., `dataset.function`).
    /// Catalog-qualified functions (e.g., `catalog.schema.function`) are not supported.
    ///
    /// This error is detected during function reference resolution from SQL.
    #[error("Catalog-qualified function in '{table_name}': {source}")]
    CatalogQualifiedFunction {
        /// The table name that contains the catalog-qualified function reference
        table_name: TableName,
        /// The underlying resolution error
        #[source]
        source:
            ResolveFunctionReferencesError<datasets_common::deps::alias::DepAliasOrSelfRefError>,
    },

    /// Dependency alias not found
    ///
    /// A table or function reference uses an alias that was not provided in the dependencies map.
    #[error(transparent)]
    DependencyAliasNotFound(CreateLogicalCatalogError),

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
            Error::NonIncrementalQuery { .. } => "NON_INCREMENTAL_QUERY",
            Error::TableReferenceResolution { .. } => "TABLE_REFERENCE_RESOLUTION",
            Error::FunctionReferenceResolution { .. } => "FUNCTION_REFERENCE_RESOLUTION",
            Error::DependencyNotFound { .. } => "DEPENDENCY_NOT_FOUND",
            Error::DependencyResolution { .. } => "DEPENDENCY_RESOLUTION",
            Error::CatalogQualifiedTable { .. } => "CATALOG_QUALIFIED_TABLE",
            Error::UnqualifiedTable(_) => "UNQUALIFIED_TABLE",
            Error::InvalidTableName(_) => "INVALID_TABLE_NAME",
            Error::InvalidDependencyAliasForTableRef { .. } => {
                "INVALID_DEPENDENCY_ALIAS_FOR_TABLE_REF"
            }
            Error::InvalidDependencyAliasForFunctionRef { .. } => {
                "INVALID_DEPENDENCY_ALIAS_FOR_FUNCTION_REF"
            }
            Error::CatalogQualifiedFunction { .. } => "CATALOG_QUALIFIED_FUNCTION",
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
            Error::NonIncrementalQuery { .. } => StatusCode::BAD_REQUEST,
            Error::TableReferenceResolution { .. } => StatusCode::BAD_REQUEST,
            Error::FunctionReferenceResolution { .. } => StatusCode::BAD_REQUEST,
            Error::DependencyNotFound { .. } => StatusCode::NOT_FOUND,
            Error::DependencyResolution { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Error::CatalogQualifiedTable { .. } => StatusCode::BAD_REQUEST,
            Error::UnqualifiedTable(_) => StatusCode::BAD_REQUEST,
            Error::InvalidTableName(_) => StatusCode::BAD_REQUEST,
            Error::InvalidDependencyAliasForTableRef { .. } => StatusCode::BAD_REQUEST,
            Error::InvalidDependencyAliasForFunctionRef { .. } => StatusCode::BAD_REQUEST,
            Error::CatalogQualifiedFunction { .. } => StatusCode::BAD_REQUEST,
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
