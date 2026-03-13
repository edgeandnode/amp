use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use axum::{
    Json,
    extract::{State, rejection::JsonRejection},
    http::StatusCode,
};
use common::{
    amp_catalog_provider::{AMP_CATALOG_NAME, AmpCatalogProvider, AsyncSchemaProvider},
    context::plan::{PlanContextBuilder, is_user_input_error},
    exec_env::default_session_config,
    incrementalizer::NonIncrementalQueryError,
    plan_visitors::prepend_special_block_num_field,
    self_schema_provider::SelfSchemaProvider,
    sql::{self, ResolveTableReferencesError},
    sql_str::SqlStr,
};
use datafusion::{arrow, sql::parser::Statement};
use datasets_common::{hash_reference::HashReference, table_name::TableName};
use datasets_derived::{
    deps::{DepAlias, DepAliasOrSelfRef, DepAliasOrSelfRefError, DepReference, HashOrVersion},
    func_name::FuncName,
    function::Function,
    manifest::TableSchema,
};

use crate::{
    ctx::Ctx,
    handlers::{
        common::{InterTableDepError, resolve_inter_table_order},
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
/// - `NON_INCREMENTAL_QUERY`: SQL query is non-incremental
/// - `SELF_REFERENCING_TABLE`: A table references itself via `self.<own_name>`
/// - `CYCLIC_DEPENDENCY`: Inter-table dependencies form a cycle
/// - `TABLE_REFERENCE_RESOLUTION`: Failed to extract table references from SQL
/// - `NO_TABLE_REFERENCES`: Table SQL does not reference any source tables
/// - `FUNCTION_REFERENCE_RESOLUTION`: Failed to extract function references from SQL
/// - `DEPENDENCY_NOT_FOUND`: Referenced dependency does not exist
/// - `DEPENDENCY_MANIFEST_LINK_CHECK`: Failed to verify manifest link for dependency
/// - `DEPENDENCY_VERSION_RESOLUTION`: Failed to resolve version for dependency
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
/// - `SESSION_CONFIG_ERROR`: Failed to create DataFusion session configuration
/// - `SCHEMA_INFERENCE`: Failed to infer output schema from query
///
/// ## Schema Analysis Process
/// 1. **Parse SQL**: Validates syntax using DataFusion's SQL parser
/// 2. **Load Datasets**: Retrieves dataset definitions from the registry for all referenced datasets
/// 3. **Create Planning Context**: Builds planning context with real table schemas from stored datasets
/// 4. **Infer Schema**: Uses DataFusion's query planner to determine output schema without executing the query
/// 5. **Prepend Special Fields**: Adds `RESERVED_BLOCK_NUM_COLUMN_NAME` field to the output schema
/// 6. **Extract Networks**: Identifies which blockchain networks are referenced by the query
///
/// # Panics
///
/// Panics if `resolve_inter_table_order` returns a table name that was not in the
/// original statements map. This is structurally impossible because the sort only
/// returns keys from its input.
#[tracing::instrument(skip_all, err)]
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
            schemas: Default::default(),
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
                        .map_err(|err| Error::DependencyManifestLinkCheck {
                            alias: alias.clone(),
                            reference: dep_ref.clone(),
                            source: err,
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
                        .map_err(|err| Error::DependencyVersionResolution {
                            alias: alias.clone(),
                            reference: dep_ref.clone(),
                            source: err,
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
    let mut statements = {
        let mut statements: BTreeMap<TableName, Statement> = BTreeMap::new();

        for (table_name, sql_query) in tables {
            let stmt = common::sql::parse(&sql_query).map_err(|err| Error::InvalidTableSql {
                table_name: table_name.clone(),
                source: err,
            })?;

            statements.insert(table_name, stmt);
        }

        statements
    };

    // Extract table references per table, validate non-empty, then determine
    // inter-table processing order via the shared helper.
    let mut parsed_refs: BTreeMap<TableName, Vec<common::sql::TableReference<DepAliasOrSelfRef>>> =
        BTreeMap::new();
    for (table_name, stmt) in &statements {
        let table_refs =
            sql::resolve_table_references::<DepAliasOrSelfRef>(stmt).map_err(|err| match &err {
                ResolveTableReferencesError::InvalidTableName { .. } => Error::InvalidTableName {
                    table_name: table_name.clone(),
                    source: err,
                },
                ResolveTableReferencesError::CatalogQualifiedTable { .. } => {
                    Error::CatalogQualifiedTableInSql {
                        table_name: table_name.clone(),
                        source: err,
                    }
                }
                _ => Error::TableReferenceResolution {
                    table_name: table_name.clone(),
                    source: err,
                },
            })?;

        // Reject tables whose SQL references no source tables (e.g., `SELECT 1`).
        if table_refs.is_empty() {
            return Err(Error::NoTableReferences {
                table_name: table_name.clone(),
            }
            .into());
        }

        parsed_refs.insert(table_name.clone(), table_refs);
    }

    let known_tables: BTreeSet<TableName> = statements.keys().cloned().collect();
    let table_order =
        resolve_inter_table_order(&parsed_refs, &known_tables).map_err(Error::InterTableDep)?;

    // Build dep_aliases for AmpCatalogProvider before dependencies is consumed
    let dep_aliases: BTreeMap<String, HashReference> = dependencies
        .iter()
        .map(|(alias, hash_ref)| (alias.to_string(), hash_ref.clone()))
        .collect();

    // Create planning context with self-schema provider.
    // Inter-table references use `self.<table_name>` syntax, which resolves through the
    // SelfSchemaProvider registered under the "self" schema in the AmpCatalogProvider.
    let session_config = default_session_config().map_err(Error::SessionConfig)?;
    let self_schema_provider = Arc::new(SelfSchemaProvider::from_manifest_udfs(&functions));
    let amp_catalog = Arc::new(
        AmpCatalogProvider::new(ctx.datasets_cache.clone(), ctx.ethcall_udfs_cache.clone())
            .with_dep_aliases(dep_aliases)
            .with_self_schema(self_schema_provider.clone() as Arc<dyn AsyncSchemaProvider>),
    );
    let planning_ctx = PlanContextBuilder::new(session_config)
        .with_table_catalog(AMP_CATALOG_NAME, amp_catalog.clone())
        .with_func_catalog(AMP_CATALOG_NAME, amp_catalog)
        .build();

    // Infer schema for each table in topological order.
    // After inferring a table's schema, register it with the self-schema provider
    // so that subsequent tables can reference it via `self.<table_name>`.
    let mut schemas = BTreeMap::new();
    for table_name in table_order {
        // topological_sort only returns keys from the input map
        let stmt = statements
            .remove(&table_name)
            .expect("topological_sort returned unknown table");
        let plan = planning_ctx.statement_to_plan(stmt).await.map_err(|err| {
            Error::SchemaPlanInference {
                table_name: table_name.clone(),
                source: err,
            }
        })?;
        // Return error if query is non-incremental
        plan.is_incremental()
            .map_err(|err| Error::NonIncrementalQuery {
                table_name: table_name.clone(),
                source: err,
            })?;
        // Infer schema using the planning context
        let schema = plan.schema();

        // Prepend the special block number field
        let schema = prepend_special_block_num_field(&schema);

        // Register the inferred schema so subsequent tables can reference this table
        let arrow_schema: Arc<arrow::datatypes::Schema> = Arc::new(schema.as_arrow().clone());
        self_schema_provider.add_table(table_name.as_str(), arrow_schema);

        schemas.insert(table_name, schema.into());
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
/// Contains inferred schemas for one or more tables.
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct SchemaResponse {
    /// Schemas for each table
    ///
    /// Maps table names to their inferred schemas.
    /// Contains one entry per table definition.
    #[cfg_attr(feature = "utoipa", schema(value_type = std::collections::BTreeMap<String, serde_json::Value>))]
    schemas: BTreeMap<TableName, TableSchema>,
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

    /// SQL query contains non-incremental operations
    ///
    /// This occurs when a table's SQL uses operations that cannot be processed incrementally
    /// (e.g., aggregations, sorts, limits, outer joins, window functions, distinct).
    #[error("Table '{table_name}' contains non-incremental SQL: {source}")]
    NonIncrementalQuery {
        /// The table whose SQL query contains non-incremental operations
        table_name: TableName,
        #[source]
        source: NonIncrementalQueryError,
    },

    /// Inter-table dependency validation failed
    ///
    /// This occurs when validating `self.`-qualified table references within the
    /// dataset: self-referencing tables, nonexistent sibling targets, or cyclic deps.
    #[error("Inter-table dependency error")]
    InterTableDep(#[source] InterTableDepError),

    /// Catalog-qualified tables (e.g., `catalog.schema.table`) are not supported.
    ///
    /// This occurs during SQL parsing when a 3-part table reference is detected.
    #[error("Catalog-qualified table reference in table '{table_name}': {source}")]
    CatalogQualifiedTableInSql {
        /// The table whose SQL query contains a catalog-qualified table reference
        table_name: TableName,
        #[source]
        source: ResolveTableReferencesError<DepAliasOrSelfRefError>,
    },

    /// Table name in SQL reference has invalid format.
    ///
    /// This occurs when a table name extracted from SQL does not conform to identifier rules.
    #[error("Invalid table name in table '{table_name}': {source}")]
    InvalidTableName {
        /// The table whose SQL query contains an invalid table name
        table_name: TableName,
        #[source]
        source: ResolveTableReferencesError<DepAliasOrSelfRefError>,
    },

    /// Failed to extract table references from SQL query
    ///
    /// This occurs when resolving table references from a parsed SQL statement fails
    /// for reasons other than catalog qualification or invalid table names.
    #[error("Failed to extract table references from table '{table_name}': {source}")]
    TableReferenceResolution {
        /// The table whose SQL query failed reference extraction
        table_name: TableName,
        /// The underlying error
        #[source]
        source: ResolveTableReferencesError<DepAliasOrSelfRefError>,
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

    /// Failed to verify manifest link for dependency
    ///
    /// This occurs when checking if a manifest hash is linked to a dataset fails,
    /// typically due to database connection issues or query failures.
    #[error("Failed to verify manifest link for dependency '{alias}' ({reference})")]
    DependencyManifestLinkCheck {
        /// The alias name used in the request
        alias: DepAlias,
        /// The dependency reference
        reference: DepReference,
        /// The underlying database error
        #[source]
        source: amp_datasets_registry::error::IsManifestLinkedError,
    },

    /// Failed to resolve version for dependency
    ///
    /// This occurs when resolving a version tag to a manifest hash fails,
    /// typically due to database connection issues or query failures.
    #[error("Failed to resolve version for dependency '{alias}' ({reference})")]
    DependencyVersionResolution {
        /// The alias name used in the request
        alias: DepAlias,
        /// The dependency reference
        reference: DepReference,
        /// The underlying database error
        #[source]
        source: amp_datasets_registry::error::ResolveRevisionError,
    },

    /// Table SQL does not reference any source tables
    ///
    /// This occurs when a derived table's SQL query contains no table references
    /// (e.g., `SELECT 1`). Derived tables must reference at least one external
    /// dependency or sibling table via `self.<table_name>`.
    #[error("Table '{table_name}' does not reference any source tables")]
    NoTableReferences {
        /// The table whose SQL query contains no table references
        table_name: TableName,
    },

    /// Failed to create DataFusion session configuration
    #[error("failed to create session config")]
    SessionConfig(#[source] datafusion::error::DataFusionError),

    /// Failed to plan SQL during schema inference for a table
    ///
    /// This occurs when SQL planning fails for a table query during schema
    /// inference (e.g., invalid references, type mismatches).
    #[error("Failed to plan SQL for table '{table_name}': {source}")]
    SchemaPlanInference {
        /// The table name that failed schema inference
        table_name: TableName,
        /// The underlying SQL planning error
        #[source]
        source: datafusion::error::DataFusionError,
    },
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidPayloadFormat { .. } => "INVALID_PAYLOAD_FORMAT",
            Error::EmptyTablesAndFunctions => "EMPTY_TABLES_AND_FUNCTIONS",
            Error::InvalidTableSql { .. } => "INVALID_TABLE_SQL",
            Error::NonIncrementalQuery { .. } => "NON_INCREMENTAL_QUERY",
            Error::InterTableDep(inner) => inner.error_code(),
            Error::CatalogQualifiedTableInSql { .. } => "CATALOG_QUALIFIED_TABLE",
            Error::InvalidTableName { .. } => "INVALID_TABLE_NAME",
            Error::TableReferenceResolution { .. } => "TABLE_REFERENCE_RESOLUTION",
            Error::DependencyNotFound { .. } => "DEPENDENCY_NOT_FOUND",
            Error::DependencyManifestLinkCheck { .. } => "DEPENDENCY_MANIFEST_LINK_CHECK",
            Error::DependencyVersionResolution { .. } => "DEPENDENCY_VERSION_RESOLUTION",
            Error::NoTableReferences { .. } => "NO_TABLE_REFERENCES",
            Error::SessionConfig(_) => "SESSION_CONFIG_ERROR",
            Error::SchemaPlanInference { source, .. } if is_user_input_error(source) => {
                "INVALID_PLAN"
            }
            Error::SchemaPlanInference { .. } => "SCHEMA_INFERENCE",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidPayloadFormat { .. } => StatusCode::BAD_REQUEST,
            Error::EmptyTablesAndFunctions => StatusCode::BAD_REQUEST,
            Error::InvalidTableSql { .. } => StatusCode::BAD_REQUEST,
            Error::NonIncrementalQuery { .. } => StatusCode::BAD_REQUEST,
            Error::InterTableDep(_) => StatusCode::BAD_REQUEST,
            Error::CatalogQualifiedTableInSql { .. } => StatusCode::BAD_REQUEST,
            Error::InvalidTableName { .. } => StatusCode::BAD_REQUEST,
            Error::TableReferenceResolution { .. } => StatusCode::BAD_REQUEST,
            Error::DependencyNotFound { .. } => StatusCode::NOT_FOUND,
            Error::DependencyManifestLinkCheck { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Error::DependencyVersionResolution { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Error::NoTableReferences { .. } => StatusCode::BAD_REQUEST,
            Error::SessionConfig(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::SchemaPlanInference { source, .. } if is_user_input_error(source) => {
                StatusCode::BAD_REQUEST
            }
            Error::SchemaPlanInference { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
