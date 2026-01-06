//! Catalog functions for derived datasets
//!
//! This module contains catalog creation functions that handle derived dataset dependencies.
//! These functions work with `DepAlias`, `DepReference`, and `FuncName` types that are specific
//! to derived datasets.
//!
//! ## SQL Catalog Functions
//!
//! SQL catalog functions provide catalog creation functions that work with pre-resolved
//! dataset dependencies (DepAlias → Hash mappings) for deterministic, reproducible
//! derived dataset execution.

use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use common::{
    BoxError, Dataset, PlanningContext, Store,
    catalog::{
        dataset_access::DatasetAccess,
        physical::{Catalog, PhysicalTable},
        resolve::{SchemaResolver, resolve_logical_catalog},
    },
    query_context::QueryEnv,
    sql::{
        FunctionReference, ResolveFunctionReferencesError, ResolveTableReferencesError,
        TableReference, resolve_function_references, resolve_table_references,
    },
};
use datafusion::sql::parser::Statement;
use datasets_common::{
    deps::alias::{DepAlias, DepAliasError, DepAliasOrSelfRef, DepAliasOrSelfRefError},
    hash_reference::HashReference,
    table_name::TableName,
};
use js_runtime::isolate_pool::IsolatePool;

// ============================================================================
// Pre-Resolved Resolver
// ============================================================================

/// Resolves schema strings using a pre-resolved dependencies map.
///
/// This resolver is used for derived datasets where dependencies are locked
/// and referenced by alias (e.g., `"eth"` → `HashReference`).
pub struct PreResolvedResolver<'a> {
    dependencies: &'a BTreeMap<DepAlias, HashReference>,
}

impl<'a> PreResolvedResolver<'a> {
    /// Creates a new pre-resolved resolver using the given dependencies map.
    pub fn new(dependencies: &'a BTreeMap<DepAlias, HashReference>) -> Self {
        Self { dependencies }
    }
}

#[async_trait]
impl SchemaResolver for PreResolvedResolver<'_> {
    type Error = PreResolvedError;

    async fn resolve(&self, schema: &str) -> Result<HashReference, Self::Error> {
        let alias: DepAlias = schema.parse().map_err(PreResolvedError::InvalidAlias)?;
        self.dependencies
            .get(&alias)
            .cloned()
            .ok_or_else(|| PreResolvedError::AliasNotFound(alias))
    }
}

/// Errors from pre-resolved schema resolution.
#[derive(Debug, thiserror::Error)]
pub enum PreResolvedError {
    /// Schema string could not be parsed as a valid alias.
    #[error("invalid alias format")]
    InvalidAlias(#[source] DepAliasError),

    /// Dependency alias was not found in the dependencies map.
    #[error("dependency alias not found: {0}")]
    AliasNotFound(DepAlias),
}

pub async fn catalog_for_derived_table(
    store: &impl DatasetAccess,
    data_store: &Store,
    query: &Statement,
    env: &QueryEnv,
    dependencies: &BTreeMap<DepAlias, HashReference>,
    self_dataset: Option<Arc<Dataset>>,
) -> Result<Catalog, CatalogForDerivedTableError> {
    // Extract table and function references from SQL
    let table_refs: Vec<_> = resolve_table_references::<DepAliasOrSelfRef>(query)
        .map_err(CatalogForDerivedTableError::TableReferenceResolution)?
        .into_iter()
        .filter_map(|r| r.into_parts())
        .collect();

    let func_refs: Vec<_> = resolve_function_references::<DepAliasOrSelfRef>(query)
        .map_err(CatalogForDerivedTableError::FunctionReferenceResolution)?
        .into_iter()
        .map(|r| r.into_parts())
        .collect();

    // Resolve using the pre-resolved resolver
    let resolver = PreResolvedResolver::new(dependencies);
    let logical_catalog = resolve_logical_catalog(
        store,
        &resolver,
        table_refs,
        func_refs,
        self_dataset,
        &env.isolate_pool,
    )
    .await
    .map_err(|e| CatalogForDerivedTableError::Resolution(Box::new(e)))?;

    // Resolve physical table locations
    let mut tables = Vec::new();
    for table in &logical_catalog.tables {
        let physical_table = PhysicalTable::get_active(data_store.clone(), table.clone())
            .await
            .map_err(|err| CatalogForDerivedTableError::PhysicalTableRetrieval {
                table: table.to_string(),
                source: err,
            })?
            .ok_or_else(|| CatalogForDerivedTableError::TableNotSynced {
                table: table.to_string(),
            })?;
        tables.push(physical_table.into());
    }

    Ok(Catalog::new(tables, logical_catalog))
}

/// Type alias for the table references map used in multi-table validation
///
/// Maps table names to their SQL references (table refs and function refs) using dependency aliases or self-references.
type TableReferencesMap = BTreeMap<
    TableName,
    (
        Vec<TableReference<DepAliasOrSelfRef>>,
        Vec<FunctionReference<DepAliasOrSelfRef>>,
    ),
>;

/// Creates a planning context for multi-table schema validation with pre-resolved dependencies.
///
/// This function validates dataset manifests by building logical catalogs for multiple
/// tables simultaneously, using dependencies that have been pre-resolved with aliases
/// by the caller.
///
/// ## Where Used
///
/// This function is used in three manifest validation paths:
///
/// 1. **Schema Endpoint** (`crates/services/admin-api/src/handlers/schema.rs`):
///    - Called via `POST /schema` endpoint from TypeScript CLI (`amp register`)
///    - Validates SQL in dataset manifests during interactive schema generation
///    - Returns schemas for manifest generation without accessing physical data
///
/// 2. **Manifest Registration** (`crates/services/admin-api/src/handlers/manifests/register.rs`):
///    - Called via `POST /manifests` endpoint during content-addressable manifest registration
///    - Validates derived dataset manifests via `datasets_derived::validate()`
///    - Ensures all SQL queries, dependencies, and table references are valid
///    - Stores validated manifests in content-addressable storage without dataset linking
///
/// 3. **Dataset Registration** (`crates/services/admin-api/src/handlers/datasets/register.rs`):
///    - Called via `POST /datasets` endpoint during dataset registration
///    - Validates derived dataset manifests via `datasets_derived::validate()`
///    - Ensures all SQL queries, dependencies, and table references are valid
///    - Prevents invalid manifests from being registered and linked to dataset versions
///
/// ## Implementation
///
/// Unlike `planning_ctx_for_sql`, this function:
/// 1. Accepts pre-resolved dependencies with aliases from the API request
/// 2. Processes multiple tables simultaneously (batch validation)
/// 3. Maps table references to user-provided dependency aliases
/// 4. Builds a unified logical catalog for all tables
/// 5. Returns planning context for schema validation only
///
/// This function does not access physical parquet files or the metadata database,
/// making it suitable for fast manifest validation during dataset registration.
///
/// ## Function Handling
///
/// Bare (unqualified) function references are handled as follows:
/// - If the function is defined in the `self_dataset` parameter, a UDF is created for it
/// - If the function is not defined, it's assumed to be a built-in function (logged as debug)
/// - TODO: Add validation against DataFusion built-in functions to catch typos
pub async fn planning_ctx_for_sql_tables_with_deps_and_funcs(
    store: &impl DatasetAccess,
    references: TableReferencesMap,
    dependencies: BTreeMap<DepAlias, HashReference>,
    self_dataset: Option<Arc<Dataset>>,
    isolate_pool: IsolatePool,
) -> Result<PlanningContext, PlanningCtxForSqlTablesWithDepsError> {
    // Check for unqualified tables and flatten refs
    let mut table_refs = Vec::new();
    let mut func_refs = Vec::new();

    for (table_name, (t_refs, f_refs)) in &references {
        for t_ref in t_refs {
            match t_ref.clone().into_parts() {
                Some(parts) => table_refs.push(parts),
                None => {
                    return Err(PlanningCtxForSqlTablesWithDepsError::UnqualifiedTable {
                        table_name: table_name.clone(),
                        table_ref: t_ref.to_string(),
                    });
                }
            }
        }
        for f_ref in f_refs {
            func_refs.push(f_ref.clone().into_parts());
        }
    }

    // Resolve using the pre-resolved resolver
    let resolver = PreResolvedResolver::new(&dependencies);
    let catalog = resolve_logical_catalog(
        store,
        &resolver,
        table_refs,
        func_refs,
        self_dataset,
        &isolate_pool,
    )
    .await
    .map_err(PlanningCtxForSqlTablesWithDepsError::Resolution)?;

    Ok(PlanningContext::new(catalog))
}

// ================================================================================================
// Error Types
// ================================================================================================
//
// Error types for derived dataset catalog operations.
// These error types were moved from `common::catalog::errors` to break the circular
// dependency between `common` and `datasets-derived`.

#[derive(Debug, thiserror::Error)]
pub enum PlanningCtxForSqlTablesWithDepsError {
    /// Table is not qualified with a schema/dataset name.
    ///
    /// All tables must be qualified with a dataset reference in the schema portion.
    /// Unqualified tables (e.g., just `table_name`) are not allowed.
    #[error(
        "In table '{table_name}': Unqualified table '{table_ref}', all tables must be qualified with a dataset"
    )]
    UnqualifiedTable {
        table_name: TableName,
        table_ref: String,
    },

    /// Failed to retrieve dataset from store when loading dataset for table reference.
    ///
    /// This occurs when loading a dataset definition fails:
    /// - Dataset not found in the store
    /// - Dataset manifest is invalid or corrupted
    /// - Unsupported dataset kind
    /// - Storage backend errors when reading the dataset
    #[error("In table '{table_name}': Failed to retrieve dataset '{reference}'")]
    GetDatasetForTableRef {
        table_name: TableName,
        reference: HashReference,
        #[source]
        source: BoxError,
    },

    /// Dependency alias not found when processing table reference.
    ///
    /// This occurs when a table reference uses an alias that was not provided
    /// in the dependencies map.
    #[error(
        "In table '{table_name}': Dependency alias '{alias}' referenced in table but not provided in dependencies"
    )]
    DependencyAliasNotFoundForTableRef {
        table_name: TableName,
        alias: DepAlias,
    },

    /// Dependency alias not found when processing function reference.
    ///
    /// This occurs when a function reference uses an alias that was not provided
    /// in the dependencies map.
    #[error(
        "In table '{table_name}': Dependency alias '{alias}' referenced in function but not provided in dependencies"
    )]
    DependencyAliasNotFoundForFunctionRef {
        table_name: TableName,
        alias: DepAlias,
    },

    /// Failed to retrieve dataset from store when loading dataset for function.
    ///
    /// This occurs when loading a dataset definition for a function fails:
    /// - Dataset not found in the store
    /// - Dataset manifest is invalid or corrupted
    /// - Unsupported dataset kind
    /// - Storage backend errors when reading the dataset
    #[error("In table '{table_name}': Failed to retrieve dataset '{reference}' for function")]
    GetDatasetForFunction {
        table_name: TableName,
        reference: HashReference,
        #[source]
        source: BoxError,
    },

    /// Failed to create ETH call UDF for dataset referenced in function name.
    ///
    /// This occurs when creating the eth_call user-defined function for a function fails:
    /// - Invalid provider configuration for the dataset
    /// - Provider connection issues
    /// - Dataset is not an EVM RPC dataset but eth_call was requested
    #[error(
        "In table '{table_name}': Failed to create ETH call UDF for dataset '{reference}' for function"
    )]
    EthCallUdfCreationForFunction {
        table_name: TableName,
        reference: HashReference,
        #[source]
        source: BoxError,
    },

    /// Function not found in dataset.
    ///
    /// This occurs when a function is referenced in the SQL query but the
    /// dataset does not contain a function with that name.
    #[error(
        "In table '{table_name}': Function '{function_name}' not found in dataset '{reference}'"
    )]
    FunctionNotFoundInDataset {
        table_name: TableName,
        function_name: String,
        reference: HashReference,
    },

    /// eth_call function not available for dataset.
    ///
    /// This occurs when the eth_call function is referenced in SQL but the
    /// dataset does not support eth_call (not an EVM RPC dataset or no provider configured).
    #[error("In table '{table_name}': Function 'eth_call' not available for dataset '{reference}'")]
    EthCallNotAvailable {
        table_name: TableName,
        reference: HashReference,
    },

    /// Table not found in dataset.
    ///
    /// This occurs when the table name is referenced in the SQL query but the
    /// dataset does not contain a table with that name.
    #[error(
        "In table '{table_name}': Table '{referenced_table_name}' not found in dataset '{reference}'"
    )]
    TableNotFoundInDataset {
        table_name: TableName,
        referenced_table_name: TableName,
        reference: HashReference,
    },

    /// Failed during catalog resolution.
    ///
    /// This wraps errors from the core resolution logic when processing
    /// flattened references where we cannot determine which derived table's
    /// SQL caused the error.
    #[error("Catalog resolution failed")]
    Resolution(#[source] ResolveError),
}

/// Type alias for resolve errors with pre-resolved dependencies.
pub type ResolveError = common::catalog::resolve::ResolveError<PreResolvedError>;

/// Errors specific to planning_ctx_for_sql operations
///
/// This error type is used exclusively by `planning_ctx_for_sql()` to create
/// a planning context for SQL queries without requiring physical data to exist.

#[derive(Debug, thiserror::Error)]
pub enum CatalogForDerivedTableError {
    /// Failed to resolve table references from the SQL statement.
    ///
    /// This occurs when:
    /// - Table references contain invalid identifiers
    /// - Table references have unsupported format (not 1-3 parts)
    /// - Table names don't conform to identifier rules
    /// - Schema portion fails to parse as DepAlias or self reference
    #[error("Failed to resolve table references from SQL")]
    TableReferenceResolution(#[source] ResolveTableReferencesError<DepAliasOrSelfRefError>),

    /// Failed to extract function names from the SQL statement.
    ///
    /// This occurs when:
    /// - The SQL statement contains DML operations (CreateExternalTable, CopyTo)
    /// - An EXPLAIN statement wraps an unsupported statement type
    /// - Schema portion fails to parse as DepAlias or self reference
    #[error("Failed to resolve function references from SQL")]
    FunctionReferenceResolution(#[source] ResolveFunctionReferencesError<DepAliasOrSelfRefError>),

    /// Failed during catalog resolution.
    ///
    /// This wraps errors from the core resolution logic.
    #[error("Catalog resolution failed")]
    Resolution(#[source] BoxError),

    /// Failed to retrieve physical table metadata from the metadata database.
    #[error("Failed to retrieve physical table metadata for table '{table}'")]
    PhysicalTableRetrieval {
        table: String,
        #[source]
        source: BoxError,
    },

    /// Table has not been synced and no physical location exists.
    #[error("Table '{table}' has not been synced")]
    TableNotSynced { table: String },
}
