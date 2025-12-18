//! SQL catalog creation and query planning functions.
//!
//! This module provides functions for building catalogs and planning contexts from SQL queries.
//! Each function serves a specific data path in the Amp architecture, with clear separation
//! between validation, planning, and execution operations.
//!
//! # Data Paths
//!
//! ## 1. Query Planning Path
//!
//! - **Purpose**: Generate query plans and schemas without execution
//! - **Function**: [`planning_ctx_for_sql`]
//! - **Entry**: Arrow Flight `GetFlightInfo` (`crates/services/server/src/flight.rs`)
//! - **Characteristics**: Fast schema response, logical catalog only, dynamic dataset resolution
//!
//! ## 2. Query Execution Path (Arrow Flight)
//!
//! - **Purpose**: Execute user queries via Arrow Flight
//! - **Function**: [`catalog_for_sql`]
//! - **Entry**: Arrow Flight `DoGet` (`crates/services/server/src/flight.rs`)
//! - **Characteristics**: Full catalog with physical parquet locations, dynamic dataset resolution, streaming results
//! - **Resolution Strategy**: Resolves dataset references to hashes at query time (supports "latest" and version tags)
//!
//! # Key Insights
//!
//! - **Clean separation**: Each public function serves exactly one primary data path
//! - **Dependency handling**: Two parallel execution paths with different resolution strategies:
//!   - **Dynamic resolution** (`catalog_for_sql`): For user queries that reference datasets by name/version
//!   - **Pre-resolved dependencies** (in `datasets-derived`): For derived datasets with locked dependencies
//! - **Core resolution**: All functions delegate to [`resolve::resolve_logical_catalog`] for the actual
//!   table and function resolution logic

use std::sync::Arc;

use datafusion::sql::parser::Statement;
use datasets_common::partial_reference::PartialReference;
use js_runtime::isolate_pool::IsolatePool;

use super::{
    dataset_access::DatasetAccess,
    errors::{CatalogForSqlError, PlanningCtxForSqlError},
    physical::{Catalog, PhysicalTable},
    resolve::{DynamicResolver, SelfReferences, resolve_logical_catalog},
};
use crate::{
    PlanningContext, Store,
    query_context::QueryEnv,
    sql::{resolve_function_references, resolve_table_references},
};
/// Creates a full catalog with physical data access for SQL query execution.
///
/// This function builds a complete catalog that includes both logical schemas and physical
/// parquet file locations, enabling actual query execution with DataFusion.
///
/// ## Where Used
///
/// This function is used exclusively in the **Query Execution Path**:
///
/// - **Arrow Flight DoGet** (`crates/services/server/src/flight.rs`):
///   - Called during Arrow Flight `DoGet` phase to execute user queries
///   - Provides physical catalog for streaming query results to clients
///
/// For derived dataset execution, use [`catalog_for_derived_table`](datasets_derived::catalog::catalog_for_derived_table) instead.
///
/// ## Implementation
///
/// The function:
/// 1. Extracts table references and function names from the query
/// 2. Calls [`resolve_logical_catalog`] to resolve datasets and build the logical catalog
/// 3. Queries metadata database for physical parquet locations
/// 4. Constructs physical catalog for query execution
pub async fn catalog_for_sql(
    dataset_store: &impl DatasetAccess,
    data_store: &Store,
    query: &Statement,
    env: QueryEnv,
) -> Result<Catalog, CatalogForSqlError> {
    // Extract table and function references from SQL
    let table_refs: Vec<_> = resolve_table_references::<PartialReference>(query)
        .map_err(CatalogForSqlError::TableReferenceResolution)?
        .into_iter()
        .filter_map(|r| r.into_parts())
        .collect();

    let func_refs: Vec<_> = resolve_function_references::<PartialReference>(query)
        .map_err(CatalogForSqlError::FunctionReferenceResolution)?
        .into_iter()
        .map(|r| r.into_parts())
        .collect();

    // Resolve using the dynamic resolver
    let resolver = DynamicResolver::new(dataset_store);
    let logical_catalog = resolve_logical_catalog(
        dataset_store,
        &resolver,
        table_refs,
        func_refs,
        SelfReferences::empty(), // No self-references for user queries
        &env.isolate_pool,
    )
    .await
    .map_err(|e| CatalogForSqlError::Resolution(Box::new(e)))?;

    // Resolve physical table locations
    let mut tables = Vec::new();
    for table in &logical_catalog.tables {
        let physical_table = PhysicalTable::get_active(data_store.clone(), table.clone())
            .await
            .map_err(|err| CatalogForSqlError::PhysicalTableRetrieval {
                table: table.to_string(),
                source: err,
            })?
            .ok_or_else(|| CatalogForSqlError::TableNotSynced {
                table: table.to_string(),
            })?;
        tables.push(Arc::new(physical_table));
    }

    Ok(Catalog::new(tables, logical_catalog))
}

/// Creates a full catalog with physical data access for SQL query execution with pre-resolved dependencies.
///
/// This function builds a complete catalog that includes both logical schemas and physical
/// parquet file locations, enabling actual query execution with DataFusion. Unlike
/// `catalog_for_sql`, this function accepts pre-resolved dependencies from a derived dataset
/// manifest, ensuring that SQL schema references map to the exact dataset versions specified
/// in the manifest's dependencies.
///
/// ## Where Used
///
/// This function is used exclusively in the **Derived Dataset Execution Path**:
///
/// - **`crates/core/dump/src/core/sql_dump.rs`**:
///   - Called during worker-based extraction to execute SQL-defined derived datasets
///   - Uses dependencies from the derived dataset manifest
///   - Writes query results as parquet files to object storage
///
/// ## Implementation
///
/// The function analyzes the SQL query to:
/// 1. Extract table references and function names from the query
/// 2. Map SQL schema references to pre-resolved dependency aliases
/// 3. Load datasets by their locked hashes from the manifest dependencies
/// 4. Build logical catalog with schemas and UDFs
/// 5. Query metadata database for physical parquet locations
/// 6. Construct physical catalog for query execution
///
/// ## Dependencies Parameter
///
/// The `dependencies` parameter is a map from dependency aliases (as used in SQL schema qualifiers)
/// to fully-qualified dataset names and their content hashes. This ensures that:
/// - SQL queries use the exact dataset versions declared in the manifest
/// - No dynamic version resolution occurs during execution
/// - Derived datasets are reproducible and deterministic
///
/// Creates a planning context for SQL query planning without physical data access.
///
/// This function builds a logical catalog with schemas only, enabling query plan generation
/// and schema inference without accessing physical parquet files.
///
/// ## Where Used
///
/// This function is used exclusively in the **Query Execution Path** for the planning phase:
///
/// - **Arrow Flight GetFlightInfo** (`crates/services/server/src/flight.rs`):
///   - Called to generate query plan and return schema to clients
///   - Fast response without accessing physical data files
///   - Precedes actual query execution which uses `catalog_for_sql`
///
/// ## Implementation
///
/// The function analyzes the SQL query to:
/// 1. Extract table references and function names from the query
/// 2. Resolve dataset names to hashes via the dataset store
/// 3. Build logical catalog with schemas and UDFs
/// 4. Return planning context for DataFusion query planning
///
/// Unlike `catalog_for_sql`, this does not query the metadata database for physical
/// parquet locations, making it faster for planning-only operations.
pub async fn planning_ctx_for_sql(
    store: &impl DatasetAccess,
    query: &Statement,
) -> Result<PlanningContext, PlanningCtxForSqlError> {
    // Extract table and function references from SQL
    let table_refs: Vec<_> = resolve_table_references::<PartialReference>(query)
        .map_err(PlanningCtxForSqlError::TableReferenceResolution)?
        .into_iter()
        .filter_map(|r| r.into_parts())
        .collect();

    let func_refs: Vec<_> = resolve_function_references::<PartialReference>(query)
        .map_err(PlanningCtxForSqlError::FunctionReferenceResolution)?
        .into_iter()
        .map(|r| r.into_parts())
        .collect();

    // Resolve using the dynamic resolver
    let resolver = DynamicResolver::new(store);
    let catalog = resolve_logical_catalog(
        store,
        &resolver,
        table_refs,
        func_refs,
        SelfReferences::empty(), // No self-references for user queries
        &IsolatePool::dummy(),
    )
    .await
    .map_err(|e| PlanningCtxForSqlError::Resolution(Box::new(e)))?;

    Ok(PlanningContext::new(catalog))
}
