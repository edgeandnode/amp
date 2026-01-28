//! Common utilities for HTTP handlers

use std::collections::BTreeMap;

use amp_dataset_store::{DatasetStore, GetDatasetError};
use amp_datasets_registry::error::ResolveRevisionError;
use common::{
    BlockNum,
    catalog::logical::for_manifest_validation::{
        self as catalog, CreateLogicalCatalogError, ResolveTablesError, ResolveUdfsError,
        TableReferencesMap,
    },
    planning_context::PlanningContext,
    query_context::Error as QueryContextErr,
    sql::{
        ResolveFunctionReferencesError, ResolveTableReferencesError, resolve_function_references,
        resolve_table_references,
    },
};
use datafusion::sql::parser::Statement;
use datasets_common::{hash_reference::HashReference, table_name::TableName};
use datasets_derived::{
    Manifest as DerivedDatasetManifest,
    deps::{DepAlias, DepAliasError, DepAliasOrSelfRef, DepAliasOrSelfRefError},
    manifest::{TableInput, View},
};
use js_runtime::isolate_pool::IsolatePool;

/// A string wrapper that ensures the value is not empty or whitespace-only
///
/// This invariant-holding _new-type_ validates that strings contain at least one non-whitespace character.
/// Validation occurs during:
/// - JSON/serde deserialization
/// - Parsing from `&str` via `FromStr`
///
/// ## Behavior
/// - Input strings are validated by checking if they contain non-whitespace characters after trimming
/// - Empty strings or whitespace-only strings are rejected with [`EmptyStringError`]
/// - The **original string is preserved** including any leading/trailing whitespace
/// - Once created, the string is guaranteed to contain at least one non-whitespace character
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "utoipa", schema(value_type = String))]
pub struct NonEmptyString(String);

impl NonEmptyString {
    /// Creates a new NonEmptyString without validation
    ///
    /// ## Safety
    /// The caller must ensure that the string contains at least one non-whitespace character.
    /// Passing an empty string or whitespace-only string violates the type's invariant and
    /// may lead to undefined behavior in code that relies on this guarantee.
    pub unsafe fn new_unchecked(value: String) -> Self {
        Self(value)
    }

    /// Returns a reference to the inner string value
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the NonEmptyString and returns the inner String
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<str> for NonEmptyString {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl AsRef<[u8]> for NonEmptyString {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl std::ops::Deref for NonEmptyString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for NonEmptyString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::str::FromStr for NonEmptyString {
    type Err = EmptyStringError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.trim().is_empty() {
            return Err(EmptyStringError);
        }
        Ok(NonEmptyString(s.to_string()))
    }
}

impl<'de> serde::Deserialize<'de> for NonEmptyString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

impl serde::Serialize for NonEmptyString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

/// Error type for NonEmptyString parsing failures
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("string cannot be empty or whitespace-only")]
pub struct EmptyStringError;

/// Parse, validate, and re-serialize a derived dataset manifest to canonical JSON format
///
/// This function handles derived datasets which require comprehensive validation:
/// 1. Deserialize from JSON string
/// 2. Validate manifest using dataset store (SQL, dependencies, tables, functions)
/// 3. Re-serialize to canonical JSON
pub async fn parse_and_canonicalize_derived_dataset_manifest(
    manifest_str: impl AsRef<str>,
    store: &DatasetStore,
) -> Result<String, ParseDerivedManifestError> {
    let manifest: DerivedDatasetManifest = serde_json::from_str(manifest_str.as_ref())
        .map_err(ParseDerivedManifestError::Deserialization)?;

    validate_derived_manifest(&manifest, store)
        .await
        .map_err(ParseDerivedManifestError::ManifestValidation)?;

    serde_json::to_string(&manifest).map_err(ParseDerivedManifestError::Serialization)
}

/// Error type for derived dataset manifest parsing and validation
#[derive(Debug, thiserror::Error)]
pub enum ParseDerivedManifestError {
    /// Failed to deserialize the JSON string into a `DerivedDatasetManifest` struct
    #[error("failed to deserialize manifest: {0}")]
    Deserialization(#[source] serde_json::Error),

    /// Failed manifest validation after successful deserialization
    #[error("manifest validation failed: {0}")]
    ManifestValidation(#[source] ManifestValidationError),

    /// Failed to serialize the validated manifest back to canonical JSON
    #[error("failed to serialize manifest: {0}")]
    Serialization(#[source] serde_json::Error),
}

/// Parse and re-serialize a raw dataset manifest to canonical JSON format
///
/// This function handles the common pattern for raw datasets (EvmRpc, Firehose, Solana):
/// 1. Deserialize from JSON string
/// 2. Re-serialize to canonical JSON
///
/// Returns canonical JSON string on success
pub fn parse_and_canonicalize_raw_dataset_manifest<T>(
    manifest_str: impl AsRef<str>,
) -> Result<String, ParseRawManifestError>
where
    T: serde::de::DeserializeOwned + serde::Serialize,
{
    let manifest: T = serde_json::from_str(manifest_str.as_ref())
        .map_err(ParseRawManifestError::Deserialization)?;
    serde_json::to_string(&manifest).map_err(ParseRawManifestError::Serialization)
}

/// Error type for raw dataset manifest parsing and canonicalization
///
/// Represents the different failure points when processing raw dataset manifests
/// (EvmRpc, Firehose, Solana) through the parse → canonicalize pipeline.
#[derive(Debug, thiserror::Error)]
pub enum ParseRawManifestError {
    /// Failed to deserialize the JSON string into the manifest struct
    ///
    /// This occurs when:
    /// - JSON syntax is invalid
    /// - JSON structure doesn't match the manifest schema
    /// - Required fields are missing or have wrong types
    #[error("failed to deserialize manifest: {0}")]
    Deserialization(#[source] serde_json::Error),

    /// Failed to serialize the manifest back to canonical JSON
    ///
    /// This occurs when:
    /// - The manifest structure cannot be serialized (rare, indicates a bug)
    /// - Memory allocation fails during serialization
    ///
    /// Note: This should rarely happen since we already deserialized successfully
    #[error("failed to serialize manifest: {0}")]
    Serialization(#[source] serde_json::Error),
}

/// Validates a derived dataset manifest with comprehensive checks.
///
/// This function performs deep validation of a manifest by:
/// 1. Parsing SQL queries from all table definitions
/// 2. Resolving all dependency references to their content hashes
/// 3. Extracting table and function references from SQL
/// 4. Building a planning context with actual dataset schemas
/// 5. Validating that all referenced tables and functions exist
///
/// Validation checks include:
/// - SQL syntax validity for all table queries
/// - All referenced datasets exist in the store
/// - All tables used in SQL exist in their datasets
/// - All functions used in SQL exist in their datasets
/// - Table schemas are compatible with SQL queries
// TODO: This validation logic was moved here from datasets-derived as part of a refactoring
//  to break the dependency between datasets-derived and common. This should eventually be
//  moved to a more appropriate location.
pub async fn validate_derived_manifest(
    manifest: &DerivedDatasetManifest,
    store: &DatasetStore,
) -> Result<(), ManifestValidationError> {
    // Step 1: Resolve all dependencies to HashReference
    // This must happen first to ensure all dependencies exist before parsing SQL
    let mut dependencies: BTreeMap<DepAlias, HashReference> = BTreeMap::new();

    for (alias, dep_reference) in &manifest.dependencies {
        // Convert DepReference to Reference for resolution
        let reference = dep_reference.to_reference();

        // Resolve reference to its manifest hash reference
        // This handles all revision types (Version, Hash)
        let reference = store
            .resolve_revision(&reference)
            .await
            .map_err(|err| ManifestValidationError::DependencyResolution {
                alias: alias.to_string(),
                reference: reference.to_string(),
                source: err,
            })?
            .ok_or_else(|| ManifestValidationError::DependencyNotFound {
                alias: alias.to_string(),
                reference: reference.to_string(),
            })?;

        dependencies.insert(alias.clone(), reference);
    }

    // Check if the start block is before the earliest available block of the dependencies
    if let Some(dataset_start_block) = &manifest.start_block {
        for (alias, dataset_ref) in &dependencies {
            let dataset = store.get_dataset(dataset_ref).await.map_err(|err| {
                ManifestValidationError::FetchDependencyDataset {
                    alias: alias.to_string(),
                    source: err,
                }
            })?;

            if let Some(dep_start_block) = dataset.start_block()
                && *dataset_start_block < dep_start_block
            {
                return Err(ManifestValidationError::StartBlockBeforeDependencies {
                    dataset_start_block: *dataset_start_block,
                    dependency_earliest_block: dep_start_block,
                });
            }
        }
    }

    // Step 2: Parse all SQL queries and extract references
    // Store parsed statements to avoid re-parsing in Step 4
    let mut statements: BTreeMap<TableName, Statement> = BTreeMap::new();
    let mut references: TableReferencesMap = BTreeMap::new();

    for (table_name, table) in &manifest.tables {
        let TableInput::View(View { sql }) = &table.input;

        // Parse SQL (validates single statement)
        let stmt =
            common::sql::parse(sql).map_err(|err| ManifestValidationError::InvalidTableSql {
                table_name: table_name.clone(),
                source: err,
            })?;

        // Extract table references
        let table_refs = resolve_table_references::<DepAlias>(&stmt).map_err(|err| match &err {
            ResolveTableReferencesError::InvalidTableName { .. } => {
                ManifestValidationError::InvalidTableName(err)
            }
            ResolveTableReferencesError::CatalogQualifiedTable { .. } => {
                ManifestValidationError::CatalogQualifiedTableInSql {
                    table_name: table_name.clone(),
                    source: err,
                }
            }
            _ => ManifestValidationError::TableReferenceResolution {
                table_name: table_name.clone(),
                source: err,
            },
        })?;

        // Extract function references (supports both external deps and self-references)
        let func_refs = resolve_function_references::<DepAliasOrSelfRef>(&stmt).map_err(|err| {
            ManifestValidationError::FunctionReferenceResolution {
                table_name: table_name.clone(),
                source: err,
            }
        })?;

        references.insert(table_name.clone(), (table_refs, func_refs));
        statements.insert(table_name.clone(), stmt);
    }

    // Step 3: Create planning context to validate all table and function references
    // This validates:
    // - All table references resolve to existing tables in dependencies
    // - All function references resolve to existing functions in dependencies
    // - Bare function references can be created as UDFs or are assumed to be built-ins
    // - Table references use valid dataset aliases from dependencies
    // - Schema compatibility across dependencies
    let planning_ctx = catalog::create(
        store,
        IsolatePool::dummy(), // For manifest validation only (no JS execution)
        dependencies,
        manifest.functions.clone(),
        references,
    )
    .await
    .map(PlanningContext::new)
    .map_err(|err| match &err {
        CreateLogicalCatalogError::ResolveTables(resolve_error) => match resolve_error {
            ResolveTablesError::UnqualifiedTable { .. } => {
                ManifestValidationError::UnqualifiedTable(err)
            }
            ResolveTablesError::DependencyAliasNotFound { .. } => {
                ManifestValidationError::DependencyAliasNotFound(err)
            }
            ResolveTablesError::GetDataset { .. } => ManifestValidationError::GetDataset(err),
            ResolveTablesError::TableNotFoundInDataset { .. } => {
                ManifestValidationError::TableNotFoundInDataset(err)
            }
        },
        CreateLogicalCatalogError::ResolveUdfs(resolve_error) => match resolve_error {
            ResolveUdfsError::DependencyAliasNotFound { .. } => {
                ManifestValidationError::DependencyAliasNotFound(err)
            }
            ResolveUdfsError::GetDataset { .. } => ManifestValidationError::GetDataset(err),
            ResolveUdfsError::EthCallUdfCreation { .. } => {
                ManifestValidationError::EthCallUdfCreation(err)
            }
            ResolveUdfsError::EthCallNotAvailable { .. } => {
                ManifestValidationError::EthCallNotAvailable(err)
            }
            ResolveUdfsError::FunctionNotFoundInDataset { .. } => {
                ManifestValidationError::FunctionNotFoundInDataset(err)
            }
            ResolveUdfsError::SelfReferencedFunctionNotFound { .. } => {
                ManifestValidationError::FunctionNotFoundInDataset(err)
            }
        },
    })?;

    // Step 4: Validate that all table SQL queries are incremental.
    // Incremental processing is required for derived datasets to efficiently update
    // as new blocks arrive. This check ensures no non-incremental operations are used.
    // Use cached parsed statements from Step 2 to avoid re-parsing.
    for (table_name, stmt) in statements {
        // Plan the SQL query to a logical plan
        let plan = planning_ctx.plan_sql(stmt).await.map_err(|err| {
            ManifestValidationError::SqlPlanningError {
                table_name: table_name.clone(),
                source: err,
            }
        })?;

        // Validate that the plan can be processed incrementally
        // This checks for non-incremental operations like aggregations, sorts, limits, outer joins, etc.
        plan.is_incremental()
            .map_err(|err| ManifestValidationError::NonIncrementalSql {
                table_name: table_name.clone(),
                source: err,
            })?;
    }

    Ok(())
}

/// Errors that occur during derived dataset manifest validation
#[derive(Debug, thiserror::Error)]
pub enum ManifestValidationError {
    /// Invalid SQL query in table definition
    ///
    /// This occurs when:
    /// - The provided SQL query has invalid syntax
    /// - Multiple statements provided (only single statement allowed)
    /// - Query parsing fails for other reasons
    #[error("Invalid SQL query for table '{table_name}': {source}")]
    InvalidTableSql {
        /// The table whose SQL query is invalid
        table_name: TableName,
        #[source]
        source: common::sql::ParseSqlError,
    },

    /// Failed to resolve table references from SQL query
    #[error("Failed to resolve table references in table '{table_name}': {source}")]
    TableReferenceResolution {
        /// The table whose SQL query contains unresolvable table references
        table_name: TableName,
        #[source]
        source: ResolveTableReferencesError<DepAliasError>,
    },

    /// Failed to resolve function references from SQL query
    #[error("Failed to resolve function references in table '{table_name}': {source}")]
    FunctionReferenceResolution {
        /// The table whose SQL query contains unresolvable function references
        table_name: TableName,
        #[source]
        source: ResolveFunctionReferencesError<DepAliasOrSelfRefError>,
    },

    /// Failed to resolve dependency reference to hash
    ///
    /// This occurs when resolving a dependency reference fails due to:
    /// - Invalid reference format
    /// - Storage backend errors when reading the dependency
    #[error("Failed to resolve dependency '{alias}' ({reference}): {source}")]
    DependencyResolution {
        /// The dependency alias used in the manifest
        alias: String,
        /// The dataset reference string (e.g., "dataset@version" or "dataset@hash")
        reference: String,
        #[source]
        source: ResolveRevisionError,
    },

    /// Dependency dataset not found
    ///
    /// This occurs when the dependency reference does not exist in the dataset store.
    #[error("Dependency '{alias}' not found ({reference})")]
    DependencyNotFound {
        /// The dependency alias used in the manifest
        alias: String,
        /// The dataset reference string (e.g., "dataset@version" or "dataset@hash")
        reference: String,
    },

    /// Failed to fetch dependency dataset for start_block validation
    ///
    /// This occurs when fetching the dataset definition for a dependency fails during
    /// start_block validation. The dataset reference was resolved successfully, but
    /// loading the actual dataset from the store failed.
    #[error("Failed to fetch dependency '{alias}' for start_block validation: {source}")]
    FetchDependencyDataset {
        /// The dependency alias that failed to load
        alias: String,
        #[source]
        source: GetDatasetError,
    },

    /// Catalog-qualified table reference in SQL query
    ///
    /// Only dataset-qualified tables are supported (e.g., `dataset.table`).
    /// Catalog-qualified tables (e.g., `catalog.schema.table`) are not supported.
    /// This error occurs during SQL parsing when a 3-part table reference is detected.
    #[error("Catalog-qualified table reference in table '{table_name}': {source}")]
    CatalogQualifiedTableInSql {
        /// The table whose SQL query contains a catalog-qualified table reference
        table_name: TableName,
        #[source]
        source: ResolveTableReferencesError<DepAliasError>,
    },

    /// Unqualified table reference
    ///
    /// All tables must be qualified with a dataset reference in the schema portion.
    /// Unqualified tables (e.g., just `table_name`) are not allowed.
    #[error("Unqualified table reference: {0}")]
    UnqualifiedTable(#[source] CreateLogicalCatalogError),

    /// Invalid table name
    ///
    /// Table name does not conform to SQL identifier rules (must start with letter/underscore,
    /// contain only alphanumeric/underscore/dollar, and be <= 63 bytes).
    #[error("Invalid table name in SQL query: {0}")]
    InvalidTableName(#[source] ResolveTableReferencesError<DepAliasError>),

    /// Failed to retrieve dataset from store
    ///
    /// This occurs when loading a dataset definition fails due to:
    /// - Invalid or corrupted manifest
    /// - Unsupported dataset kind
    /// - Storage backend errors
    #[error("Failed to retrieve dataset from store: {0}")]
    GetDataset(#[source] CreateLogicalCatalogError),

    /// Failed to create ETH call UDF
    ///
    /// This occurs when creating the eth_call user-defined function fails.
    #[error("Failed to create ETH call UDF: {0}")]
    EthCallUdfCreation(#[source] CreateLogicalCatalogError),

    /// Table not found in dataset
    ///
    /// The referenced table does not exist in the dataset.
    #[error("Table not found in dataset: {0}")]
    TableNotFoundInDataset(#[source] CreateLogicalCatalogError),

    /// Function not found in dataset
    ///
    /// The referenced function does not exist in the dataset.
    #[error("Function not found in dataset: {0}")]
    FunctionNotFoundInDataset(#[source] CreateLogicalCatalogError),

    /// eth_call function not available
    ///
    /// The eth_call function is not available for the referenced dataset.
    #[error("eth_call function not available: {0}")]
    EthCallNotAvailable(#[source] CreateLogicalCatalogError),

    /// Dependency alias not found
    ///
    /// A table reference uses an alias that was not provided in the dependencies map.
    #[error("Dependency alias not found: {0}")]
    DependencyAliasNotFound(#[source] CreateLogicalCatalogError),

    /// Non-incremental SQL operation in table query
    ///
    /// This occurs when a table's SQL query contains operations that cannot be
    /// processed incrementally (aggregations, sorts, limits, outer joins, window
    /// functions, distinct, or recursive queries).
    ///
    /// Incremental processing is required for derived datasets to efficiently
    /// update as new blocks arrive. Non-incremental operations would require
    /// recomputing the entire table on each update.
    ///
    /// Common non-incremental operations:
    /// - Aggregations: COUNT, SUM, MAX, MIN, AVG, GROUP BY
    /// - Sorting: ORDER BY
    /// - Deduplication: DISTINCT
    /// - Limiting: LIMIT, TOP
    /// - Outer joins: LEFT JOIN, RIGHT JOIN, FULL JOIN
    /// - Window functions: ROW_NUMBER, RANK, LAG, LEAD
    /// - Recursive queries: WITH RECURSIVE
    ///
    /// To resolve this error:
    /// - Remove aggregations and use raw data or pre-aggregated sources
    /// - Replace outer joins with inner joins
    /// - Remove sorting, limiting, and window functions
    /// - Ensure queries only use incremental operations (projection, filter, inner join, union)
    #[error("Table '{table_name}' contains non-incremental SQL: {source}")]
    NonIncrementalSql {
        /// The table whose SQL query contains non-incremental operations
        table_name: TableName,
        #[source]
        source: common::incrementalizer::NonIncrementalQueryError,
    },

    /// SQL query planning failed
    ///
    /// This error occurs when DataFusion fails to create a logical plan from
    /// the SQL query defined for a derived dataset table. Common causes include:
    /// - Invalid SQL syntax that passed parsing but failed planning
    /// - References to non-existent tables or columns
    /// - Type mismatches in expressions
    /// - Unsupported SQL features
    #[error("Failed to plan query for table '{table_name}': {source}")]
    SqlPlanningError {
        /// The table whose SQL query failed to plan
        table_name: TableName,
        #[source]
        source: QueryContextErr,
    },

    /// Start block before dependencies
    ///
    /// This occurs when the start block of the derived dataset is before
    /// the earliest block of one of the dependencies.
    #[error(
        "derived dataset start_block ({dataset_start_block}) is before dependency's earliest available block ({dependency_earliest_block})"
    )]
    StartBlockBeforeDependencies {
        /// The start block of the derived dataset
        dataset_start_block: BlockNum,
        /// The earliest available block of the dependency dataset
        dependency_earliest_block: BlockNum,
    },
}
