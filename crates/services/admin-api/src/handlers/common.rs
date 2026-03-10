//! Common utilities for HTTP handlers

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use amp_data_store::{DataStore, PhyTableRevision};
use amp_datasets_registry::error::ResolveRevisionError;
use amp_parquet::footer::{AmpMetadataFromParquetError, amp_metadata_from_parquet_file};
use common::{
    amp_catalog_provider::{AMP_CATALOG_NAME, AmpCatalogProvider, AsyncSchemaProvider},
    context::plan::PlanContextBuilder,
    datasets_cache::{DatasetsCache, GetDatasetError},
    ethcall_udfs_cache::EthCallUdfsCache,
    exec_env::default_session_config,
    self_schema_provider::SelfSchemaProvider,
    sql::{
        FunctionReference, ResolveFunctionReferencesError, ResolveTableReferencesError,
        TableReference, resolve_function_references, resolve_table_references,
    },
};
use datafusion::sql::parser::Statement;
use datasets_common::{hash_reference::HashReference, table_name::TableName};
use datasets_derived::{
    Manifest as DerivedDatasetManifest,
    deps::{DepAlias, DepAliasOrSelfRef, DepAliasOrSelfRefError},
    manifest::{TableInput, View},
    sorting::{self, CyclicDepError},
};
use futures::{StreamExt as _, stream};
use js_runtime::isolate_pool::IsolatePool;

/// Map of table names to their SQL references (table refs and function refs) using dependency aliases or self-references.
type TableReferencesMap = BTreeMap<
    TableName,
    (
        Vec<TableReference<DepAliasOrSelfRef>>,
        Vec<FunctionReference<DepAliasOrSelfRef>>,
    ),
>;

/// Extracts inter-table dependencies from parsed table references and returns
/// a topologically sorted processing order.
///
/// For each table, inspects `self.`-qualified table references, validates that
/// they don't self-reference or target nonexistent siblings, builds a dependency
/// graph, and topologically sorts it.
///
/// ## Parameters
/// - `table_refs`: Iterator of `(table_name, table_references)` pairs
/// - `known_tables`: Set of all table names in the dataset (used to validate
///   that `self.` targets exist)
pub fn resolve_inter_table_order<'a>(
    table_refs: impl IntoIterator<Item = (&'a TableName, &'a [TableReference<DepAliasOrSelfRef>])>,
    known_tables: &BTreeSet<TableName>,
) -> Result<Vec<TableName>, InterTableDepError> {
    let mut deps: BTreeMap<TableName, Vec<TableName>> = BTreeMap::new();
    for (table_name, refs) in table_refs {
        let mut table_deps = Vec::new();
        for table_ref in refs {
            if let TableReference::Partial { schema, table } = table_ref
                && schema.as_ref().is_self()
            {
                if table.as_ref() == table_name {
                    return Err(InterTableDepError::SelfReferencingTable {
                        table_name: table_name.clone(),
                    });
                }
                if known_tables.contains(table.as_ref()) {
                    table_deps.push(table.as_ref().clone());
                } else {
                    return Err(InterTableDepError::SelfRefTableNotFound {
                        source_table: table_name.clone(),
                        referenced_table: table.as_ref().clone(),
                    });
                }
            }
        }
        deps.insert(table_name.clone(), table_deps);
    }
    sorting::topological_sort(deps).map_err(InterTableDepError::CyclicDependency)
}

/// Errors from extracting and validating inter-table dependencies.
#[derive(Debug, thiserror::Error)]
pub enum InterTableDepError {
    /// A table references itself via `self.<table_name>`
    ///
    /// This occurs when a table's SQL query contains `self.<own_name>`, which would
    /// create a trivial circular dependency. Tables cannot reference themselves.
    #[error("Table '{table_name}' references itself via self.{table_name}")]
    SelfReferencingTable {
        /// The table that references itself
        table_name: TableName,
    },

    /// A `self.`-qualified table reference targets a table that does not exist in the dataset.
    #[error(
        "Table '{source_table}' references non-existent sibling table 'self.{referenced_table}'"
    )]
    SelfRefTableNotFound {
        /// The table whose SQL contains the invalid self-ref
        source_table: TableName,
        /// The referenced table name that does not exist
        referenced_table: TableName,
    },

    /// Cyclic dependency detected among tables within the dataset
    ///
    /// This occurs when tables reference each other in a cycle (e.g., table_a references
    /// table_b which references table_a). Inter-table dependencies must form a DAG.
    #[error("Cyclic dependency detected among inter-table references: {0}")]
    CyclicDependency(#[source] CyclicDepError<TableName>),
}

impl InterTableDepError {
    /// Returns the machine-readable error code for this error.
    pub fn error_code(&self) -> &'static str {
        match self {
            Self::SelfReferencingTable { .. } => "SELF_REFERENCING_TABLE",
            Self::SelfRefTableNotFound { .. } => "SELF_REF_TABLE_NOT_FOUND",
            Self::CyclicDependency(_) => "CYCLIC_DEPENDENCY",
        }
    }
}

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
    datasets_cache: &DatasetsCache,
    ethcall_udfs_cache: &EthCallUdfsCache,
) -> Result<String, ParseDerivedManifestError> {
    let manifest: DerivedDatasetManifest = serde_json::from_str(manifest_str.as_ref())
        .map_err(ParseDerivedManifestError::Deserialization)?;

    validate_derived_manifest(&manifest, datasets_cache, ethcall_udfs_cache)
        .await
        .map_err(|err| ParseDerivedManifestError::ManifestValidation(Box::new(err)))?;

    serde_json::to_string(&manifest).map_err(ParseDerivedManifestError::Serialization)
}

/// Error type for derived dataset manifest parsing and validation
#[derive(Debug, thiserror::Error)]
pub enum ParseDerivedManifestError {
    /// Failed to deserialize the JSON string into a `DerivedDatasetManifest` struct
    #[error("failed to deserialize manifest")]
    Deserialization(#[source] serde_json::Error),

    /// Failed manifest validation after successful deserialization
    #[error("manifest validation failed")]
    ManifestValidation(#[source] Box<ManifestValidationError>),

    /// Failed to serialize the validated manifest back to canonical JSON
    #[error("failed to serialize manifest")]
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
    #[error("failed to deserialize manifest")]
    Deserialization(#[source] serde_json::Error),

    /// Failed to serialize the manifest back to canonical JSON
    ///
    /// This occurs when:
    /// - The manifest structure cannot be serialized (rare, indicates a bug)
    /// - Memory allocation fails during serialization
    ///
    /// Note: This should rarely happen since we already deserialized successfully
    #[error("failed to serialize manifest")]
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
///
/// # Panics
///
/// Panics if `topological_sort` returns a table name that was not in the original
/// statements or manifest tables maps. This is structurally impossible because the
/// sort only returns keys from its input, which are derived from these maps.
// TODO: This validation logic was moved here from datasets-derived as part of a refactoring
//  to break the dependency between datasets-derived and common. This should eventually be
//  moved to a more appropriate location.
pub async fn validate_derived_manifest(
    manifest: &DerivedDatasetManifest,
    datasets_cache: &DatasetsCache,
    ethcall_udfs_cache: &EthCallUdfsCache,
) -> Result<(), ManifestValidationError> {
    // Step 1: Resolve all dependencies to HashReference
    // This must happen first to ensure all dependencies exist before parsing SQL
    let mut dependencies: BTreeMap<DepAlias, HashReference> = BTreeMap::new();

    for (alias, dep_reference) in &manifest.dependencies {
        // Convert DepReference to Reference for resolution
        let reference = dep_reference.to_reference();

        // Resolve reference to its manifest hash reference
        // This handles all revision types (Version, Hash)
        let reference = datasets_cache
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

    // Step 2: Parse all SQL queries and extract references
    // Store parsed statements to avoid re-parsing in Step 4
    let mut statements: BTreeMap<TableName, Statement> = BTreeMap::new();
    let mut references: TableReferencesMap = Default::default();

    for (table_name, table) in &manifest.tables {
        let TableInput::View(View { sql }) = &table.input;

        // Parse SQL (validates single statement)
        let stmt =
            common::sql::parse(sql).map_err(|err| ManifestValidationError::InvalidTableSql {
                table_name: table_name.clone(),
                source: err,
            })?;

        // Extract table references (using DepAliasOrSelfRef to support `self.` inter-table refs)
        let table_refs =
            resolve_table_references::<DepAliasOrSelfRef>(&stmt).map_err(|err| match &err {
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

        // Reject tables whose SQL references no source tables (e.g., `SELECT 1`).
        // Derived tables must reference at least one external dependency or sibling table.
        if table_refs.is_empty() {
            return Err(ManifestValidationError::NoTableReferences {
                table_name: table_name.clone(),
            });
        }

        // Validate dependency aliases in table references before catalog creation.
        // Skip `self` schema refs — those are inter-table references, not external deps.
        for table_ref in &table_refs {
            if let TableReference::Partial { schema, .. } = table_ref
                && let DepAliasOrSelfRef::DepAlias(dep_alias) = schema.as_ref()
                && !dependencies.contains_key(dep_alias)
            {
                return Err(ManifestValidationError::DependencyAliasNotFound {
                    table_name: table_name.clone(),
                    alias: dep_alias.to_string(),
                });
            }
        }

        // Extract function references (supports both external deps and self-references)
        let func_refs = resolve_function_references::<DepAliasOrSelfRef>(&stmt).map_err(|err| {
            ManifestValidationError::FunctionReferenceResolution {
                table_name: table_name.clone(),
                source: err,
            }
        })?;

        // Validate dependency aliases in function references before catalog creation
        for func_ref in &func_refs {
            if let FunctionReference::Qualified { schema, .. } = func_ref
                && let DepAliasOrSelfRef::DepAlias(dep_alias) = schema.as_ref()
                && !dependencies.contains_key(dep_alias)
            {
                return Err(ManifestValidationError::DependencyAliasNotFound {
                    table_name: table_name.clone(),
                    alias: dep_alias.to_string(),
                });
            }
        }

        references.insert(table_name.clone(), (table_refs, func_refs));
        statements.insert(table_name.clone(), stmt);
    }

    // Step 2b: Extract inter-table dependencies and determine processing order.
    // `self.`-qualified table references that match sibling table names are inter-table deps.
    let known_tables: BTreeSet<TableName> = manifest.tables.keys().cloned().collect();
    let table_order = resolve_inter_table_order(
        references
            .iter()
            .map(|(name, (refs, _))| (name, refs.as_slice())),
        &known_tables,
    )
    .map_err(ManifestValidationError::InterTableDep)?;

    // Step 3: Create planning context to validate all table and function references.
    // Inter-table references use `self.<table_name>` syntax, which resolves through the
    // SelfSchemaProvider registered under the "self" schema in the AmpCatalogProvider.
    let session_config =
        default_session_config().map_err(ManifestValidationError::SessionConfig)?;
    let dep_aliases: BTreeMap<String, HashReference> = dependencies
        .iter()
        .map(|(alias, hash_ref)| (alias.to_string(), hash_ref.clone()))
        .collect();
    let self_schema_provider = Arc::new(SelfSchemaProvider::from_manifest_udfs(
        IsolatePool::dummy(),
        &manifest.functions,
    ));
    let amp_catalog = Arc::new(
        AmpCatalogProvider::new(
            datasets_cache.clone(),
            ethcall_udfs_cache.clone(),
            IsolatePool::dummy(),
        )
        .with_dep_aliases(dep_aliases)
        .with_self_schema(self_schema_provider.clone() as Arc<dyn AsyncSchemaProvider>),
    );
    let planning_ctx = PlanContextBuilder::new(session_config)
        .with_table_catalog(AMP_CATALOG_NAME, amp_catalog.clone())
        .with_func_catalog(AMP_CATALOG_NAME, amp_catalog)
        .build();

    // Step 4: Validate that all table SQL queries are incremental, in topological order.
    // After validating each table, register its manifest schema so subsequent tables
    // can reference it via `self.<table_name>`.
    for table_name in table_order {
        // topological_sort only returns keys from the input map
        let stmt = statements
            .remove(&table_name)
            .expect("topological_sort returned unknown table");

        // Plan the SQL query to a logical plan
        let plan = planning_ctx.statement_to_plan(stmt).await.map_err(|err| {
            ManifestValidationError::SqlPlanningError {
                table_name: table_name.clone(),
                source: err,
            }
        })?;

        // Validate that the plan can be processed incrementally
        plan.is_incremental()
            .map_err(|err| ManifestValidationError::NonIncrementalSql {
                table_name: table_name.clone(),
                source: err,
            })?;

        // Register the table's manifest schema so subsequent tables can reference it.
        // table_name comes from the manifest's own tables
        let table = manifest
            .tables
            .get(&table_name)
            .expect("manifest table missing after topological sort");
        let schema = table.schema.arrow.clone().into_schema_ref();
        self_schema_provider.add_table(table_name.as_str(), schema);
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
        source: ResolveTableReferencesError<DepAliasOrSelfRefError>,
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
        source: ResolveTableReferencesError<DepAliasOrSelfRefError>,
    },

    /// Invalid table name
    ///
    /// Table name does not conform to SQL identifier rules (must start with letter/underscore,
    /// contain only alphanumeric/underscore/dollar, and be <= 63 bytes).
    #[error("Invalid table name in SQL query: {0}")]
    InvalidTableName(#[source] ResolveTableReferencesError<DepAliasOrSelfRefError>),

    /// Dependency alias not found
    ///
    /// A table or function reference uses an alias that was not provided in the dependencies map.
    #[error(
        "Dependency alias not found: In table '{table_name}': Dependency alias '{alias}' referenced in table but not provided in dependencies"
    )]
    DependencyAliasNotFound {
        /// The table being processed when the error occurred
        table_name: TableName,
        /// The dependency alias that was not found
        alias: String,
    },

    /// Inter-table dependency validation failed
    ///
    /// This occurs when validating `self.`-qualified table references within the
    /// dataset: self-referencing tables, nonexistent sibling targets, or cyclic deps.
    #[error("Inter-table dependency error")]
    InterTableDep(#[source] InterTableDepError),

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
        source: datafusion::error::DataFusionError,
    },

    /// Failed to create DataFusion session configuration
    #[error("failed to create session config")]
    SessionConfig(#[source] datafusion::error::DataFusionError),
}

/// Registers all files in a revision with their Amp-specific metadata.
///
/// Lists all files in the revision directory in object storage, extracts
/// Parquet metadata including Amp-specific block range information, and
/// registers each file in the metadata database.
///
/// Files are processed concurrently (up to 16 at a time).
#[tracing::instrument(skip_all, err)]
pub async fn register_revision_files(
    store: &DataStore,
    revision: &PhyTableRevision,
) -> Result<i32, RegisterRevisionFilesError> {
    let files = store
        .list_revision_files_in_object_store(revision)
        .await
        .map_err(RegisterRevisionFilesError::ListFiles)?;
    let total_files = files.len();

    // Process files in parallel using buffered stream
    const CONCURRENT_METADATA_FETCHES: usize = 16;

    let object_store = store.clone();
    let mut file_stream = stream::iter(files.into_iter())
        .map(|object_meta| {
            let store = object_store.clone();
            async move {
                let (file_name, amp_meta, footer) =
                    amp_metadata_from_parquet_file(&store, &object_meta)
                        .await
                        .map_err(RegisterRevisionFilesError::ReadParquetMetadata)?;

                let parquet_meta_json = serde_json::to_value(amp_meta)
                    .map_err(RegisterRevisionFilesError::SerializeMetadata)?;

                let object_size = object_meta.size;
                let object_e_tag = object_meta.e_tag;
                let object_version = object_meta.version;

                Ok((
                    file_name,
                    object_size,
                    object_e_tag,
                    object_version,
                    parquet_meta_json,
                    footer,
                ))
            }
        })
        .buffered(CONCURRENT_METADATA_FETCHES);

    // Register all files in the metadata database as they complete
    while let Some(result) = file_stream.next().await {
        let (file_name, object_size, object_e_tag, object_version, parquet_meta_json, footer) =
            result?;
        store
            .register_revision_file(
                revision,
                &file_name,
                object_size,
                object_e_tag,
                object_version,
                parquet_meta_json,
                &footer,
            )
            .await
            .map_err(RegisterRevisionFilesError::RegisterFile)?;
    }

    Ok(total_files as i32)
}

/// Errors that occur when registering revision files
///
/// This error type is used by [`register_revision_files`].
#[derive(Debug, thiserror::Error)]
pub enum RegisterRevisionFilesError {
    /// Failed to list files in the revision directory
    ///
    /// This occurs when:
    /// - The object storage path for the revision is inaccessible
    /// - Network or permission errors when listing objects
    /// - The revision directory does not exist in object storage
    #[error("Failed to list files in revision")]
    ListFiles(#[source] amp_data_store::ListRevisionFilesInObjectStoreError),

    /// Failed to read Amp metadata from parquet file
    ///
    /// This occurs when extracting Amp-specific metadata from a Parquet file fails.
    /// Common causes include:
    /// - Corrupted or invalid Parquet file structure
    /// - Missing required metadata keys in the file
    /// - Incompatible metadata schema version
    /// - I/O errors reading from object store
    /// - JSON parsing failures in metadata values
    ///
    /// See `AmpMetadataFromParquetError` for specific error details.
    #[error("Failed to read Amp metadata from parquet file")]
    ReadParquetMetadata(#[source] AmpMetadataFromParquetError),

    /// Failed to serialize parquet metadata to JSON
    ///
    /// This occurs when:
    /// - The extracted Amp metadata cannot be represented as valid JSON
    /// - Serialization encounters unsupported types or values
    #[error("Failed to serialize parquet metadata to JSON")]
    SerializeMetadata(#[source] serde_json::Error),

    /// Failed to register file in metadata database
    ///
    /// This occurs when:
    /// - Database connection or transaction errors during file registration
    /// - Constraint violations when inserting file metadata
    /// - Concurrent modification conflicts
    #[error("Failed to register file in metadata database")]
    RegisterFile(#[source] amp_data_store::RegisterFileError),
}
