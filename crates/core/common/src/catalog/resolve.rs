//! Core name resolution for SQL catalogs.
//!
//! This module provides a unified implementation for resolving table and function
//! references from SQL queries into logical catalogs. It abstracts over different
//! schema resolution strategies (dynamic vs pre-resolved) via the [`SchemaResolver`] trait.
//!
//! # Schema Resolvers
//!
//! Two resolver implementations are provided:
//!
//! - [`DynamicResolver`]: Resolves dataset references dynamically via a store.
//!   Used for user queries where datasets are referenced by name/version.
//!
//! - For pre-resolved dependencies (derived datasets), see the `datasets-derived` crate
//!   which provides its own resolver implementation.

use std::{
    collections::{BTreeMap, btree_map::Entry},
    sync::Arc,
};

use async_trait::async_trait;
use datafusion::logical_expr::{ScalarUDF, async_udf::AsyncScalarUDF};
use datasets_common::{
    func_name::{ETH_CALL_FUNCTION_NAME, FuncName},
    hash::Hash,
    hash_reference::HashReference,
    partial_reference::{PartialReference, PartialReferenceError},
    reference::Reference,
    table_name::TableName,
};
use js_runtime::isolate_pool::IsolatePool;

use super::{
    dataset_access::DatasetAccess,
    logical::{Dataset, Function, LogicalCatalog},
};
use crate::{BoxError, ResolvedTable, js_udf::JsUdf, sql::TableReference};

/// The schema name used for self-references in derived datasets.
pub const SELF_SCHEMA: &str = "self";

// ============================================================================
// Self References
// ============================================================================

/// Functions available for self-reference resolution in derived datasets.
///
/// This type contains the functions needed to resolve `self.function_name()` references
/// in SQL queries. It can be created from a `Dataset` or directly from function definitions.
#[derive(Clone)]
pub struct SelfReferences {
    functions: Vec<Function>,
}

impl SelfReferences {
    /// Creates a new `SelfReferences` from a list of functions.
    pub fn new(functions: Vec<Function>) -> Self {
        Self { functions }
    }

    /// Creates an empty `SelfReferences` with no functions.
    pub fn empty() -> Self {
        Self {
            functions: Vec::new(),
        }
    }

    /// Returns true if there are no functions available for self-reference.
    pub fn is_empty(&self) -> bool {
        self.functions.is_empty()
    }

    /// Returns a specific JS function by name.
    ///
    /// This implements lazy loading by only instantiating the requested function.
    pub fn function_by_name(&self, name: &str, isolate_pool: IsolatePool) -> Option<ScalarUDF> {
        self.functions.iter().find(|f| f.name == name).map(|f| {
            AsyncScalarUDF::new(Arc::new(JsUdf::new(
                isolate_pool,
                SELF_SCHEMA.to_string(),
                f.source.source.clone(),
                f.source.filename.clone().into(),
                f.name.clone().into(),
                f.input_types.clone(),
                f.output_type.clone(),
            )))
            .into_scalar_udf()
        })
    }
}

impl From<&Dataset> for SelfReferences {
    fn from(dataset: &Dataset) -> Self {
        Self {
            functions: dataset.functions.clone(),
        }
    }
}

impl From<&Arc<Dataset>> for SelfReferences {
    fn from(dataset: &Arc<Dataset>) -> Self {
        Self {
            functions: dataset.functions.clone(),
        }
    }
}

/// Trait for resolving schema strings to hash references.
///
/// This trait abstracts over different resolution strategies:
/// - **Dynamic resolution**: Resolve dataset names/versions via a store (for user queries)
/// - **Pre-resolved**: Lookup aliases in a dependencies map (for derived datasets)
#[async_trait]
pub trait SchemaResolver: Send + Sync {
    /// Error type for resolution failures.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Resolve a schema string to a [`HashReference`].
    ///
    /// The schema string format depends on the resolver implementation:
    /// - For dynamic resolution: `"namespace/name"` or `"namespace/name@version"`
    /// - For pre-resolved: dependency alias like `"eth"` or `"uniswap"`
    async fn resolve(&self, schema: &str) -> Result<HashReference, Self::Error>;
}

// ============================================================================
// Dynamic Resolver
// ============================================================================

/// Resolves schema strings dynamically via a dataset store.
///
/// This resolver is used for user queries where datasets are referenced by
/// name and optionally version (e.g., `"namespace/dataset"` or `"namespace/dataset@v1.0.0"`).
///
/// The schema string is parsed as a [`PartialReference`] and then resolved
/// via the store's `resolve_revision` method.
pub struct DynamicResolver<'a, S> {
    store: &'a S,
}

impl<'a, S> DynamicResolver<'a, S> {
    /// Creates a new dynamic resolver using the given store.
    pub fn new(store: &'a S) -> Self {
        Self { store }
    }
}

#[async_trait]
impl<S> SchemaResolver for DynamicResolver<'_, S>
where
    S: DatasetAccess + Sync,
{
    type Error = DynamicResolveError;

    async fn resolve(&self, schema: &str) -> Result<HashReference, Self::Error> {
        let partial: PartialReference = schema
            .parse()
            .map_err(DynamicResolveError::InvalidReference)?;
        let reference: Reference = partial.into();
        self.store
            .resolve_revision(&reference)
            .await
            .map_err(DynamicResolveError::StoreError)?
            .ok_or_else(|| DynamicResolveError::NotFound(reference))
    }
}

/// Errors from dynamic schema resolution.
#[derive(Debug, thiserror::Error)]
pub enum DynamicResolveError {
    /// Schema string could not be parsed as a valid reference.
    #[error("invalid reference format: {0}")]
    InvalidReference(#[source] PartialReferenceError),

    /// Store returned an error during resolution.
    #[error("store error: {0}")]
    StoreError(#[source] BoxError),

    /// Dataset was not found.
    #[error("dataset not found: {0}")]
    NotFound(Reference),
}

// ============================================================================
// Resolve Errors
// ============================================================================

/// Errors that can occur during catalog resolution.
#[derive(Debug, thiserror::Error)]
pub enum ResolveError<SE> {
    /// Schema resolution failed.
    #[error("schema resolution failed: {0}")]
    SchemaResolution(SE),

    /// Failed to load dataset from store.
    #[error("failed to load dataset {hash}")]
    DatasetLoad {
        hash: HashReference,
        #[source]
        source: BoxError,
    },

    /// Table not found in dataset.
    #[error("table '{table}' not found in dataset {hash}")]
    TableNotFound {
        table: TableName,
        hash: HashReference,
    },

    /// Function not found in dataset.
    #[error("function '{schema}.{function}' not found in dataset {hash}")]
    FunctionNotFound {
        schema: String,
        function: FuncName,
        hash: HashReference,
    },

    /// Self-referenced function not found in self dataset.
    #[error("self-referenced function '{function}' not found in self dataset")]
    SelfFunctionNotFound { function: FuncName },

    /// Self-reference used but no self dataset provided.
    #[error("self-reference to function '{function}' but no self dataset provided")]
    NoSelfDataset { function: FuncName },

    /// eth_call function not available for dataset.
    #[error("eth_call not available for dataset {hash}")]
    EthCallNotAvailable { hash: HashReference },

    /// Failed to create eth_call UDF.
    #[error("failed to create eth_call UDF for dataset {hash}")]
    EthCallCreation {
        hash: HashReference,
        #[source]
        source: BoxError,
    },
}

/// Resolves table and function references into a logical catalog.
///
/// This is the core resolution function that handles:
/// - Table resolution: schema + table name → dataset → table schema
/// - Function resolution: schema + function name → dataset → UDF
/// - Self-function resolution: `self.function` → self_dataset → UDF
/// - Deduplication: Multiple references to the same table/function share one instance
///
/// # Parameters
///
/// - `store`: Dataset access for loading datasets
/// - `resolver`: Schema resolver for converting schema strings to hash references
/// - `table_refs`: Iterator of (schema, table_name) tuples for qualified table references
/// - `func_refs`: Iterator of (schema?, function_name) tuples. `None` schema means bare
///   function (built-in DataFusion function)
/// - `self_refs`: Functions for resolving `self.function` references (for derived datasets)
/// - `isolate_pool`: JavaScript isolate pool for creating UDFs
///
/// # Errors
///
/// Returns [`ResolveError`] if schema resolution, dataset loading, or entity lookup fails.
pub async fn resolve_logical_catalog<R>(
    store: &impl DatasetAccess,
    resolver: &R,
    table_refs: impl IntoIterator<Item = (String, TableName)>,
    func_refs: impl IntoIterator<Item = (Option<String>, FuncName)>,
    self_refs: SelfReferences,
    isolate_pool: &IsolatePool,
) -> Result<LogicalCatalog, ResolveError<R::Error>>
where
    R: SchemaResolver,
{
    // Use hash-based map to deduplicate datasets and collect resolved tables
    // Outer key: dataset hash, Inner key: "schema.table" string
    let mut tables: BTreeMap<Hash, BTreeMap<String, ResolvedTable>> = BTreeMap::new();
    // Track UDFs from external datasets
    // Outer key: dataset hash, Inner key: "schema.function" string
    let mut udfs: BTreeMap<Hash, BTreeMap<String, ScalarUDF>> = BTreeMap::new();
    // Track self-referenced UDFs separately (deduplicated by function name)
    let mut self_udfs: BTreeMap<FuncName, ScalarUDF> = BTreeMap::new();

    // === Table Resolution ===
    for (schema, table) in table_refs {
        let hash_ref = resolver
            .resolve(&schema)
            .await
            .map_err(ResolveError::SchemaResolution)?;

        let key = format!("{}.{}", schema, table);

        // Skip if already resolved (deduplication)
        if let Entry::Occupied(_) = tables
            .entry(hash_ref.hash().clone())
            .or_default()
            .entry(key.clone())
        {
            continue;
        }

        let dataset =
            store
                .get_dataset(&hash_ref)
                .await
                .map_err(|e| ResolveError::DatasetLoad {
                    hash: hash_ref.clone(),
                    source: e,
                })?;

        let dataset_table = dataset
            .tables
            .iter()
            .find(|t| t.name() == &table)
            .ok_or_else(|| ResolveError::TableNotFound {
                table: table.clone(),
                hash: hash_ref.clone(),
            })?;

        let resolved = ResolvedTable::new(
            TableReference::partial(schema.clone(), table),
            dataset_table.clone(),
            dataset,
        );

        tables
            .entry(hash_ref.hash().clone())
            .or_default()
            .insert(key, resolved);
    }

    // === Function Resolution ===
    for (schema, function) in func_refs {
        match schema {
            // Bare function - skip (built-in DataFusion function)
            None => continue,

            // Self-reference - resolve from self_refs
            Some(ref s) if s == SELF_SCHEMA => {
                // Skip if already resolved (deduplication)
                if self_udfs.contains_key(&function) {
                    continue;
                }

                // Check if self-references are available
                if self_refs.is_empty() {
                    return Err(ResolveError::NoSelfDataset {
                        function: function.clone(),
                    });
                }

                let udf = self_refs
                    .function_by_name(function.as_ref(), isolate_pool.clone())
                    .ok_or_else(|| ResolveError::SelfFunctionNotFound {
                        function: function.clone(),
                    })?;

                self_udfs.insert(function, udf);
            }

            // External dataset reference
            Some(schema) => {
                let hash_ref = resolver
                    .resolve(&schema)
                    .await
                    .map_err(ResolveError::SchemaResolution)?;

                let key = format!("{}.{}", schema, function);

                // Skip if already resolved (deduplication)
                if let Entry::Occupied(_) = udfs
                    .entry(hash_ref.hash().clone())
                    .or_default()
                    .entry(key.clone())
                {
                    continue;
                }

                let dataset =
                    store
                        .get_dataset(&hash_ref)
                        .await
                        .map_err(|e| ResolveError::DatasetLoad {
                            hash: hash_ref.clone(),
                            source: e,
                        })?;

                let udf = if function.as_ref() == ETH_CALL_FUNCTION_NAME {
                    store
                        .eth_call_for_dataset(&schema, &dataset)
                        .await
                        .map_err(|e| ResolveError::EthCallCreation {
                            hash: hash_ref.clone(),
                            source: e,
                        })?
                        .ok_or_else(|| ResolveError::EthCallNotAvailable {
                            hash: hash_ref.clone(),
                        })?
                } else {
                    dataset
                        .function_by_name(schema.clone(), function.as_ref(), isolate_pool.clone())
                        .ok_or_else(|| ResolveError::FunctionNotFound {
                            schema: schema.clone(),
                            function: function.clone(),
                            hash: hash_ref.clone(),
                        })?
                };

                udfs.entry(hash_ref.hash().clone())
                    .or_default()
                    .insert(key, udf);
            }
        }
    }

    Ok(LogicalCatalog {
        tables: tables.into_values().flat_map(|m| m.into_values()).collect(),
        udfs: self_udfs
            .into_values()
            .chain(udfs.into_values().flat_map(|m| m.into_values()))
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    // Unit tests can be added here as needed
}
