use std::{collections::BTreeMap, sync::Arc};

use datafusion::{
    logical_expr::{ScalarUDF, async_udf::AsyncScalarUDF},
    sql::parser,
};
use datasets_common::{
    block_num::BlockNum, dataset::Table, dataset_kind_str::DatasetKindStr,
    hash_reference::HashReference, table_name::TableName,
};
use js_runtime::{isolate_pool::IsolatePool, js_udf::JsUdf};

use crate::{
    DerivedDatasetKind, Manifest,
    deps::{DepAlias, DepReference},
    function::{Function, FunctionSource},
    manifest::TableInput,
    sql::{ResolveTableReferencesError, TableReference, resolve_table_references},
};

/// Convert a derived dataset manifest into a logical dataset representation.
///
/// This function transforms a derived dataset manifest with its tables, functions, and metadata
/// into the internal `Dataset` structure used by the query engine. Dataset identity (namespace,
/// name, version, hash reference) must be provided externally as they are not part of the manifest.
pub fn dataset(reference: HashReference, manifest: Manifest) -> Result<Dataset, DatasetError> {
    let queries = {
        let mut queries = BTreeMap::new();
        for (table_name, table) in &manifest.tables {
            let TableInput::View(query) = &table.input;
            let query = crate::sql::parse(&query.sql).map_err(|err| DatasetError::ParseSql {
                table_name: table_name.clone(),
                source: err,
            })?;
            queries.insert(table_name.clone(), query);
        }
        queries
    };

    // Convert manifest tables into logical tables
    let unsorted_tables: Vec<Table> = manifest
        .tables
        .into_iter()
        .map(|(name, table)| Table::new(name, table.schema.arrow.into(), table.network, vec![]))
        .collect();
    let tables = sort_tables_by_dependencies(unsorted_tables, &queries)
        .map_err(DatasetError::SortTableDependencies)?;

    // Convert manifest functions into logical functions
    let functions = manifest
        .functions
        .into_iter()
        .map(|(name, f)| Function {
            name: name.into_inner(),
            input_types: f.input_types.into_iter().map(|dt| dt.0).collect(),
            output_type: f.output_type.0,
            source: FunctionSource {
                source: f.source.source,
                filename: f.source.filename,
            },
        })
        .collect();

    Ok(Dataset::new(
        reference,
        manifest.dependencies,
        DerivedDatasetKind,
        false,
        tables,
        functions,
    ))
}

/// A [`Dataset`](datasets_common::dataset::Dataset) built from SQL transformations over other datasets.
///
/// Tables are stored in dependency order (topologically sorted) so that each table
/// appears after all tables it references in its SQL query.
pub struct Dataset {
    reference: HashReference,
    kind: DerivedDatasetKind,
    dependencies: BTreeMap<DepAlias, DepReference>,
    tables: Vec<Table>,
    functions: Vec<Function>,
    finalized_blocks_only: bool,
}

impl Dataset {
    /// Creates a new Dataset instance.
    pub fn new(
        reference: HashReference,
        dependencies: BTreeMap<DepAlias, DepReference>,
        kind: DerivedDatasetKind,
        finalized_blocks_only: bool,
        tables: Vec<Table>,
        functions: Vec<Function>,
    ) -> Self {
        Self {
            reference,
            dependencies,
            kind,
            finalized_blocks_only,
            tables,
            functions,
        }
    }

    /// Returns the dependencies of this derived dataset.
    ///
    /// The map keys are aliases used to reference dependencies in SQL queries,
    /// and values are references to the dependent datasets.
    pub fn dependencies(&self) -> &BTreeMap<DepAlias, DepReference> {
        &self.dependencies
    }

    /// Looks up a user-defined function by name.
    ///
    /// Returns the [`ScalarUDF`] for the function if found. This is used
    /// for derived datasets that define custom JavaScript functions.
    ///
    /// Returns `None` if the function name is not found.
    pub fn function_by_name(
        &self,
        schema: String,
        name: &str,
        isolate_pool: IsolatePool,
    ) -> Option<ScalarUDF> {
        self.functions.iter().find(|f| f.name == name).map(|f| {
            AsyncScalarUDF::new(Arc::new(JsUdf::new(
                isolate_pool,
                schema,
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

impl datasets_common::dataset::Dataset for Dataset {
    fn reference(&self) -> &HashReference {
        &self.reference
    }

    fn kind(&self) -> DatasetKindStr {
        self.kind.into()
    }

    fn tables(&self) -> &[Table] {
        &self.tables
    }

    fn start_block(&self) -> Option<BlockNum> {
        None
    }

    fn finalized_blocks_only(&self) -> bool {
        self.finalized_blocks_only
    }
}

/// Errors that occur when converting a derived dataset manifest to a logical dataset
///
/// This error type is used by the `dataset()` function.
#[derive(Debug, thiserror::Error)]
pub enum DatasetError {
    /// Failed to parse SQL query in table definition
    #[error("Failed to parse SQL query for table '{table_name}'")]
    ParseSql {
        table_name: TableName,
        #[source]
        source: crate::sql::ParseSqlError,
    },

    /// Failed to sort tables by their SQL dependencies
    #[error("Failed to sort tables by dependencies")]
    SortTableDependencies(#[source] SortTablesByDependenciesError),
}

/// Sort tables by their SQL dependencies using topological ordering.
///
/// Analyzes table queries to determine dependencies and returns tables in dependency order.
/// Tables with no dependencies come first, followed by tables that depend on them.
pub fn sort_tables_by_dependencies(
    tables: Vec<Table>,
    queries: &BTreeMap<TableName, parser::Statement>,
) -> Result<Vec<Table>, SortTablesByDependenciesError> {
    // Map of table name -> Table
    let table_map = tables
        .into_iter()
        .map(|t| (t.name().clone(), t))
        .collect::<BTreeMap<TableName, Table>>();

    // Dependency map: table -> [tables it depends on]
    let mut deps: BTreeMap<TableName, Vec<TableName>> = BTreeMap::new();

    // Initialize empty deps with all tables
    for table_name in table_map.keys() {
        deps.insert(table_name.clone(), Vec::new());
    }

    for (table_name, query) in queries {
        let table_refs = resolve_table_references::<String>(query).map_err(|err| {
            SortTablesByDependenciesError::ResolveTableReferences {
                table_name: table_name.clone(),
                source: err,
            }
        })?;

        // Filter to only include dependencies within the same dataset
        let mut table_deps: Vec<TableName> = vec![];
        for table_ref in table_refs {
            match &table_ref {
                TableReference::Bare { table } if table.as_ref() != table_name => {
                    // Unqualified reference is assumed to be to a table in the same dataset
                    if table_map.contains_key(table.as_ref()) {
                        table_deps.push(table.as_ref().clone());
                    }
                }
                _ => {
                    // Reference to external dataset or self-reference, ignore
                }
            }
        }

        // Update the existing entry with dependencies
        if let Some(existing_deps) = deps.get_mut(table_name) {
            *existing_deps = table_deps;
        }
    }

    let sorted_names = crate::sorting::topological_sort(deps)
        .map_err(SortTablesByDependenciesError::TopologicalSort)?;

    let mut sorted_tables = Vec::new();
    for name in sorted_names {
        if let Some(table) = table_map.get(&name) {
            sorted_tables.push(table.clone());
        }
    }

    Ok(sorted_tables)
}

/// Errors that occur when sorting tables by their SQL dependencies
#[derive(Debug, thiserror::Error)]
pub enum SortTablesByDependenciesError {
    /// Failed to resolve table references from SQL query
    #[error("Failed to resolve table references in table '{table_name}': {source}")]
    ResolveTableReferences {
        table_name: TableName,
        #[source]
        source: ResolveTableReferencesError,
    },

    /// Failed to perform topological sort on table dependencies
    #[error("Failed to perform topological sort on table dependencies")]
    TopologicalSort(#[source] crate::sorting::CyclicDepError<TableName>),
}
