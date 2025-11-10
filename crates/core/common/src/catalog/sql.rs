use std::collections::{BTreeMap, btree_map::Entry};

use datafusion::sql::{TableReference, parser::Statement, resolve::resolve_table_references};
use datasets_common::{
    hash::Hash, partial_reference::PartialReference, reference::Reference, revision::Revision,
    table_name::TableName,
};
use js_runtime::isolate_pool::IsolatePool;
use metadata_db::MetadataDb;

use super::{
    dataset_access::DatasetAccess,
    errors::{
        CatalogForSqlError, GetLogicalCatalogError, GetPhysicalCatalogError, PlanningCtxForSqlError,
    },
    logical::LogicalCatalog,
    physical::{Catalog, PhysicalTable},
};
use crate::{BoxError, PlanningContext, ResolvedTable, query_context::QueryEnv};

/// Build catalog from SQL query statement.
///
/// This function analyzes the query to:
/// 1. Collect table references in the query.
/// 2. Assume that in `foo.bar`, `foo` is a dataset name.
/// 3. Look up the dataset names in the configured dataset store.
/// 4. Collect the datasets into a catalog.
pub async fn catalog_for_sql(
    store: &impl DatasetAccess,
    metadata_db: &MetadataDb,
    query: &Statement,
    env: QueryEnv,
) -> Result<Catalog, CatalogForSqlError> {
    let (tables, _) = resolve_table_references(query, true)
        .map_err(|err| CatalogForSqlError::TableReferenceResolution { source: err.into() })?;
    let function_names = all_function_names(query)
        .map_err(|err| CatalogForSqlError::FunctionNameExtraction { source: err })?;

    get_physical_catalog(store, metadata_db, tables, function_names, &env)
        .await
        .map_err(CatalogForSqlError::GetPhysicalCatalog)
}

/// Similar to `catalog_for_sql`, but only for planning and not execution. This does not require a
/// physical location to exist for the dataset views.
pub async fn planning_ctx_for_sql(
    store: &impl DatasetAccess,
    query: &Statement,
) -> Result<PlanningContext, PlanningCtxForSqlError> {
    // Get table and function references from the SQL query
    let (tables, _) = resolve_table_references(query, true)
        .map_err(PlanningCtxForSqlError::TableReferenceResolution)?;
    let function_names =
        all_function_names(query).map_err(PlanningCtxForSqlError::FunctionNameExtraction)?;

    // Use hash-based map to deduplicate datasets and collect resolved tables
    let mut datasets: BTreeMap<Hash, Vec<ResolvedTable>> = BTreeMap::new();
    let mut udfs = Vec::new();

    // Part 1: Process table references
    for table_ref in &tables {
        // Check if table reference is catalog-qualified (not supported)
        if table_ref.catalog().is_some() {
            return Err(PlanningCtxForSqlError::CatalogQualifiedTable {
                table_ref: table_ref.to_string(),
            });
        }

        // Check if schema is present
        let schema_str =
            table_ref
                .schema()
                .ok_or_else(|| PlanningCtxForSqlError::UnqualifiedTable {
                    table_ref: table_ref.to_string(),
                })?;

        // Validate table name is a valid TableName
        let table_name: TableName =
            table_ref
                .table()
                .parse()
                .map_err(|err| PlanningCtxForSqlError::InvalidTableName {
                    table_name: table_ref.table().to_string(),
                    table_ref: table_ref.to_string(),
                    source: err,
                })?;

        // Parse schema to PartialReference -> Reference
        let partial_ref: PartialReference =
            schema_str
                .parse()
                .map_err(|err| PlanningCtxForSqlError::InvalidSchemaReference {
                    schema: schema_str.to_string(),
                    source: err,
                })?;

        let reference: Reference = partial_ref.into();

        // Resolve reference to hash
        let hash = store
            .resolve_dataset_reference(&reference)
            .await
            .map_err(|err| PlanningCtxForSqlError::ResolveHash {
                reference: reference.clone(),
                source: err,
            })?
            .ok_or_else(|| {
                tracing::error!(
                    reference = %reference,
                    "Dataset not found"
                );
                PlanningCtxForSqlError::DatasetNotFound {
                    reference: reference.clone(),
                }
            })?;

        // Use the full reference with hash revision
        let reference = {
            let (namespace, name, _) = reference.into_parts();
            Reference::new(namespace, name, Revision::Hash(hash.clone()))
        };

        // Load dataset by hash (cached by store)
        let dataset = store
            .get_dataset_by_hash(&hash)
            .await
            .map_err(|err| PlanningCtxForSqlError::GetDataset {
                reference: reference.clone(),
                source: err,
            })?
            .ok_or_else(|| PlanningCtxForSqlError::DatasetNotFound {
                reference: reference.clone(),
            })?;

        // Create UDFs if this is the first time seeing this dataset
        let resolved_tables = match datasets.entry(hash) {
            Entry::Vacant(entry) => {
                // Create UDFs for this dataset
                // Use the original schema string from SQL as the UDF prefix
                if let Some(udf) = store
                    .eth_call_for_dataset(schema_str, &dataset)
                    .await
                    .map_err(|err| PlanningCtxForSqlError::EthCallUdfCreation {
                        reference: reference.clone(),
                        source: err,
                    })?
                {
                    udfs.push(udf);
                }

                for udf in dataset.functions(schema_str.to_string(), IsolatePool::dummy()) {
                    udfs.push(udf.into());
                }

                entry.insert(Vec::new())
            }
            Entry::Occupied(entry) => entry.into_mut(),
        };

        // Find table in dataset and create ResolvedTable
        let table = dataset
            .tables
            .iter()
            .find(|t| t.name() == &table_name)
            .ok_or_else(|| PlanningCtxForSqlError::TableNotFoundInDataset {
                table_name,
                reference,
            })?;

        // Use the original schema string from SQL as the schema name
        let table_ref = TableReference::partial(schema_str, table.name().as_str());
        let resolved_table = ResolvedTable::new(table.clone(), dataset.clone(), table_ref);
        resolved_tables.push(resolved_table);
    }

    // Part 2: Process function names (load datasets for UDFs only)
    for func_name in &function_names {
        let parts: Vec<_> = func_name.split('.').collect();
        match parts.as_slice() {
            [_] => continue, // Built-in DataFusion function
            [schema_str, _fn_name] => {
                // Parse schema part to PartialReference -> Reference
                let reference: Reference = schema_str
                    .parse::<PartialReference>()
                    .map_err(|err| PlanningCtxForSqlError::InvalidFunctionReference {
                        function: func_name.clone(),
                        source: err,
                    })?
                    .into();

                // Resolve reference to hash
                let hash = store
                    .resolve_dataset_reference(&reference)
                    .await
                    .map_err(|err| PlanningCtxForSqlError::ResolveHash {
                        reference: reference.clone(),
                        source: err,
                    })?
                    .ok_or_else(|| PlanningCtxForSqlError::DatasetNotFound {
                        reference: reference.clone(),
                    })?;

                // Use the full reference with hash revision
                let reference = {
                    let (namespace, name, _) = reference.into_parts();
                    Reference::new(namespace, name, Revision::Hash(hash.clone()))
                };

                // Load dataset if not already loaded
                if let Entry::Vacant(entry) = datasets.entry(hash) {
                    let dataset = store
                        .get_dataset_by_hash(entry.key())
                        .await
                        .map_err(|err| PlanningCtxForSqlError::GetDataset {
                            reference: reference.clone(),
                            source: err,
                        })?
                        .ok_or_else(|| PlanningCtxForSqlError::DatasetNotFound {
                            reference: reference.clone(),
                        })?;

                    // Create UDFs for this dataset
                    // Use the original schema string from SQL as the UDF prefix
                    if let Some(udf) = store
                        .eth_call_for_dataset(schema_str, &dataset)
                        .await
                        .map_err(|err| PlanningCtxForSqlError::EthCallUdfCreation {
                            reference: reference.clone(),
                            source: err,
                        })?
                    {
                        udfs.push(udf);
                    }

                    for udf in dataset.functions(schema_str.to_string(), IsolatePool::dummy()) {
                        udfs.push(udf.into());
                    }

                    entry.insert(Vec::new());
                }
            }
            _ => {
                return Err(PlanningCtxForSqlError::InvalidFunctionFormat {
                    function: func_name.clone(),
                });
            }
        }
    }

    // Flatten to Vec<ResolvedTable>

    Ok(PlanningContext::new(LogicalCatalog {
        tables: datasets.into_values().flatten().collect(),
        udfs,
    }))
}

/// Looks up the datasets for the given table references and gets them into a catalog.
async fn get_physical_catalog(
    store: &impl DatasetAccess,
    metadata_db: &MetadataDb,
    table_refs: impl IntoIterator<Item = TableReference>,
    function_names: impl IntoIterator<Item = String>,
    env: &QueryEnv,
) -> Result<Catalog, GetPhysicalCatalogError> {
    let logical_catalog = get_logical_catalog(store, table_refs, function_names, &env.isolate_pool)
        .await
        .map_err(GetPhysicalCatalogError::GetLogicalCatalog)?;

    let mut tables = Vec::new();
    for table in &logical_catalog.tables {
        let physical_table = PhysicalTable::get_active(table, metadata_db.clone())
            .await
            .map_err(|err| GetPhysicalCatalogError::PhysicalTableRetrieval {
                table: table.to_string(),
                source: err,
            })?
            .ok_or(GetPhysicalCatalogError::TableNotSynced {
                table: table.to_string(),
            })?;
        tables.push(physical_table.into());
    }
    Ok(Catalog::new(tables, logical_catalog))
}

/// Looks up the datasets for the given table references and creates resolved tables. Create
/// UDFs specific to the referenced datasets.
async fn get_logical_catalog(
    store: &impl DatasetAccess,
    table_refs: impl IntoIterator<Item = TableReference>,
    function_names: impl IntoIterator<Item = String>,
    isolate_pool: &IsolatePool,
) -> Result<LogicalCatalog, GetLogicalCatalogError> {
    let table_refs = table_refs.into_iter().collect::<Vec<_>>();
    let function_names = function_names.into_iter().collect::<Vec<_>>();

    // Use hash-based map to deduplicate datasets and collect resolved tables
    let mut datasets: BTreeMap<Hash, Vec<ResolvedTable>> = BTreeMap::new();
    let mut udfs = Vec::new();

    // Part 1: Process table references
    for table_ref in &table_refs {
        // Check if table reference is catalog-qualified (not supported)
        if table_ref.catalog().is_some() {
            return Err(GetLogicalCatalogError::CatalogQualifiedTable {
                table_ref: table_ref.to_string(),
            });
        }

        // Check if schema is present
        let schema_str =
            table_ref
                .schema()
                .ok_or_else(|| GetLogicalCatalogError::UnqualifiedTable {
                    table_ref: table_ref.to_string(),
                })?;

        // Validate table name is a valid TableName
        let table_name: TableName =
            table_ref
                .table()
                .parse()
                .map_err(|err| GetLogicalCatalogError::InvalidTableName {
                    table_name: table_ref.table().to_string(),
                    table_ref: table_ref.to_string(),
                    source: err,
                })?;

        // Parse schema to PartialReference -> Reference
        let partial_ref: PartialReference =
            schema_str
                .parse()
                .map_err(|err| GetLogicalCatalogError::InvalidSchemaReference {
                    schema: schema_str.to_string(),
                    source: err,
                })?;

        let reference: Reference = partial_ref.into();

        // Resolve reference to hash
        let hash = store
            .resolve_dataset_reference(&reference)
            .await
            .map_err(|err| GetLogicalCatalogError::ResolveHash {
                reference: reference.clone(),
                source: err,
            })?
            .ok_or_else(|| {
                tracing::error!(
                    reference = %reference,
                    "Dataset not found"
                );
                GetLogicalCatalogError::DatasetNotFound {
                    reference: reference.clone(),
                }
            })?;

        // Use the full reference with hash revision
        let reference = {
            let (namespace, name, _) = reference.into_parts();
            Reference::new(namespace, name, Revision::Hash(hash.clone()))
        };

        // Load dataset by hash (cached by store)
        let dataset = store
            .get_dataset_by_hash(&hash)
            .await
            .map_err(|err| GetLogicalCatalogError::GetDataset {
                reference: reference.clone(),
                source: err,
            })?
            .ok_or_else(|| GetLogicalCatalogError::DatasetNotFound {
                reference: reference.clone(),
            })?;

        // Create UDFs if this is the first time seeing this dataset
        let resolved_tables = match datasets.entry(hash) {
            Entry::Vacant(entry) => {
                // Create UDFs for this dataset
                // Use the original schema string from SQL as the UDF prefix
                if let Some(udf) = store
                    .eth_call_for_dataset(schema_str, &dataset)
                    .await
                    .map_err(|err| GetLogicalCatalogError::EthCallUdfCreation {
                        reference: reference.clone(),
                        source: err,
                    })?
                {
                    udfs.push(udf);
                }

                for udf in dataset.functions(schema_str.to_string(), isolate_pool.clone()) {
                    udfs.push(udf.into());
                }

                entry.insert(Vec::new())
            }
            Entry::Occupied(entry) => entry.into_mut(),
        };

        // Find table in dataset and create ResolvedTable
        if let Some(table) = dataset.tables.iter().find(|t| t.name() == &table_name) {
            // Use the original schema string from SQL as the schema name
            let table_ref = TableReference::partial(schema_str, table.name().as_str());
            let resolved_table = ResolvedTable::new(table.clone(), dataset.clone(), table_ref);
            resolved_tables.push(resolved_table);
        }
    }

    // Part 2: Process function names (load datasets for UDFs only)
    for func_name in &function_names {
        let parts: Vec<_> = func_name.split('.').collect();
        match parts.as_slice() {
            [_] => continue, // Built-in DataFusion function
            [schema_str, _fn_name] => {
                // Parse schems part to PartialReference -> Reference
                let reference: Reference = schema_str
                    .parse::<PartialReference>()
                    .map_err(|err| GetLogicalCatalogError::InvalidFunctionReference {
                        function: func_name.clone(),
                        source: err,
                    })?
                    .into();

                // Resolve reference to hash
                let hash = store
                    .resolve_dataset_reference(&reference)
                    .await
                    .map_err(|err| GetLogicalCatalogError::ResolveHash {
                        reference: reference.clone(),
                        source: err,
                    })?
                    .ok_or_else(|| GetLogicalCatalogError::DatasetNotFound {
                        reference: reference.clone(),
                    })?;

                // Use the full reference with hash revision
                let reference = {
                    let (namespace, name, _) = reference.into_parts();
                    Reference::new(namespace, name, Revision::Hash(hash.clone()))
                };

                // Load dataset if not already loaded
                if let Entry::Vacant(entry) = datasets.entry(hash) {
                    let dataset = store
                        .get_dataset_by_hash(entry.key())
                        .await
                        .map_err(|err| GetLogicalCatalogError::GetDataset {
                            reference: reference.clone(),
                            source: err,
                        })?
                        .ok_or_else(|| GetLogicalCatalogError::DatasetNotFound {
                            reference: reference.clone(),
                        })?;

                    // Create UDFs for this dataset
                    // Use the original schema string from SQL as the UDF prefix
                    if let Some(udf) = store
                        .eth_call_for_dataset(schema_str, &dataset)
                        .await
                        .map_err(|err| GetLogicalCatalogError::EthCallUdfCreation {
                            reference: reference.clone(),
                            source: err,
                        })?
                    {
                        udfs.push(udf);
                    }

                    for udf in dataset.functions(schema_str.to_string(), isolate_pool.clone()) {
                        udfs.push(udf.into());
                    }

                    entry.insert(Vec::new());
                }
            }
            _ => {
                return Err(GetLogicalCatalogError::InvalidFunctionFormat {
                    function: func_name.clone(),
                });
            }
        }
    }

    // Flatten to Vec<ResolvedTable>
    let resolved_tables: Vec<ResolvedTable> = datasets.into_values().flatten().collect();

    Ok(LogicalCatalog {
        tables: resolved_tables,
        udfs,
    })
}

/// Returns a list of all function names in the SQL statement.
///
/// Errors in case of some DML statements.
fn all_function_names(stmt: &Statement) -> Result<Vec<String>, BoxError> {
    use std::ops::ControlFlow;

    use datafusion::sql::sqlparser::ast::{Expr, Function, ObjectNamePart, Visit, Visitor};
    use itertools::Itertools;

    struct FunctionCollector {
        functions: Vec<Function>,
    }

    impl Visitor for FunctionCollector {
        type Break = ();

        fn pre_visit_expr(&mut self, function: &Expr) -> ControlFlow<()> {
            if let Expr::Function(f) = function {
                self.functions.push(f.clone());
            }
            ControlFlow::Continue(())
        }
    }

    let mut collector = FunctionCollector {
        functions: Vec::new(),
    };
    let stmt = match stmt {
        Statement::Statement(statement) => statement,
        Statement::CreateExternalTable(_) | Statement::CopyTo(_) => {
            return Err("DML not supported".into());
        }
        Statement::Explain(explain) => match explain.statement.as_ref() {
            Statement::Statement(statement) => statement,
            _ => return Err("unsupported statement in EXPLAIN".into()),
        },
    };

    let c = stmt.visit(&mut collector);
    assert!(c.is_continue());

    Ok(collector
        .functions
        .into_iter()
        .map(|f| {
            f.name
                .0
                .into_iter()
                .filter_map(|s| match s {
                    ObjectNamePart::Identifier(ident) => Some(ident.value),
                    ObjectNamePart::Function(_) => None,
                })
                .join(".")
        })
        .collect())
}
