use std::collections::BTreeSet;

use datafusion::sql::{TableReference, parser::Statement, resolve::resolve_table_references};
use datasets_common::partial_reference::PartialReference;
use js_runtime::isolate_pool::IsolatePool;
use metadata_db::MetadataDb;

use super::{
    dataset_access::DatasetAccess,
    errors::{
        CatalogForSqlError, ExtractDatasetFromFunctionNamesError, ExtractDatasetFromTableRefsError,
        GetLogicalCatalogError, GetPhysicalCatalogError, PlanningCtxForSqlError,
    },
    logical::LogicalCatalog,
    physical::{Catalog, PhysicalTable},
};
use crate::{BoxError, PlanningContext, query_context::QueryEnv};

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
    let (tables, _) = resolve_table_references(query, true)
        .map_err(|err| PlanningCtxForSqlError::TableReferenceResolution { source: err.into() })?;
    let function_names = all_function_names(query)
        .map_err(|err| PlanningCtxForSqlError::FunctionNameExtraction { source: err })?;
    let resolved_tables = get_logical_catalog(store, tables, function_names, &IsolatePool::dummy())
        .await
        .map_err(PlanningCtxForSqlError::GetLogicalCatalog)?;
    Ok(PlanningContext::new(resolved_tables))
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

    let datasets = {
        let mut datasets = BTreeSet::new();

        let table_refs_datasets = dataset_versions_from_table_refs(table_refs.iter())
            .map_err(GetLogicalCatalogError::ExtractDatasetFromTableRefs)?;
        tracing::debug!(
            table_refs_datasets = ?table_refs_datasets,
            "Extracted datasets from table references"
        );
        datasets.extend(table_refs_datasets);

        let function_datasets =
            dataset_versions_from_function_names(function_names.iter().map(AsRef::as_ref))
                .map_err(GetLogicalCatalogError::ExtractDatasetFromFunctionNames)?;
        tracing::debug!(
            function_datasets = ?function_datasets,
            "Extracted datasets from function names"
        );
        datasets.extend(function_datasets);

        tracing::debug!(
            total_datasets = ?datasets,
            "Combined datasets from table references and function names"
        );
        datasets
    };

    let mut resolved_tables = Vec::new();
    let mut udfs = Vec::new();
    for partial_ref in datasets {
        let dataset = store
            .get_dataset(partial_ref.clone())
            .await
            .map_err(GetLogicalCatalogError::GetDataset)?;

        // Check if this is an EVM RPC dataset and create eth_call UDF if needed
        let udf = store
            .eth_call_for_dataset(&partial_ref.to_string(), &dataset)
            .await
            .map_err(|err| GetLogicalCatalogError::EthCallUdfCreation {
                dataset: partial_ref.name.clone(),
                source: err,
            })?;
        if let Some(udf) = udf {
            udfs.push(udf);
        }

        // Add JS UDFs
        for udf in dataset.functions(partial_ref.name.to_string(), isolate_pool.clone()) {
            udfs.push(udf.into());
        }

        for table in &dataset.tables {
            // Only include tables that are actually referenced in the query
            let is_referenced = table_refs.iter().any(|table_ref| {
                match (table_ref.schema(), table_ref.table()) {
                    (Some(schema), table_name) => {
                        // Reconstruct the schema name from partial reference
                        let schema_name = partial_ref.to_string();
                        schema == schema_name && table_name == table.name()
                    }
                    _ => false, // Unqualified table
                }
            });

            if is_referenced {
                // Use partial reference string representation as schema name
                let schema_name = partial_ref.to_string();
                let table_ref =
                    TableReference::partial(schema_name.clone(), table.name().to_string());
                let resolved_table =
                    crate::ResolvedTable::new(table.clone(), dataset.clone(), table_ref);
                resolved_tables.push(resolved_table);
            }
        }
    }

    Ok(LogicalCatalog {
        tables: resolved_tables,
        udfs,
    })
}

/// Extracts dataset names and versions from table references in SQL queries.
///
/// This function processes table references from SQL queries and extracts the dataset names
/// and optional versions from the schema portion of qualified table names.
///
/// # Table Reference Format
/// - All tables must be qualified with namespace and dataset name: `namespace/dataset_name.table_name`
/// - Versioned datasets: `namespace/dataset_name@version.table_name` (e.g., `_/eth_rpc@1.0.0.blocks`)
/// - Unversioned datasets: `namespace/dataset_name.table_name` (e.g., `_/eth_rpc.blocks`)
/// - Catalog-qualified tables are not supported
///
/// # Returns
/// A set of unique partial references extracted from the table references.
fn dataset_versions_from_table_refs<'a>(
    table_refs: impl Iterator<Item = &'a TableReference>,
) -> Result<BTreeSet<PartialReference>, ExtractDatasetFromTableRefsError> {
    let mut datasets = BTreeSet::new();

    for table_ref in table_refs {
        if table_ref.catalog().is_some() {
            return Err(ExtractDatasetFromTableRefsError::CatalogQualifiedTable {
                table: table_ref.table().to_string(),
            });
        }

        let Some(catalog_schema) = table_ref.schema() else {
            return Err(ExtractDatasetFromTableRefsError::UnqualifiedTable {
                table: table_ref.table().to_string(),
            });
        };

        // Parse using PartialReference to handle namespace/name@version format
        let partial_ref = catalog_schema.parse::<PartialReference>().map_err(|err| {
            ExtractDatasetFromTableRefsError::ReferenceParse {
                schema: catalog_schema.to_string(),
                source: err,
            }
        })?;

        // Validate that revision (if present) is a Version type, not hash/latest/dev
        if let Some(revision) = &partial_ref.revision
            && !revision.is_version()
        {
            return Err(ExtractDatasetFromTableRefsError::InvalidVersion {
                version: revision.to_string(),
                schema: catalog_schema.to_string(),
            });
        }

        datasets.insert(partial_ref);
    }

    Ok(datasets)
}

/// Extracts dataset names and versions from function names in SQL queries.
///
/// This function processes qualified function names (e.g., `namespace/dataset.function` or `namespace/dataset@version.function`)
/// and extracts the dataset name and optional version from the qualifier.
///
/// # Function Name Format
/// - Simple function names (no qualifier) are assumed to be built-in DataFusion functions
/// - Qualified functions: `namespace/dataset_name.function_name` or `namespace/dataset_name@version.function_name`
/// - Examples: `_/eth_rpc.my_function()` or `_/eth_rpc@1.0.0.my_function()`
///
/// # Returns
/// A set of unique partial references extracted from the function names.
fn dataset_versions_from_function_names<'a>(
    function_names: impl IntoIterator<Item = &'a str>,
) -> Result<BTreeSet<PartialReference>, ExtractDatasetFromFunctionNamesError> {
    let mut datasets = BTreeSet::new();

    for func_name in function_names {
        let parts: Vec<_> = func_name.split('.').collect();
        let fn_dataset = match parts.as_slice() {
            // Simple name assumed to be Datafusion built-in function.
            [_] => continue,
            [dataset, _] => dataset,
            _ => {
                return Err(
                    ExtractDatasetFromFunctionNamesError::InvalidFunctionFormat {
                        function: func_name.to_string(),
                    },
                );
            }
        };

        // Parse using PartialReference to handle namespace/name@version format
        let partial_ref = fn_dataset.parse::<PartialReference>().map_err(|err| {
            ExtractDatasetFromFunctionNamesError::ReferenceParse {
                function: func_name.to_string(),
                source: err,
            }
        })?;

        // Validate that revision (if present) is a Version type, not hash/latest/dev
        if let Some(revision) = &partial_ref.revision
            && !revision.is_version()
        {
            return Err(ExtractDatasetFromFunctionNamesError::InvalidVersion {
                version: revision.to_string(),
                function: func_name.to_string(),
            });
        }

        datasets.insert(partial_ref);
    }

    Ok(datasets)
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
