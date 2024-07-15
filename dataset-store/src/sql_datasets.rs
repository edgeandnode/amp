use std::sync::Arc;

use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    datasource::TableType,
    error::DataFusionError,
    execution::{runtime_env::RuntimeEnv, SendableRecordBatchStream},
    logical_expr::{col, lit, Filter, LogicalPlan, Sort, TableScan},
};

use common::{
    catalog::resolve_table_references,
    meta_tables::scanned_ranges,
    multirange::MultiRange,
    query_context::{parse_sql, Error as CoreError},
    BlockNum, BoxError, Dataset, Table, BLOCK_NUM,
};
use futures::StreamExt as _;
use object_store::ObjectMeta;
use serde::Deserialize;

use crate::DatasetStore;

pub const DATASET_KIND: &str = "sql";

#[derive(Debug, Deserialize)]
pub(super) struct DatasetDef {
    pub kind: String,
    pub name: String,
}

pub(super) async fn dataset(
    store: Arc<DatasetStore>,
    dataset_def: toml::Value,
) -> Result<Dataset, BoxError> {
    let def: DatasetDef = dataset_def.try_into()?;
    if def.kind != DATASET_KIND {
        return Err(format!("expected dataset kind '{DATASET_KIND}', got '{}'", def.kind).into());
    }

    let defs_store = store.dataset_defs_store();
    let mut files = defs_store.list(def.name.clone());

    // List all `.sql` files in the dataset dir and infer the output schema to get `Table`s.
    let mut tables: Vec<Table> = vec![];
    while let Some(file) = files.next().await {
        let file: ObjectMeta = file?;

        // Unwrap: Listed paths are always files.
        let filename = file.location.filename().unwrap();
        let Some(table_name) = filename.strip_suffix(".sql") else {
            continue;
        };

        let raw_query = defs_store.get_string(file.location.clone()).await?;
        let query = parse_sql(&raw_query)?;
        let env = Arc::new(store.config.make_runtime_env()?);
        let ctx = store.clone().ctx_for_sql(&query, env).await?;
        let schema = ctx.sql_output_schema(query).await?;
        let table = Table {
            name: table_name.to_string(),
            schema: schema.as_ref().clone().into(),
        };
        tables.push(table);
    }

    Ok(Dataset {
        name: def.name,
        tables,
    })
}

/// This will:
/// - Plan the query against the configured datasets.
/// - Validate that the query is materializable.
/// - Validate that dependencies have synced the required block range.
/// - Inject block range constraints into the plan.
/// - Inject 'order by block_num' into the plan.
/// - Execute the plan.
async fn execute_query_for_range(
    query: &str,
    dataset_store: Arc<DatasetStore>,
    env: Arc<RuntimeEnv>,
    start: BlockNum,
    end: BlockNum,
) -> Result<SendableRecordBatchStream, BoxError> {
    let statement = parse_sql(query)?;
    let (tables, _) = resolve_table_references(&statement, true)
        .map_err(|e| CoreError::SqlParseError(e.into()))?;
    let ctx = dataset_store.ctx_for_sql(&statement, env).await?;

    // Validate dependency scanned ranges
    for table in tables {
        // Unwrap: A valid catalog was built with this table name.
        let catalog_schema = table.catalog().unwrap();
        let ranges = scanned_ranges::ranges_for_table(&ctx, catalog_schema, table.table()).await?;
        let ranges = MultiRange::from_ranges(ranges);
        let needed_range = MultiRange::from_ranges(vec![(start, end)]);
        let synced = ranges.intersection(&needed_range) == needed_range;
        if !synced {
            return Err(format!("tried to query range {needed_range} of dataset {catalog_schema} but it has only synced {ranges}").into());
        }
    }

    let plan = ctx.plan_sql(statement).await?;
    check_support(&plan)?;
    let plan = inject_block_range_constraints(plan, start, end)?;
    let plan = order_by_block_num(plan);
    Ok(ctx.execute_plan(plan).await?)
}

/// This function validates that a query can be used in a dataset definiton.
///
/// Currently, only 'embarassingly parallel' queries consisting of just projections and filters are
/// supported, these are easy to compute incrementally as they are just maps of the input rows.
///
/// Support for aggregations and joins would be desirable but will require more thought.
fn check_support(plan: &LogicalPlan) -> Result<bool, BoxError> {
    use LogicalPlan::*;

    // For error messages if an invalid node is found.
    let mut bad_operator = String::new();

    // The plan is materializable if no non-materializable nodes are found.
    let is_materializable = !plan
        .exists(|node| {
            let is_materializable = match node {
                // Embarrassingly parallel operators
                Projection(_) | Filter(_) | Union(_) | Unnest(_) => true,

                // Not really logical operators, so we just skip them.
                Repartition(_) | TableScan(_) | EmptyRelation(_) | Values(_) | Subquery(_)
                | SubqueryAlias(_) => true,

                // Aggregations and join materialization seem doable but need thinking through.
                Aggregate(_) | Distinct(_) => false,
                Join(_) => false,

                // Sorts are not parallel or incremental, so a questionable thing to materialize
                // unless the input is truly bounded. Top K queries may be something to think about.
                Sort(_) | Limit(_) => false,

                // Window functions are complicated, they often result in a sort.
                Window(_) => false,

                // Another complicated one.
                RecursiveQuery(_) => false,

                // Nobody uses cross-joins.
                CrossJoin(_) => false,

                // Commands that don't make sense in a dataset definition.
                DescribeTable(_) | Explain(_) | Analyze(_) | Prepare(_) => false,

                // Definitely not supported and would be caught eleswhere.
                Dml(_) | Ddl(_) | Statement(_) | Copy(_) => false,

                // We don't currently have any custom operators.
                Extension(_) => false,
            };

            if !is_materializable {
                bad_operator = format!("{}", node.display());
            }

            // Stop recursion if we found a non-materializable node.
            Ok(!is_materializable)
        })
        .unwrap();

    if is_materializable {
        Ok(true)
    } else {
        Err(format!("unsupported operation in query: {bad_operator}").into())
    }
}

fn inject_block_range_constraints(
    plan: LogicalPlan,
    start: u64,
    end: u64,
) -> Result<LogicalPlan, DataFusionError> {
    plan.transform(|node| match &node {
        // Insert the clauses in non-view table scans
        LogicalPlan::TableScan(TableScan { source, .. })
            if source.table_type() == TableType::Base && source.get_logical_plan().is_none() =>
        {
            // `where start <= block_num and block_num <= end`
            // Is it ok for this to be unqualified? Or should it be `TABLE_NAME.block_num`?
            let predicate = lit(start)
                .lt_eq(col(BLOCK_NUM))
                .and(col(BLOCK_NUM).lt_eq(lit(end)));
            let with_filter = Filter::try_new(predicate, Arc::new(node))?;
            Ok(Transformed::yes(LogicalPlan::Filter(with_filter)))
        }
        _ => Ok(Transformed::no(node)),
    })
    .map(|t| t.data)
}

fn order_by_block_num(plan: LogicalPlan) -> LogicalPlan {
    let sort = Sort {
        expr: vec![col(BLOCK_NUM)],
        input: Arc::new(plan),
        fetch: None,
    };
    LogicalPlan::Sort(sort)
}
