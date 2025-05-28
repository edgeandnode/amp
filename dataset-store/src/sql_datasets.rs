use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    sync::Arc,
};

use common::{
    multirange::MultiRange,
    query_context::{parse_sql, QueryEnv},
    BlockNum, BoxError, Dataset, QueryContext, Table, BLOCK_NUM,
};
use datafusion::{
    common::tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    datasource::TableType,
    error::DataFusionError,
    execution::SendableRecordBatchStream,
    logical_expr::{col, lit, Filter, LogicalPlan, Sort, TableScan},
    sql::{parser, TableReference},
};
use futures::StreamExt as _;
use metadata_db::LocationId;
use object_store::ObjectMeta;
use serde::Deserialize;
use tracing::instrument;

use crate::DatasetStore;

pub struct SqlDataset {
    pub dataset: Dataset,

    /// Maps a table name to the query that defines that table.
    pub queries: BTreeMap<String, parser::Statement>,
}

impl SqlDataset {
    pub fn name(&self) -> &str {
        &self.dataset.name
    }
}

pub const DATASET_KIND: &str = "sql";

#[derive(Debug, Deserialize)]
pub(super) struct DatasetDef {
    pub kind: String,
    pub name: String,
}

pub(super) async fn dataset(
    store: Arc<DatasetStore>,
    dataset_def: toml::Value,
) -> Result<SqlDataset, BoxError> {
    let def: DatasetDef = dataset_def.try_into()?;
    if def.kind != DATASET_KIND {
        return Err(format!("expected dataset kind '{DATASET_KIND}', got '{}'", def.kind).into());
    }

    let defs_store = store.dataset_defs_store();
    let mut files = defs_store.list(def.name.clone());

    // List all `.sql` files in the dataset dir and infer the output schema to get `Table`s.
    let mut tables: Vec<Table> = vec![];
    let mut queries: BTreeMap<String, parser::Statement> = BTreeMap::new();
    while let Some(file) = files.next().await {
        let file: ObjectMeta = file?;

        // Unwrap: Listed paths are always files.
        let filename = file.location.filename().unwrap();
        let Some(table_name) = filename.strip_suffix(".sql") else {
            continue;
        };

        let raw_query = defs_store.get_string(file.location.clone()).await?;
        let query = parse_sql(&raw_query)?;
        let ctx = store.clone().planning_ctx_for_sql(&query).await?;
        let schema = ctx.sql_output_schema(query.clone()).await?;
        let network = {
            let tables = ctx.catalog().iter();
            let mut networks: BTreeSet<_> = tables.map(|t| t.table.network.clone()).collect();
            if networks.len() > 1 {
                return Err(format!(
                    "table {} has dependencies in multiple networks: {:?}",
                    table_name, networks
                )
                .into());
            }
            networks.pop_first().flatten().map(|n| n.to_string())
        };
        let table = Table {
            name: table_name.to_string(),
            schema: schema.as_ref().clone().into(),
            network,
        };
        tables.push(table);
        queries.insert(table_name.to_string(), query);
    }

    Ok(SqlDataset {
        dataset: Dataset {
            kind: def.kind,
            name: def.name,
            tables,
            functions: vec![],
        },
        queries,
    })
}

// Get synced ranges for table
async fn get_ranges_for_table(
    table: &TableReference,
    ctx: &QueryContext,
) -> Result<MultiRange, BoxError> {
    let Some(table) = ctx.get_table(table) else {
        return Err(format!("table {}.{} not found", table.schema().unwrap(), table.table()).into());
    };
    let ranges = table.ranges().await?;
    let ranges = MultiRange::from_ranges(ranges)?;

    Ok(ranges)
}

/// This will:
/// - Validate that dependencies have synced the required block range.
/// - Inject block range constraints into the plan.
/// - Inject 'order by block_num' into the plan.
/// - Execute the plan.
#[instrument(skip_all, err)]
pub async fn execute_plan_for_range(
    plan: LogicalPlan,
    ctx: &QueryContext,
    start: BlockNum,
    end: BlockNum,
) -> Result<SendableRecordBatchStream, BoxError> {
    let tables = extract_table_references_from_plan(&plan)?;

    // Validate dependency block ranges
    {
        let needed_range = MultiRange::from_ranges(vec![(start, end)]).unwrap();
        for table in tables {
            let physical_table = ctx
                .get_table(&table)
                .ok_or::<BoxError>(format!("table {} not found", table).into())?;
            let ranges = physical_table.ranges().await?;
            let ranges = MultiRange::from_ranges(ranges)?;
            let synced = ranges.intersection(&needed_range) == needed_range;
            if !synced {
                return Err(format!("tried to query range {needed_range} of table {table} but it has not been synced").into());
            }
        }
    }

    let plan = {
        let plan = inject_block_range_constraints(plan, start, end)?;
        order_by_block_num(plan)
    };
    Ok(ctx.execute_plan(plan).await?)
}

/// This will:
/// - Plan the query against the configured datasets.
/// - Validate that the query is materializable.
/// - Execute the plan for a specified range
#[instrument(skip_all, err)]
pub async fn execute_query_for_range(
    query: parser::Statement,
    dataset_store: Arc<DatasetStore>,
    env: QueryEnv,
    start: BlockNum,
    end: BlockNum,
) -> Result<SendableRecordBatchStream, BoxError> {
    let ctx = dataset_store.clone().ctx_for_sql(&query, env).await?;
    let plan = ctx.plan_sql(query).await?;

    execute_plan_for_range(plan, &ctx, start, end).await
}

// All physical tables locations that have been queried
#[instrument(skip_all, err)]
pub async fn queried_physical_tables(
    plan: &LogicalPlan,
    qc: &QueryContext,
) -> Result<Vec<LocationId>, BoxError> {
    let tables = extract_table_references_from_plan(&plan)?;

    let locations: Vec<_> = tables
        .into_iter()
        .filter_map(|t| qc.get_table(&t).map(|t| t.location_id()))
        .collect();

    Ok(locations)
}

fn extract_table_references_from_plan(plan: &LogicalPlan) -> Result<Vec<TableReference>, BoxError> {
    let mut refs = HashSet::new();

    plan.apply(|node| {
        match node {
            LogicalPlan::TableScan(scan) => {
                refs.insert(scan.table_name.clone());
            }
            _ => {}
        }

        Ok(TreeNodeRecursion::Continue)
    })?;

    Ok(refs.into_iter().collect())
}

/// The blocks that have been synced for all tables in the plan.
#[instrument(skip_all, err)]
pub async fn synced_blocks_for_plan(
    plan: &LogicalPlan,
    ctx: &QueryContext
) -> Result<MultiRange, BoxError> {
    let tables = extract_table_references_from_plan(&plan)?;

    let mut ranges = MultiRange::empty();

    if !tables.is_empty() {
        let mut tables = tables.into_iter();
        ranges = get_ranges_for_table(&tables.next().unwrap(), ctx).await?;
        for table in tables {
            let other_ranges = get_ranges_for_table(&table, ctx).await?;
            ranges = ranges.intersection(&other_ranges);
        }
    }

    Ok(ranges)
}

/// The most recent block that has been synced for all tables in the plan.
#[instrument(skip_all, err)]
pub async fn max_end_block(
    plan: &LogicalPlan,
    ctx: Arc<QueryContext>,
) -> Result<Option<BlockNum>, BoxError> {
    let tables = extract_table_references_from_plan(&plan)?;

    if tables.is_empty() {
        return Ok(None);
    }

    let synced_block_for_table = move |ctx: Arc<QueryContext>, table: TableReference| async move {
        let table = ctx.get_table(&table).expect("table not found");
        let ranges = table.ranges().await?;
        let ranges = MultiRange::from_ranges(ranges)?;

        // Take the end block of the earliest contiguous range as the "synced block"
        Ok::<_, BoxError>(ranges.first().map(|r| r.1))
    };

    let mut tables = tables.into_iter();

    // Unwrap: `tables` is not empty.
    let mut end = synced_block_for_table(ctx.clone(), tables.next().unwrap()).await?;
    for table in tables {
        let next_end = synced_block_for_table(ctx.clone(), table).await?;
        end = end.min(next_end);
    }

    Ok(end)
}

/// How a logical plan can be materialized in a dataset. For some queries,
/// we support incremental materialization, whereas for others we need to
/// recalculate the entire output.
pub fn is_incremental(plan: &LogicalPlan) -> Result<bool, BoxError> {
    use LogicalPlan::*;

    fn unsupported(op: String) -> Option<BoxError> {
        Some(format!("unsupported operation in query: {op}").into())
    }

    // As we traverse the tree, assume we can materialize incrementally. If
    // we find a node that requires materialization of the entire query
    // nonincrementally, `is_incr` to `true`. If we find anything that
    // cannot be materialized, we set `Err` to `Some(_)`. This ensures that
    // we always report an error if there is one, and never go from entire
    // to incremental materialization.
    let mut is_incr = true;
    let mut err: Option<BoxError> = None;

    // The plan is materializable if no non-materializable nodes are found.
    plan.apply(|node| {
        match node {
            // Embarrassingly parallel operators
            Projection(_) | Filter(_) | Union(_) | Unnest(_) => { /* incremental */ }

            // Not really logical operators, so we just skip them.
            Repartition(_) | TableScan(_) | EmptyRelation(_) | Values(_) | Subquery(_)
            | SubqueryAlias(_) => { /* incremental */ }

            // Aggregations and join materialization seem doable
            // incrementally but need thinking through.
            Aggregate(_) | Distinct(_) => is_incr = false,
            Join(_) => is_incr = false,

            // Sorts are not parallel or incremental
            Sort(_) | Limit(_) => is_incr = false,

            // Window functions are complicated, they often result in a sort.
            Window(_) => is_incr = false,

            // Another complicated one.
            RecursiveQuery(_) => is_incr = false,

            // Commands that don't make sense in a dataset definition.
            DescribeTable(_) | Explain(_) | Analyze(_) => {
                err = unsupported(format!("{}", node.display()))
            }

            // Definitely not supported and would be caught elsewhere.
            Dml(_) | Ddl(_) | Statement(_) | Copy(_) => {
                err = unsupported(format!("{}", node.display()))
            }

            // We don't currently have any custom operators.
            Extension(_) => err = unsupported(format!("{}", node.display())),
        };

        // Stop recursion if we found a non-materializable node.
        match err {
            Some(_) => Ok(TreeNodeRecursion::Stop),
            None => Ok(TreeNodeRecursion::Continue),
        }
    })
    .unwrap();

    match err {
        Some(err) => Err(err),
        None => Ok(is_incr),
    }
}

#[instrument(skip_all, err)]
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
            let mut predicate = col(BLOCK_NUM).lt_eq(lit(end));
            predicate = predicate.and(lit(start).lt_eq(col(BLOCK_NUM)));

            let with_filter = Filter::try_new(predicate, Arc::new(node))?;
            Ok(Transformed::yes(LogicalPlan::Filter(with_filter)))
        }
        _ => Ok(Transformed::no(node)),
    })
    .map(|t| t.data)
}

fn order_by_block_num(plan: LogicalPlan) -> LogicalPlan {
    let sort = Sort {
        expr: vec![col(BLOCK_NUM).sort(true, false)],
        input: Arc::new(plan),
        fetch: None,
    };
    LogicalPlan::Sort(sort)
}
