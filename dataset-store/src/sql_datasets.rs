use datafusion::{
    common::tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    datasource::TableType,
    error::DataFusionError,
    execution::{runtime_env::RuntimeEnv, SendableRecordBatchStream},
    logical_expr::{col, lit, Filter, LogicalPlan, Sort, TableScan},
    sql::resolve::resolve_table_references,
    sql::{parser, TableReference},
};
use std::collections::HashSet;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use crate::DatasetStore;
use common::{
    meta_tables::scanned_ranges,
    multirange::MultiRange,
    query_context::{parse_sql, Error as CoreError},
    BlockNum, BoxError, Dataset, QueryContext, Table, BLOCK_NUM,
};
use futures::StreamExt as _;
use metadata_db::{MetadataDb, TableId};
use object_store::ObjectMeta;
use serde::Deserialize;
use tracing::instrument;

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
        },
        queries,
    })
}

/// This will:
/// - Plan the query against the configured datasets.
/// - Validate that the query is materializable.
/// - Validate that dependencies have synced the required block range.
/// - Inject block range constraints into the plan.
/// - Inject 'order by block_num' into the plan.
/// - Execute the plan.
#[instrument(skip_all, err)]
pub async fn execute_query_for_range(
    query: parser::Statement,
    dataset_store: Arc<DatasetStore>,
    env: Arc<RuntimeEnv>,
    start: BlockNum,
    end: BlockNum,
) -> Result<SendableRecordBatchStream, BoxError> {
    let (tables, _) =
        resolve_table_references(&query, true).map_err(|e| CoreError::SqlParseError(e.into()))?;
    let ctx = dataset_store.clone().ctx_for_sql(&query, env).await?;
    let metadata_db = dataset_store.metadata_db.as_ref();

    // Validate dependency scanned ranges
    {
        let needed_range = MultiRange::from_ranges(vec![(start, end)]).unwrap();
        for table in tables {
            let tbl = TableId {
                // Unwrap: table references are of the partial form: [dataset].[table_name]
                dataset: table.schema().unwrap(),
                dataset_version: None,
                table: table.table(),
            };
            let ranges = scanned_ranges::ranges_for_table(&ctx, metadata_db, tbl).await?;
            let ranges = MultiRange::from_ranges(ranges)?;
            let synced = ranges.intersection(&needed_range) == needed_range;
            if !synced {
                return Err(format!("tried to query range {needed_range} of table {table} but it has not been synced").into());
            }
        }
    }

    let plan = ctx.plan_sql(query).await?;
    let plan = {
        let plan = inject_block_range_constraints(plan, start, end)?;
        order_by_block_num(plan)
    };
    Ok(ctx.execute_plan(plan).await?)
}

// All datasets that have been queried
#[instrument(skip_all, err)]
pub async fn queried_datasets(plan: &LogicalPlan) -> Result<Vec<String>, BoxError> {
    let tables = extract_table_references_from_plan(&plan);

    let ds = tables
        .into_iter()
        .filter_map(|t| t.schema().map(|s| s.to_string()))
        .collect::<Vec<_>>();
    Ok(ds)
}

fn extract_table_references_from_plan(plan: &LogicalPlan) -> Vec<TableReference> {
    let mut refs = HashSet::new();
    collect_table_refs(plan, &mut refs);
    refs.into_iter().collect()
}

fn collect_table_refs(plan: &LogicalPlan, refs: &mut HashSet<TableReference>) {
    match plan {
        LogicalPlan::TableScan(scan) => {
            refs.insert(scan.table_name.clone());
        }
        LogicalPlan::Join(join) => {
            collect_table_refs(&join.left, refs);
            collect_table_refs(&join.right, refs);
        }
        LogicalPlan::Projection(projection) => {
            collect_table_refs(&projection.input, refs);
        }
        LogicalPlan::Filter(filter) => {
            collect_table_refs(&filter.input, refs);
        }
        LogicalPlan::Aggregate(Aggregate) => {
            collect_table_refs(&Aggregate.input, refs);
        }
        LogicalPlan::Sort(sort) => {
            collect_table_refs(&sort.input, refs);
        }
        LogicalPlan::Limit(limit) => {
            collect_table_refs(&limit.input, refs);
        }
        LogicalPlan::Subquery(subquery) => collect_table_refs(&subquery.subquery, refs),
        LogicalPlan::Union(union) => {
            for input in &union.inputs {
                collect_table_refs(input, refs);
            }
        }
        LogicalPlan::SubqueryAlias(alias) => {
            collect_table_refs(&alias.input, refs);
        }
        LogicalPlan::Window(window) => {
            collect_table_refs(&window.input, refs);
        }
        LogicalPlan::Repartition(repartition) => {
            collect_table_refs(&repartition.input, refs);
        }
        LogicalPlan::Distinct(distinct) => {
            collect_table_refs(&distinct.input(), refs);
        }
        LogicalPlan::Unnest(unnest) => {
            collect_table_refs(&unnest.input, refs);
        }
        LogicalPlan::RecursiveQuery(recursive) => {
            collect_table_refs(&recursive.static_term, refs);
            collect_table_refs(&recursive.recursive_term, refs);
        }

        LogicalPlan::Explain(_) => {}
        LogicalPlan::Analyze(_) => {}
        LogicalPlan::Dml(_) => {}
        LogicalPlan::Ddl(_) => {}
        LogicalPlan::Copy(_) => {}
        LogicalPlan::DescribeTable(_) => {}

        LogicalPlan::EmptyRelation(_) => {}
        LogicalPlan::Statement(_) => {}
        LogicalPlan::Values(_) => {}
        LogicalPlan::Extension(_) => {}
    }
}

/// The most recent block that has been synced for all tables in the plan.
#[instrument(skip_all, err)]
pub async fn max_end_block_for_plan(
    plan: &LogicalPlan,
    ctx: &QueryContext,
    metadata_db: &MetadataDb,
) -> Result<Option<BlockNum>, BoxError> {
    let tables = extract_table_references_from_plan(&plan);

    if tables.is_empty() {
        return Ok(None);
    }

    // let metadata_db = metadata_db.as_ref();

    let synced_block_for_table = |ctx, table: TableReference| async move {
        let tbl = TableId {
            // Unwrap: table references are of the partial form: [dataset].[table_name]
            dataset: table.schema().unwrap(),
            dataset_version: None,
            table: table.table(),
        };
        let ranges = scanned_ranges::ranges_for_table(ctx, Some(metadata_db), tbl).await?;
        let ranges = MultiRange::from_ranges(ranges)?;

        // Take the end block of the earliest contiguous range as the "synced block"
        Ok::<_, BoxError>(ranges.first().map(|r| r.1))
    };

    let mut tables = tables.into_iter();
    // let ctx_ref = ctx.as_ref();

    // Unwrap: `tables` is not empty.
    let mut end = synced_block_for_table(ctx, tables.next().unwrap()).await?;
    for table in tables {
        let next_end = synced_block_for_table(ctx, table).await?;
        end = end.min(next_end);
    }

    Ok(end)
}

/// The most recent block that has been synced for all tables in the query.
#[instrument(skip_all, err)]
pub async fn max_end_block(
    query: &parser::Statement,
    dataset_store: Arc<DatasetStore>,
    env: Arc<RuntimeEnv>,
) -> Result<Option<BlockNum>, BoxError> {
    let (tables, _) =
        resolve_table_references(&query, true).map_err(|e| CoreError::SqlParseError(e.into()))?;

    if tables.is_empty() {
        return Ok(None);
    }

    let ctx = dataset_store.clone().ctx_for_sql(&query, env).await?;
    let metadata_db = &dataset_store.metadata_db;

    let synced_block_for_table = move |ctx, table: TableReference| async move {
        let tbl = TableId {
            // Unwrap: table references are of the partial form: [dataset].[table_name]
            dataset: table.schema().unwrap(),
            dataset_version: None,
            table: table.table(),
        };
        let ranges = scanned_ranges::ranges_for_table(ctx, metadata_db.as_ref(), tbl).await?;
        let ranges = MultiRange::from_ranges(ranges)?;

        // Take the end block of the earliest contiguous range as the "synced block"
        Ok::<_, BoxError>(ranges.first().map(|r| r.1))
    };

    let mut tables = tables.into_iter();

    // Unwrap: `tables` is not empty.
    let mut end = synced_block_for_table(&ctx, tables.next().unwrap()).await?;
    for table in tables {
        let next_end = synced_block_for_table(&ctx, table).await?;
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

            // Definitely not supported and would be caught eleswhere.
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

#[cfg(test)]
mod tests {
    use super::{is_incremental, queried_datasets};
    use common::catalog::physical::{Catalog, PhysicalDataset, PhysicalTable};
    use common::config::Config;
    use common::query_context::parse_sql;
    use common::{Dataset, QueryContext, Table};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::LogicalPlan;
    use std::sync::Arc;
    use url::Url;

    async fn create_test_query_context() -> QueryContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_num", DataType::UInt64, false),
            Field::new("timestamp", DataType::Date32, false),
        ]));

        let datasets = vec![
            PhysicalDataset::new(
                Dataset {
                    kind: "firehose".to_string(),
                    name: "eth_firehose".to_string(),
                    tables: vec![],
                },
                vec![PhysicalTable::new(
                    "eth_firehose",
                    Table {
                        name: "blocks".to_string(),
                        schema: schema.clone(),
                        network: None,
                    },
                    Url::parse("s3://bucket/blocks").unwrap(),
                    None,
                )
                .unwrap()],
            ),
            PhysicalDataset::new(
                Dataset {
                    kind: "sql".to_string(),
                    name: "sql_ds".to_string(),
                    tables: vec![],
                },
                vec![PhysicalTable::new(
                    "sql_ds",
                    Table {
                        name: "even_blocks".to_string(),
                        schema: schema.clone(),
                        network: None,
                    },
                    Url::parse("s3://bucket/blocks").unwrap(),
                    None,
                )
                .unwrap()],
            ),
        ];
        let catalog = Catalog::new(datasets);
        let config = Arc::new(Config::in_memory());
        let env = Arc::new(config.make_runtime_env().unwrap());
        let qc = QueryContext::for_catalog(catalog, env.clone()).unwrap();

        qc
    }

    async fn get_plan(sql: &str) -> LogicalPlan {
        let qc = create_test_query_context().await;

        let stmt = parse_sql(sql).unwrap();
        let plan = qc.plan_sql(stmt).await.unwrap();

        plan
    }

    async fn assert_incremental_for_all_is(sql_queries: Vec<&str>, expected: bool) {
        for sql in sql_queries {
            let plan = get_plan(sql).await;
            assert_eq!(is_incremental(&plan).unwrap(), expected);
        }
    }

    #[tokio::test]
    async fn incremental_queries() {
        let queries = vec![
            "SELECT * FROM eth_firehose.blocks",
            "SELECT * FROM (select block_num from eth_firehose.blocks) as t",
            "SELECT * FROM eth_firehose.blocks WHERE block_num < 10",
            "SELECT * FROM (SELECT block_num FROM eth_firehose.blocks WHERE block_num < 10) t WHERE block_num > 10",
            "SELECT block_num FROM eth_firehose.blocks WHERE block_num < 10 UNION ALL SELECT block_num FROM eth_firehose.blocks WHERE block_num > 10",
        ];
        assert_incremental_for_all_is(queries, true).await;
    }

    #[tokio::test]
    async fn not_incremental_queries() {
        let queries = vec!["SELECT MAX(block_num) FROM eth_firehose.blocks"];
        assert_incremental_for_all_is(queries, false).await;
    }

    #[tokio::test]
    async fn extract_datasets_from_plan() {
        let plan = get_plan("SELECT * FROM eth_firehose.blocks AS b INNER JOIN sql_ds.even_blocks AS e ON b.block_num = e.block_num").await;

        let mut datasets = queried_datasets(&plan).await.unwrap();
        datasets.sort(); // For consistent test results
        assert_eq!(
            datasets,
            vec!["eth_firehose".to_string(), "sql_ds".to_string()]
        );
    }
}
