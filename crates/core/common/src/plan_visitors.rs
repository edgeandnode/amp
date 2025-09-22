use std::{collections::BTreeSet, sync::Arc};

use datafusion::{
    common::{
        plan_err,
        tree_node::{Transformed, TransformedResult as _, TreeNode as _, TreeNodeRecursion},
    },
    datasource::TableType,
    error::DataFusionError,
    logical_expr::{Filter, LogicalPlan, Projection, Sort, TableScan},
    optimizer::{OptimizerContext, OptimizerRule, push_down_filter::PushDownFilter},
    prelude::{Expr, col, lit},
    sql::TableReference,
};
use tracing::instrument;

use crate::{
    BLOCK_NUM, BoxError, SPECIAL_BLOCK_NUM, internal,
    query_context::prepend_special_block_num_field,
};

/// Aliases with a name starting with `_` are always forbidden, since underscore-prefixed
/// names are reserved for special columns.
pub fn forbid_underscore_prefixed_aliases(plan: &LogicalPlan) -> Result<(), DataFusionError> {
    plan.apply(|node| {
        match node {
            LogicalPlan::Projection(projection) => {
                for expr in projection.expr.iter() {
                    if let Expr::Alias(alias) = expr {
                        if alias.name.starts_with('_') {
                            return plan_err!(
                                "projection contains a column alias starting with '_': '{}'. Underscore-prefixed names are reserved. Please rename your column",
                                alias.name
                            );
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

/// Propagate the `SPECIAL_BLOCK_NUM` column through the logical plan.
pub fn propagate_block_num(plan: LogicalPlan) -> Result<LogicalPlan, DataFusionError> {
    plan.transform(|node| {
        match node {
            LogicalPlan::Projection(mut projection) => {
                // If the projection already selects the `SPECIAL_BLOCK_NUM` column directly, we don't need to
                // add that column, but we do have to ensure that the `SPECIAL_BLOCK_NUM` is not
                // qualified with a table name (since it does not necessarily belong to a table).
                for expr in projection.expr.iter_mut() {
                    if let Expr::Column(c) = expr
                        && c.name == SPECIAL_BLOCK_NUM
                    {
                        *expr = expr.clone().alias(SPECIAL_BLOCK_NUM);
                        return Ok(Transformed::yes(LogicalPlan::Projection(projection)));
                    }
                }

                // Prepend `SPECIAL_BLOCK_NUM` column to the projection.
                projection.expr.insert(0, col(SPECIAL_BLOCK_NUM));
                projection.schema = prepend_special_block_num_field(&projection.schema);
                Ok(Transformed::yes(LogicalPlan::Projection(projection)))
            }
            LogicalPlan::Union(mut union) => {
                // Add the `SPECIAL_BLOCK_NUM` column to the union schema.
                union.schema = prepend_special_block_num_field(&union.schema);
                Ok(Transformed::yes(LogicalPlan::Union(union)))
            }
            _ => Ok(Transformed::no(node)),
        }
    })
    .data()
}

/// This will project the `SPECIAL_BLOCK_NUM` out of the plan by adding a projection on top of the
/// query which selects all columns except `SPECIAL_BLOCK_NUM`.
pub fn unproject_special_block_num_column(
    plan: LogicalPlan,
) -> Result<LogicalPlan, DataFusionError> {
    let fields = plan.schema().fields();
    if !fields.iter().any(|f| f.name() == SPECIAL_BLOCK_NUM) {
        // Nothing to do.
        return Ok(plan);
    }
    let exprs = fields
        .iter()
        .filter(|f| f.name() != SPECIAL_BLOCK_NUM)
        .map(|f| col(f.name()))
        .collect();
    let projection = Projection::try_new(exprs, Arc::new(plan)).map_err(|e| {
        internal!(
            "error while removing {} from projection: {}",
            SPECIAL_BLOCK_NUM,
            e
        )
    })?;
    Ok(LogicalPlan::Projection(projection))
}

/// Adds `where start <= _block_num and _block_num <= end` to the plan and runs filter pushdown.
#[instrument(skip_all, err)]
pub fn constrain_by_block_num(
    plan: LogicalPlan,
    start: u64,
    end: u64,
) -> Result<LogicalPlan, DataFusionError> {
    plan.transform(|node| match &node {
        // Insert the clauses in non-view table scans
        LogicalPlan::TableScan(TableScan { source, .. })
            if source.table_type() == TableType::Base && source.get_logical_plan().is_none() =>
        {
            let column_name = if source
                .schema()
                .fields()
                .iter()
                .any(|f| f.name() == SPECIAL_BLOCK_NUM)
            {
                SPECIAL_BLOCK_NUM
            } else {
                BLOCK_NUM
            };
            // `where start <= block_num and block_num <= end`
            let mut predicate = col(column_name).lt_eq(lit(end));
            predicate = predicate.and(lit(start).lt_eq(col(column_name)));

            let with_filter = LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(node))?);
            let with_pushdown =
                PushDownFilter::new().rewrite(with_filter, &OptimizerContext::default())?;
            Ok(Transformed::yes(with_pushdown.data))
        }
        _ => Ok(Transformed::no(node)),
    })
    .map(|t| t.data)
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

            // Limit is stateful, it needs to count rows
            Limit(_) => is_incr = false,

            // Not really logical operators, so we just skip them.
            Repartition(_) | TableScan(_) | EmptyRelation(_) | Values(_) | Subquery(_)
            | SubqueryAlias(_) | DescribeTable(_) | Explain(_) | Analyze(_) => { /* incremental */ }

            // Aggregations and join materialization seem doable
            // incrementally but need thinking through.
            Aggregate(_) | Distinct(_) => is_incr = false,
            Join(_) => is_incr = false,

            // Sorts are not parallel or incremental
            Sort(_) => is_incr = false,

            // Window functions are complicated, they often result in a sort.
            Window(_) => is_incr = false,

            // Another complicated one.
            RecursiveQuery(_) => is_incr = false,

            // Definitely not supported.
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

pub fn extract_table_references_from_plan(
    plan: &LogicalPlan,
) -> Result<Vec<TableReference>, BoxError> {
    let mut refs = BTreeSet::new();

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

pub fn order_by_block_num(plan: LogicalPlan) -> LogicalPlan {
    let sort = Sort {
        expr: vec![col(SPECIAL_BLOCK_NUM).sort(true, false)],
        input: Arc::new(plan),
        fetch: None,
    };
    LogicalPlan::Sort(sort)
}

#[cfg(test)]
mod tests {
    use datafusion::{
        arrow::{
            array,
            datatypes::{DataType, Field, Schema},
        },
        common::Column,
        datasource::{MemTable, provider_as_source},
        logical_expr::{JoinType, LogicalPlanBuilder},
    };

    use super::*;

    #[tokio::test]
    async fn test_propagate_block_num_with_qualified_wildcard() {
        // Create two tables that both contain SPECIAL_BLOCK_NUM columns
        let foo_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(SPECIAL_BLOCK_NUM, DataType::UInt64, false),
            Field::new("foo_value", DataType::Utf8, false),
        ]));

        let bar_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(SPECIAL_BLOCK_NUM, DataType::UInt64, false),
            Field::new("bar_value", DataType::Utf8, false),
        ]));

        let ids = array::Int32Array::from(vec![1, 2, 3]);
        let foo_values = array::StringArray::from(vec!["foo1", "foo2", "foo3"]);
        let bar_values = array::StringArray::from(vec!["bar1", "bar2", "bar3"]);
        let block_nums = array::UInt64Array::from(vec![10, 20, 30]);
        let foo_batch = datafusion::arrow::record_batch::RecordBatch::try_new(
            foo_schema.clone(),
            vec![
                Arc::new(ids.clone()),
                Arc::new(block_nums.clone()),
                Arc::new(foo_values),
            ],
        )
        .unwrap();
        let bar_batch = datafusion::arrow::record_batch::RecordBatch::try_new(
            bar_schema.clone(),
            vec![Arc::new(ids), Arc::new(block_nums), Arc::new(bar_values)],
        )
        .unwrap();

        // Create memory tables
        let foo_table = MemTable::try_new(foo_schema.clone(), vec![vec![foo_batch]]).unwrap();
        let bar_table = MemTable::try_new(bar_schema.clone(), vec![vec![bar_batch]]).unwrap();

        // Build a logical plan with `SELECT foo.* FROM foo JOIN bar ON foo.id = bar.id`
        let foo_scan =
            LogicalPlanBuilder::scan("foo", provider_as_source(Arc::new(foo_table)), None)
                .unwrap()
                .build()
                .unwrap();

        let bar_scan =
            LogicalPlanBuilder::scan("bar", provider_as_source(Arc::new(bar_table)), None)
                .unwrap()
                .build()
                .unwrap();

        // Create a join
        let join_plan = LogicalPlanBuilder::from(foo_scan)
            .join(
                bar_scan,
                JoinType::Inner,
                (
                    vec![Column::from_qualified_name("foo.id")],
                    vec![Column::from_qualified_name("bar.id")],
                ),
                None,
            )
            .unwrap()
            .build()
            .unwrap();

        // Project foo.* (which includes foo._block_num)
        let projection_plan = LogicalPlanBuilder::from(join_plan)
            .project(vec![
                col("foo.id"),
                col(format!("foo.{}", SPECIAL_BLOCK_NUM)),
                col("foo.foo_value"),
            ])
            .unwrap()
            .build()
            .unwrap();

        // Apply propagate_block_num - this should handle the qualified column correctly
        let result = propagate_block_num(projection_plan);

        assert!(
            result.is_ok(),
            "propagate_block_num should succeed with qualified SPECIAL_BLOCK_NUM column"
        );

        let transformed_plan = result.unwrap();

        // Check that the plan was transformed (should be a Projection)
        match &transformed_plan {
            LogicalPlan::Projection(projection) => {
                // The first expression should be the aliased SPECIAL_BLOCK_NUM
                assert!(projection.expr.len() == 3, "Should have exactly 3 columns");

                // Check if the qualified column was properly aliased
                if let Expr::Alias(alias) = &projection.expr[1] {
                    assert_eq!(
                        alias.name, SPECIAL_BLOCK_NUM,
                        "Should alias to _block_num"
                    );
                    if let Expr::Column(c) = alias.expr.as_ref() {
                        assert_eq!(
                            c.name, SPECIAL_BLOCK_NUM,
                            "Should reference SPECIAL_BLOCK_NUM (_block_num) column"
                        );
                        assert_eq!(
                            c.relation.as_ref().unwrap().table(),
                            "foo",
                            "Should retain the 'foo' qualifier"
                        );
                    }
                } else {
                    panic!("Expected an aliased expression for qualified _block_num column");
                }
            }
            _ => panic!("Expected a Projection plan after propagate_block_num"),
        }

        // Check the schema to ensure SPECIAL_BLOCK_NUM is present and correctly aliased
        let schema = transformed_plan.schema();
        assert!(
            schema.fields().iter().any(|f| f.name() == SPECIAL_BLOCK_NUM),
            "Schema should contain the SPECIAL_BLOCK_NUM field"
        );
    }
}
