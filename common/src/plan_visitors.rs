use std::{collections::BTreeSet, sync::Arc};

use datafusion::{
    common::{
        plan_err,
        tree_node::{Transformed, TransformedResult as _, TreeNode as _, TreeNodeRecursion},
        DFSchema,
    },
    datasource::TableType,
    error::DataFusionError,
    logical_expr::{Filter, LogicalPlan, Projection, Sort, TableScan},
    prelude::{col, lit, Expr},
    sql::TableReference,
};
use tracing::instrument;

use crate::{
    internal, query_context::prepend_special_block_num_field, BoxError, BLOCK_NUM,
    SPECIAL_BLOCK_NUM,
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
                    if let Expr::Column(c) = expr {
                        if c.name() == SPECIAL_BLOCK_NUM {
                            // Unqualify the column.
                            c.relation = None;
                            return Ok(Transformed::yes(LogicalPlan::Projection(projection)));
                        }
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

/// If the original query does not select the `SPECIAL_BLOCK_NUM` column, this will
/// project the `SPECIAL_BLOCK_NUM` out of the plan. (Essentially, add a projection
/// on top of the query which selects all columns except `SPECIAL_BLOCK_NUM`.)
pub fn unproject_special_block_num_column(
    plan: LogicalPlan,
    original_schema: Arc<DFSchema>,
) -> Result<LogicalPlan, DataFusionError> {
    if original_schema
        .fields()
        .iter()
        .any(|f| f.name() == SPECIAL_BLOCK_NUM)
    {
        // If the original schema already contains the `SPECIAL_BLOCK_NUM` column, we don't need to
        // project it out.
        return Ok(plan);
    }

    let exprs = original_schema
        .fields()
        .iter()
        .map(|f| col(f.name()))
        .collect();
    let projection = Projection::try_new_with_schema(exprs, Arc::new(plan), original_schema)
        .map_err(|e| {
            internal!(
                "error while removing {} from projection: {}",
                SPECIAL_BLOCK_NUM,
                e
            )
        })?;
    Ok(LogicalPlan::Projection(projection))
}

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
            // Is it ok for this to be unqualified? Or should it be `TABLE_NAME.block_num`?
            let mut predicate = col(column_name).lt_eq(lit(end));
            predicate = predicate.and(lit(start).lt_eq(col(column_name)));

            let with_filter = Filter::try_new(predicate, Arc::new(node))?;
            Ok(Transformed::yes(LogicalPlan::Filter(with_filter)))
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
