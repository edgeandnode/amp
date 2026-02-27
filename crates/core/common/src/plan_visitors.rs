use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
};

use datafusion::{
    common::{
        Column, JoinType, plan_err,
        tree_node::{Transformed, TreeNode as _, TreeNodeRecursion, TreeNodeRewriter},
    },
    error::DataFusionError,
    functions::core::expr_fn::greatest,
    logical_expr::{
        Join as JoinStruct, LogicalPlan, LogicalPlanBuilder, Sort,
        SubqueryAlias as SubqueryAliasStruct, Union as UnionStruct,
    },
    physical_plan::ExecutionPlan,
    prelude::{Expr, col, lit},
    sql::{TableReference, utils::UNNEST_PLACEHOLDER},
};
use datasets_common::{block_num::RESERVED_BLOCK_NUM_COLUMN_NAME, network_id::NetworkId};

use crate::{
    block_num_udf::{
        BLOCK_NUM_UDF_SCHEMA_NAME, expr_outputs_block_num, is_block_num_udf_or_normalized,
    },
    incrementalizer::{NonIncrementalQueryError, incremental_op_kind},
};

/// Helper function to create a column reference to `_block_num`
fn block_num_col() -> Expr {
    col(RESERVED_BLOCK_NUM_COLUMN_NAME)
}

/// Rejects a user plan that violates either of two rules needed for safe `_block_num`
/// propagation:
///
/// 1. **No `_`-prefixed aliases** — `_block_num` and all other `_…` names are reserved.
///    The check walks every expression in the entire plan tree exhaustively
///    (`node.expressions()` + `Expr::apply`), so no node type can be missed.
///
/// 2. **No bare `col("_block_num")` in multi-table Projections** — over a join there
///    is one `_block_num` per side; a bare column reference picks one arbitrarily.
///    Users must write `block_num()` instead, which the propagator replaces with
///    `greatest(left._block_num, right._block_num)`.
///
/// Rule 1 establishes the invariant that [`expr_outputs_block_num`] relies on: any
/// expression whose `physical_name` is `"_block_num"` in a validated plan is a genuine
/// column tracing back to a real source, never a user alias — so the propagator can
/// safely use it as the "already has `_block_num`" signal.
pub fn forbid_underscore_prefixed_aliases(plan: &LogicalPlan) -> Result<(), DataFusionError> {
    plan.apply(|node| {
        node.apply_expressions(|expr| {
            expr.apply(|e| {
                if let Expr::Alias(alias) = e
                    && alias.name.starts_with('_')
                    && !alias.name.starts_with(UNNEST_PLACEHOLDER)
                // DF built-in we want to allow
                {
                    return plan_err!(
                        "expression contains a column alias starting with '_': '{}'. \
                         Underscore-prefixed names are reserved. Please rename your column",
                        alias.name
                    );
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })?;

        if let LogicalPlan::Projection(projection) = node {
            let input_schema = projection.input.schema();
            let input_qualifiers: BTreeSet<&TableReference> =
                input_schema.iter().filter_map(|(q, _)| q).collect();
            if input_qualifiers.len() > 1 {
                for expr in projection.expr.iter() {
                    if matches!(expr, Expr::Column(c) if c.name == RESERVED_BLOCK_NUM_COLUMN_NAME) {
                        return plan_err!(
                            "selecting `{}` from a multi-table context (e.g. a join) is ambiguous. \
                             Use the `block_num()` function instead to get the correct value",
                            RESERVED_BLOCK_NUM_COLUMN_NAME
                        );
                    }
                }
            }
        }

        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

/// Ensures that there are no duplicate field names in the plan's schema.
/// This includes fields that are qualified with different table names.
/// For example, `table1.column` and `table2.column` would be considered duplicates
/// because they both refer to `column`.
pub fn forbid_duplicate_field_names(
    physical_plan: &Arc<dyn ExecutionPlan>,
    logical_plan: &LogicalPlan,
) -> Result<(), DataFusionError> {
    let schema = physical_plan.schema();
    let mut duplicates: Vec<Vec<Column>> = Vec::new();
    let mut seen = BTreeSet::new();
    for field in schema.fields() {
        let name = field.name();
        if !seen.insert(name.as_str()) {
            let sources = logical_plan.schema().columns_with_unqualified_name(name);
            duplicates.push(sources);
        }
    }

    if !duplicates.is_empty() {
        return plan_err!(
            "Duplicate field names detected in plan schema: [{}]. Please alias your columns to be unique.",
            duplicates
                .into_iter()
                .map(|cols| {
                    cols.into_iter()
                        .map(|c| c.to_string())
                        .collect::<Vec<_>>()
                        .join(" and ")
                })
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    Ok(())
}

/// The expression `greatest(left._block_num, right._block_num)`
fn block_num_for_join(join: &JoinStruct) -> Result<Expr, DataFusionError> {
    // Get the schema field names from left and right inputs
    let left_schema = join.left.schema();
    let right_schema = join.right.schema();

    // Find the qualified _block_num columns from each side
    let left_block_num = left_schema
        .iter()
        .find(|(_, field)| field.name() == RESERVED_BLOCK_NUM_COLUMN_NAME)
        .map(|(qualifier, _)| {
            col(Column::new(
                qualifier.cloned(),
                RESERVED_BLOCK_NUM_COLUMN_NAME,
            ))
        })
        .ok_or_else(|| {
            df_err(format!(
                "Left side of join missing {RESERVED_BLOCK_NUM_COLUMN_NAME}"
            ))
        })?;

    let right_block_num = right_schema
        .iter()
        .find(|(_, field)| field.name() == RESERVED_BLOCK_NUM_COLUMN_NAME)
        .map(|(qualifier, _)| {
            col(Column::new(
                qualifier.cloned(),
                RESERVED_BLOCK_NUM_COLUMN_NAME,
            ))
        })
        .ok_or_else(|| {
            df_err(format!(
                "Right side of join missing {RESERVED_BLOCK_NUM_COLUMN_NAME}"
            ))
        })?;

    Ok(greatest(vec![left_block_num, right_block_num]))
}

/// Rewriter that propagates the `RESERVED_BLOCK_NUM_COLUMN_NAME` column through the logical plan.
struct BlockNumPropagator {
    // State variable of the transformation.
    // This is the block num value being bubbled up to be applied in the next projection as:
    // `<block_num_expr> as _block_num`
    next_block_num_expr: Option<Expr>,
}

impl BlockNumPropagator {
    fn new() -> Self {
        Self {
            next_block_num_expr: None,
        }
    }
}

impl TreeNodeRewriter for BlockNumPropagator {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>, DataFusionError> {
        use LogicalPlan::*;

        // Check that the op is actually incremental
        let op_kind =
            incremental_op_kind(&node).map_err(|e| DataFusionError::External(e.into()))?;

        // Step 1: Replace block_num() UDF in all expressions of this node using
        // the currently accumulated _block_num expression. For setter nodes
        // (TableScan, Join, etc.) block_num() won't appear in their expressions
        // (we run before optimization), so this is effectively a no-op for them.
        //
        // Unwrap: `next_block_num_expr` is only `None` at initialization; any
        // leaf node (TableScan, EmptyRelation, Values) unconditionally sets it.
        // The unwrap_or fallback is only reached for the leaf nodes themselves,
        // where block_num() cannot appear, so the replacement is always a no-op.
        let block_num_expr = {
            let raw = self
                .next_block_num_expr
                .clone()
                .unwrap_or_else(block_num_col);
            if raw != block_num_col() {
                raw.alias(RESERVED_BLOCK_NUM_COLUMN_NAME)
            } else {
                raw
            }
        };
        // Step 1 replaces block_num() UDF in expressions.
        //
        // Output-column positions (Projection.expr, DistinctOn.select_expr) use
        // `replace_udf_select`: when block_num() IS the entire expression it gets
        // aliased as BLOCK_NUM_UDF_SCHEMA_NAME (preserving the user's output column
        // name); when block_num() is nested inside a larger expression (e.g.
        // `block_num() + 1`) the alias would be swallowed by the outer expression
        // anyway, so the plain unaliased replacement is used there.
        //
        // All other positions (sort keys, Aggregate group keys, Filter …) always use
        // the unaliased replacement via `replace_udf`.
        use datafusion::logical_expr::Distinct as DistinctKind;
        let select_replacement = block_num_expr.clone().alias(BLOCK_NUM_UDF_SCHEMA_NAME);
        let replace_udf = |e: Expr, repl: &Expr| {
            e.transform(|e| {
                if is_block_num_udf_or_normalized(&e) {
                    Ok(Transformed::yes(repl.clone()))
                } else {
                    Ok(Transformed::no(e))
                }
            })
        };
        let replace_udf_select = |e: Expr| -> Result<Transformed<Expr>, DataFusionError> {
            if is_block_num_udf_or_normalized(&e) {
                Ok(Transformed::yes(select_replacement.clone()))
            } else {
                replace_udf(e, &block_num_expr)
            }
        };
        let (was_replaced, node) = match node {
            Distinct(DistinctKind::On(mut on)) => {
                // on_expr / sort_expr are sort keys — unaliased.
                // select_expr is named output — top-level-aware alias.
                let mut changed = false;
                for e in std::mem::take(&mut on.on_expr) {
                    let t = replace_udf(e, &block_num_expr)?;
                    changed |= t.transformed;
                    on.on_expr.push(t.data);
                }
                for e in std::mem::take(&mut on.select_expr) {
                    let t = replace_udf_select(e)?;
                    changed |= t.transformed;
                    on.select_expr.push(t.data);
                }
                if let Some(sort_exprs) = on.sort_expr.take() {
                    let mut new_sort = Vec::with_capacity(sort_exprs.len());
                    for mut s in sort_exprs {
                        let t = replace_udf(s.expr, &block_num_expr)?;
                        changed |= t.transformed;
                        s.expr = t.data;
                        new_sort.push(s);
                    }
                    on.sort_expr = Some(new_sort);
                }
                let node = if changed {
                    Distinct(DistinctKind::On(on)).recompute_schema()?
                } else {
                    Distinct(DistinctKind::On(on))
                };
                (changed, node)
            }
            node => {
                let is_projection = matches!(node, Projection(_));
                let r = node.map_expressions(|e| {
                    if is_projection {
                        replace_udf_select(e)
                    } else {
                        replace_udf(e, &block_num_expr)
                    }
                })?;
                let was = r.transformed;
                let node = if was {
                    r.data.recompute_schema()?
                } else {
                    r.data
                };
                (was, node)
            }
        };

        // Step 2: Handle structural changes — consuming/setting next_block_num_expr,
        // prepending _block_num to outputs, schema fixes, sanity checks.
        match node {
            Projection(mut projection) => {
                // Consume next_block_num_expr: reset to block_num_col() so parent nodes
                // see a simple _block_num column reference from this projection's output.
                self.next_block_num_expr = Some(block_num_col());

                // If any top-level expression already outputs _block_num, skip auto-prepend.
                // This covers backward-compat `SELECT _block_num, ...` on a single table,
                // and `SELECT block_num(), ...` after UDF replacement above.
                if projection.expr.iter().any(expr_outputs_block_num) {
                    return Ok(Transformed::new_transformed(
                        LogicalPlan::Projection(projection),
                        was_replaced,
                    ));
                }

                // Auto-prepend the _block_num expression.
                projection.expr.insert(0, block_num_expr);
                projection.schema = prepend_special_block_num_field(&projection.schema);
                Ok(Transformed::yes(LogicalPlan::Projection(projection)))
            }

            // Rebuild union schemas to match their child projections
            Union(union) => {
                // Sanity check
                if self.next_block_num_expr != Some(block_num_col()) {
                    return Err(df_err(format!(
                        "unexpected `next_block_num_expr`: {:?}",
                        self.next_block_num_expr
                    )));
                }

                Ok(Transformed::yes(Union(UnionStruct::try_new(union.inputs)?)))
            }

            Join(ref join) => {
                self.next_block_num_expr = Some(block_num_for_join(join)?);
                Ok(Transformed::new_transformed(node, was_replaced))
            }

            TableScan(ref scan) => {
                // We run this before optimizations, so we can assume the projection to be empty
                if scan.projection.is_some() {
                    return Err(df_err(format!("Scan should not have projection: {scan:?}")));
                }
                self.next_block_num_expr = Some(block_num_col());
                Ok(Transformed::new_transformed(node, was_replaced))
            }

            // Constants are formally produced "before block 0" but hopefully it's correct enough to assign them 0.
            EmptyRelation(_) | Values(_) => {
                self.next_block_num_expr = Some(lit(0));
                Ok(Transformed::new_transformed(node, was_replaced))
            }

            // SubqueryAlias caches its schema - we need to rebuild it to reflect schema changes in its input.
            // When the child projection gets _block_num prepended, the SubqueryAlias must be rebuilt
            // so its cached schema includes _block_num. Otherwise, JOINs will fail to find _block_num.
            SubqueryAlias(subquery_alias) => {
                let rebuilt =
                    SubqueryAliasStruct::try_new(subquery_alias.input, subquery_alias.alias)?;
                Ok(Transformed::yes(LogicalPlan::SubqueryAlias(rebuilt)))
            }

            // These nodes do not cache schema and are not leaves. block_num() UDF in their
            // expressions (e.g. `WHERE block_num() > 100`) is handled by the replacement above.
            Filter(_) | Repartition(_) | Subquery(_) | Explain(_) | Analyze(_)
            | DescribeTable(_) | Unnest(_) => Ok(Transformed::new_transformed(node, was_replaced)),

            // DISTINCT ON (_block_num) is incrementally valid: the DistinctOn node has a
            // cached schema field that must be rebuilt to include _block_num after propagation.
            Distinct(distinct) => {
                match distinct {
                    DistinctKind::On(mut on) => {
                        // Consume next_block_num_expr.
                        self.next_block_num_expr = Some(block_num_col());

                        // Skip auto-prepend only when _block_num is already explicitly present.
                        if on.select_expr.iter().any(expr_outputs_block_num) {
                            return Ok(Transformed::new_transformed(
                                LogicalPlan::Distinct(DistinctKind::On(on)),
                                was_replaced,
                            ));
                        }

                        // Prepend _block_num to select_expr and rebuild the cached schema.
                        on.select_expr.insert(0, block_num_expr);
                        on.schema = prepend_special_block_num_field(&on.schema);
                        Ok(Transformed::yes(LogicalPlan::Distinct(DistinctKind::On(
                            on,
                        ))))
                    }
                    DistinctKind::All(_) => Err(df_err(format!(
                        "incremental_op_kind should have already rejected Distinct::All: {:?}",
                        op_kind
                    ))),
                }
            }

            // GROUP BY _block_num, ... — _block_num is already the first group key so it
            // appears naturally in the aggregate's output schema. Just update
            // next_block_num_expr for the parent and leave the node unchanged.
            Aggregate(_) => {
                self.next_block_num_expr = Some(block_num_col());
                Ok(Transformed::new_transformed(node, was_replaced))
            }

            // These variants would have already errored in `incremental_op_kind` above
            Limit(_) | Sort(_) | Window(_) | RecursiveQuery(_) | Statement(_) | Dml(_) | Ddl(_)
            | Copy(_) | Extension(_) => Err(df_err(format!(
                "incremental_op_kind should have already rejected this node type: {:?}",
                op_kind
            ))),
        }
    }
}

/// Propagate the `RESERVED_BLOCK_NUM_COLUMN_NAME` column through the logical plan.
pub fn propagate_block_num(plan: LogicalPlan) -> Result<LogicalPlan, DataFusionError> {
    // The transformation relies on the invariants of `forbid_underscore_prefixed_aliases`
    // to prevent conflicts between user-selected columns and the propagated `_block_num` column.
    forbid_underscore_prefixed_aliases(&plan)?;
    let mut propagator = BlockNumPropagator::new();
    plan.rewrite(&mut propagator).map(|t| t.data)
}

/// This will project the `RESERVED_BLOCK_NUM_COLUMN_NAME` out of the plan by adding a projection on top of the
/// query which selects all columns except `RESERVED_BLOCK_NUM_COLUMN_NAME`.
pub fn unproject_special_block_num_column(
    plan: LogicalPlan,
) -> Result<LogicalPlan, DataFusionError> {
    let fields = plan.schema().fields();
    if !fields
        .iter()
        .any(|f| f.name() == RESERVED_BLOCK_NUM_COLUMN_NAME)
    {
        // Nothing to do.
        return Ok(plan);
    }
    let expr = plan
        .schema()
        .iter()
        .filter(|(_, field)| field.name() != RESERVED_BLOCK_NUM_COLUMN_NAME)
        .map(Expr::from)
        .collect::<Vec<_>>();

    let builder = LogicalPlanBuilder::from(plan);

    builder.project(expr)?.build()
}

/// Reasons why a logical plan cannot be materialized incrementally
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NonIncrementalOp {
    /// Limit requires counting rows across batches
    Limit,
    /// Aggregations need state management
    Aggregate,
    /// Distinct operations need global deduplication
    Distinct,
    /// Outer joins are not incremental
    Join(JoinType),
    /// Sorts require seeing all data
    Sort,
    /// Window functions often require sorting and state
    Window,
    /// Recursive queries are inherently stateful
    RecursiveQuery,
}

impl fmt::Display for NonIncrementalOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use NonIncrementalOp::*;
        match self {
            Limit => write!(f, "Limit"),
            Aggregate => write!(f, "Aggregate"),
            Distinct => write!(f, "Distinct"),
            Join(join_type) => write!(f, "Join({})", join_type),
            Sort => write!(f, "Sort"),
            Window => write!(f, "Window"),
            RecursiveQuery => write!(f, "RecursiveQuery"),
        }
    }
}

/// Returns `Ok(())` if the given logical plan can be synced incrementally, `Err` otherwise.
pub fn is_incremental(plan: &LogicalPlan) -> Result<(), NonIncrementalQueryError> {
    let mut err: Option<NonIncrementalQueryError> = None;

    // TODO: Detect unsupported join stacking, possibly by doing a dry run of the incrementalizer.
    plan.exists(|node| match incremental_op_kind(node) {
        Ok(_) => Ok(false),
        Err(e) => {
            err = Some(e);
            Ok(true)
        }
    })
    .map_err(|df_err| NonIncrementalQueryError::Invalid(df_err.to_string()))?;

    if let Some(err) = err {
        return Err(err);
    }

    Ok(())
}

pub fn extract_table_references_from_plan(
    plan: &LogicalPlan,
) -> Result<Vec<TableReference>, DataFusionError> {
    let mut refs = BTreeSet::new();

    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            refs.insert(scan.table_name.clone());
        }

        Ok(TreeNodeRecursion::Continue)
    })?;

    Ok(refs.into_iter().collect())
}

/// Information about a cross-network join detected in the logical plan.
///
/// A cross-network join occurs when the left and right inputs of a join reference
/// tables from different blockchain networks. This is not supported for streaming
/// queries because blocks from different chains cannot be synchronized.
#[derive(Debug, Clone)]
pub struct CrossNetworkJoinInfo {
    /// Networks involved in the cross-network join
    pub networks: BTreeSet<datasets_common::network_id::NetworkId>,
}

impl fmt::Display for CrossNetworkJoinInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "join across multiple networks: {:?}", self.networks)
    }
}

/// Checks if any join in the logical plan crosses multiple networks.
///
/// Streaming queries cannot join tables from different networks because blocks
/// from different chains cannot be synchronized. Returns the first cross-network
/// join found, or `None` if all joins operate within a single network.
pub fn find_cross_network_join(
    plan: &LogicalPlan,
    catalog: &crate::catalog::physical::Catalog,
) -> Result<Option<CrossNetworkJoinInfo>, DataFusionError> {
    let table_to_network: BTreeMap<TableReference, NetworkId> = catalog
        .entries()
        .iter()
        .map(|t| (t.table_ref().into(), t.physical_table().network().clone()))
        .collect();

    let reference_networks =
        |subtree: &LogicalPlan| -> Result<BTreeSet<NetworkId>, DataFusionError> {
            let table_refs = extract_table_references_from_plan(subtree)?;
            Ok(table_refs
                .into_iter()
                .filter_map(|table_ref| table_to_network.get(&table_ref).cloned())
                .collect())
        };

    let mut cross_network_join: Option<CrossNetworkJoinInfo> = None;

    plan.apply(|node| {
        if cross_network_join.is_some() {
            return Ok(TreeNodeRecursion::Stop);
        }

        if let LogicalPlan::Join(join) = node {
            let mut networks: BTreeSet<NetworkId> = Default::default();
            networks.extend(reference_networks(&join.left)?);
            networks.extend(reference_networks(&join.right)?);

            if networks.len() > 1 {
                cross_network_join = Some(CrossNetworkJoinInfo { networks });
                return Ok(TreeNodeRecursion::Stop);
            }
        }

        Ok(TreeNodeRecursion::Continue)
    })?;

    Ok(cross_network_join)
}

pub fn order_by_block_num(plan: LogicalPlan) -> LogicalPlan {
    let sort = Sort {
        expr: vec![block_num_col().sort(true, false)],
        input: Arc::new(plan),
        fetch: None,
    };
    LogicalPlan::Sort(sort)
}

pub fn prepend_special_block_num_field(
    schema: &datafusion::common::DFSchema,
) -> Arc<datafusion::common::DFSchema> {
    use datafusion::arrow::datatypes::{DataType, Field, Fields};

    // Do nothing if a field with the same name is already present. Note that this
    // is not redundant with `DFSchema::merge`, because that will consider
    // different qualifiers as different fields even if the name is the same.
    if schema
        .fields()
        .iter()
        .any(|f| f.name() == RESERVED_BLOCK_NUM_COLUMN_NAME)
    {
        return Arc::new(schema.clone());
    }

    let mut new_schema = datafusion::common::DFSchema::from_unqualified_fields(
        Fields::from(vec![Field::new(
            RESERVED_BLOCK_NUM_COLUMN_NAME,
            DataType::UInt64,
            false,
        )]),
        Default::default(),
    )
    .unwrap();
    new_schema.merge(schema);
    new_schema.into()
}

fn df_err(msg: String) -> DataFusionError {
    DataFusionError::External(msg.into())
}

#[cfg(test)]
mod tests;
