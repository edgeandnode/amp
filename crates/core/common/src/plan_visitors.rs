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
                let node = if was { r.data.recompute_schema()? } else { r.data };
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
mod tests {
    use datafusion::{
        arrow::{
            array,
            datatypes::{DataType, Field, Schema},
        },
        common::Column,
        datasource::{MemTable, provider_as_source},
        logical_expr::{JoinType, LogicalPlanBuilder},
        physical_planner::PhysicalPlanner,
    };

    use super::*;
    use crate::block_num_udf::{BLOCK_NUM_UDF_SCHEMA_NAME, is_block_num_udf};

    // ── helpers ──────────────────────────────────────────────────────────────

    /// Simple single-table scan: id Int32, _block_num UInt64, value Int64.
    fn simple_scan(name: &str) -> LogicalPlan {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(RESERVED_BLOCK_NUM_COLUMN_NAME, DataType::UInt64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let batch = array::RecordBatch::new_empty(schema.clone());
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        LogicalPlanBuilder::scan(name, provider_as_source(Arc::new(table)), None)
            .unwrap()
            .build()
            .unwrap()
    }

    /// Constructs a `block_num()` sentinel UDF call expression.
    fn block_num_call() -> Expr {
        use datafusion::logical_expr::ScalarUDF;

        use crate::block_num_udf::BlockNumUdf;
        Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
            Arc::new(ScalarUDF::from(BlockNumUdf::new())),
            vec![],
        ))
    }

    /// Builds a logical plan from a SQL string.
    ///
    /// The `block_num()` UDF and a single table `t` (schema: id Int32,
    /// _block_num UInt64, value Int64) are pre-registered in the session.
    async fn sql_plan(sql: &str) -> LogicalPlan {
        use datafusion::{logical_expr::ScalarUDF, prelude::SessionContext};

        use crate::block_num_udf::BlockNumUdf;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(RESERVED_BLOCK_NUM_COLUMN_NAME, DataType::UInt64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let batch = array::RecordBatch::new_empty(schema.clone());
        let table = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());

        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(BlockNumUdf::new()));
        ctx.register_table("t", table).unwrap();

        ctx.sql(sql).await.unwrap().logical_plan().clone()
    }

    /// Runs a SQL query through `propagate_block_num` and executes it, returning the
    /// collected record batches.
    ///
    /// Table `t` is pre-populated with two rows:
    ///   id=1, _block_num=5,  value=100
    ///   id=2, _block_num=10, value=200
    async fn execute_propagated(sql: &str) -> Vec<array::RecordBatch> {
        use datafusion::{logical_expr::ScalarUDF, prelude::SessionContext};

        use crate::block_num_udf::BlockNumUdf;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(RESERVED_BLOCK_NUM_COLUMN_NAME, DataType::UInt64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(array::Int32Array::from(vec![1, 2])),
                Arc::new(array::UInt64Array::from(vec![5u64, 10u64])),
                Arc::new(array::Int64Array::from(vec![100i64, 200i64])),
            ],
        )
        .unwrap();
        let table = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());

        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(BlockNumUdf::new()));
        ctx.register_table("t", table).unwrap();

        let plan = ctx.sql(sql).await.unwrap().logical_plan().clone();
        let propagated = propagate_block_num(plan).unwrap();
        ctx.execute_logical_plan(propagated)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
    }

    /// Serializes record batches to a CSV string with a header row.
    fn to_csv(batches: &[array::RecordBatch]) -> String {
        use datafusion::arrow::csv::WriterBuilder;
        let mut buf = Vec::new();
        {
            let mut writer = WriterBuilder::new().with_header(true).build(&mut buf);
            for batch in batches {
                writer.write(batch).unwrap();
            }
        }
        String::from_utf8(buf).unwrap()
    }

    // ── Projection tests ──────────────────────────────────────────────────────

    #[test]
    fn test_propagate_auto_prepends_to_plain_projection() {
        // SELECT id, value FROM t  (no _block_num in select)
        // → _block_num is prepended as the first output column
        let plan = LogicalPlanBuilder::from(simple_scan("t"))
            .project(vec![col("id"), col("value")])
            .unwrap()
            .build()
            .unwrap();

        let result = propagate_block_num(plan).unwrap();

        let LogicalPlan::Projection(p) = &result else {
            panic!("expected Projection")
        };
        assert_eq!(p.expr.len(), 3, "_block_num + id + value");
        assert!(expr_outputs_block_num(&p.expr[0]));
    }

    #[test]
    fn test_propagate_no_prepend_when_block_num_col_in_projection() {
        // SELECT _block_num, id FROM t  (explicit backward-compat form)
        // → no duplicate; exactly one _block_num in output
        let plan = LogicalPlanBuilder::from(simple_scan("t"))
            .project(vec![col(RESERVED_BLOCK_NUM_COLUMN_NAME), col("id")])
            .unwrap()
            .build()
            .unwrap();

        let result = propagate_block_num(plan).unwrap();

        let LogicalPlan::Projection(p) = &result else {
            panic!("expected Projection")
        };
        assert_eq!(
            p.expr.iter().filter(|e| expr_outputs_block_num(e)).count(),
            1
        );
        assert_eq!(p.expr.len(), 2);
    }

    #[test]
    fn test_propagate_block_num_udf_in_projection_is_replaced() {
        // SELECT block_num(), id FROM t
        // Two-copies: block_num() is replaced in-place (preserving the "block_num()" output
        // name) AND _block_num is still prepended as the system column.
        let plan = LogicalPlanBuilder::from(simple_scan("t"))
            .project(vec![block_num_call(), col("id")])
            .unwrap()
            .build()
            .unwrap();

        let result = propagate_block_num(plan).unwrap();

        let LogicalPlan::Projection(p) = &result else {
            panic!("expected Projection")
        };
        assert_eq!(
            p.expr.len(),
            3,
            "_block_num prepended + block_num() preserved + id"
        );
        assert!(
            expr_outputs_block_num(&p.expr[0]),
            "_block_num system column at position 0"
        );
        assert!(
            matches!(&p.expr[1], Expr::Alias(a) if a.name == BLOCK_NUM_UDF_SCHEMA_NAME),
            "block_num() preserved at position 1"
        );
        assert!(!is_block_num_udf(&p.expr[1]), "UDF must be replaced");
    }

    #[test]
    fn test_propagate_nested_block_num_udf_still_auto_prepends() {
        // SELECT (block_num() + 1) AS offset, id FROM t
        // → block_num() is replaced inside the expression but the top-level
        //   output is "offset", not "_block_num", so _block_num is still prepended
        let block_num_plus_one =
            (block_num_call() + datafusion::prelude::lit(1u64)).alias("offset");
        let plan = LogicalPlanBuilder::from(simple_scan("t"))
            .project(vec![block_num_plus_one, col("id")])
            .unwrap()
            .build()
            .unwrap();

        let result = propagate_block_num(plan).unwrap();

        let LogicalPlan::Projection(p) = &result else {
            panic!("expected Projection")
        };
        assert_eq!(
            p.expr.len(),
            3,
            "_block_num prepended alongside offset + id"
        );
        assert!(expr_outputs_block_num(&p.expr[0]));
        // No block_num() UDF survives in any expression
        assert!(p.expr.iter().all(|e| !is_block_num_udf(e)));
    }

    fn assert_nested_block_num_is_bare_col(expr: &Expr) {
        let Expr::Alias(outer) = expr else { panic!("expected Alias, got {:?}", expr) };
        assert_eq!(outer.name, "offset");
        let Expr::BinaryExpr(bin) = outer.expr.as_ref() else {
            panic!("expected BinaryExpr inside alias")
        };
        assert!(
            matches!(bin.left.as_ref(), Expr::Column(c) if c.name == RESERVED_BLOCK_NUM_COLUMN_NAME),
            "nested block_num() must be replaced with bare col, got {:?}",
            bin.left
        );
    }

    #[test]
    fn test_nested_block_num_udf_replaced_without_alias_projection() {
        // SELECT (block_num() + 1) AS offset, id FROM t
        // block_num() is nested — replacement must be bare col("_block_num"), not
        // col("_block_num") AS "block_num()" (the alias is only meaningful at top level).
        let plan = LogicalPlanBuilder::from(simple_scan("t"))
            .project(vec![
                (block_num_call() + datafusion::prelude::lit(1u64)).alias("offset"),
                col("id"),
            ])
            .unwrap()
            .build()
            .unwrap();

        let result = propagate_block_num(plan).unwrap();

        let LogicalPlan::Projection(p) = &result else { panic!("expected Projection") };
        assert_nested_block_num_is_bare_col(&p.expr[1]);
    }

    #[test]
    fn test_nested_block_num_udf_replaced_without_alias_distinct_on() {
        // DISTINCT ON (block_num()) SELECT (block_num() + 1) AS offset, id FROM t
        let plan = LogicalPlanBuilder::from(simple_scan("t"))
            .distinct_on(
                vec![block_num_call()],
                vec![
                    (block_num_call() + datafusion::prelude::lit(1u64)).alias("offset"),
                    col("id"),
                ],
                None,
            )
            .unwrap()
            .build()
            .unwrap();

        let result = propagate_block_num(plan).unwrap();

        let LogicalPlan::Distinct(datafusion::logical_expr::Distinct::On(on)) = &result else {
            panic!("expected DistinctOn")
        };
        // on_expr[0] is the sort key — also unaliased
        assert!(expr_outputs_block_num(&on.on_expr[0]));
        // select_expr[0] is _block_num prepended; select_expr[1] is the nested expression
        assert_eq!(on.select_expr.len(), 3, "_block_num + offset + id");
        assert_nested_block_num_is_bare_col(&on.select_expr[1]);
    }

    // ── Aggregate tests ───────────────────────────────────────────────────────

    #[test]
    fn test_propagate_aggregate_outer_projection_gets_block_num_prepended() {
        // SELECT cnt FROM t GROUP BY _block_num
        // (outer Projection selects only cnt, not _block_num)
        // → _block_num prepended to the outer Projection
        use datafusion::functions_aggregate::count::count;
        let plan = LogicalPlanBuilder::from(simple_scan("t"))
            .aggregate(
                vec![col(RESERVED_BLOCK_NUM_COLUMN_NAME)],
                vec![count(col("id")).alias("cnt")],
            )
            .unwrap()
            .project(vec![col("cnt")])
            .unwrap()
            .build()
            .unwrap();

        let result = propagate_block_num(plan).unwrap();

        let LogicalPlan::Projection(p) = &result else {
            panic!("expected Projection")
        };
        assert_eq!(p.expr.len(), 2, "_block_num + cnt");
        assert!(expr_outputs_block_num(&p.expr[0]));
    }

    #[test]
    fn test_propagate_aggregate_outer_projection_already_has_block_num() {
        // SELECT _block_num, cnt FROM t GROUP BY _block_num
        // → outer Projection already has _block_num; no extra prepend
        use datafusion::functions_aggregate::count::count;
        let plan = LogicalPlanBuilder::from(simple_scan("t"))
            .aggregate(
                vec![col(RESERVED_BLOCK_NUM_COLUMN_NAME)],
                vec![count(col("id")).alias("cnt")],
            )
            .unwrap()
            .project(vec![col(RESERVED_BLOCK_NUM_COLUMN_NAME), col("cnt")])
            .unwrap()
            .build()
            .unwrap();

        let result = propagate_block_num(plan).unwrap();

        let LogicalPlan::Projection(p) = &result else {
            panic!("expected Projection")
        };
        assert_eq!(p.expr.len(), 2);
        assert_eq!(
            p.expr.iter().filter(|e| expr_outputs_block_num(e)).count(),
            1
        );
    }

    #[test]
    fn test_propagate_aggregate_block_num_udf_in_group_key_is_replaced() {
        // Verify that the block_num() UDF in an Aggregate's group key is replaced.
        //
        // Note: when a Projection above an Aggregate selects `block_num()`,
        // DataFusion's `columnize_expr` normalises the expression to a plain
        // `col("block_num()")` reference during plan construction — so the UDF
        // never exists in the Projection's expr list after building. We therefore
        // test the group-key replacement by keeping the Aggregate as the top-level
        // plan (no outer Projection that references the group-by output column).
        use datafusion::functions_aggregate::count::count;
        let plan = LogicalPlanBuilder::from(simple_scan("t"))
            .aggregate(vec![block_num_call()], vec![count(col("id")).alias("cnt")])
            .unwrap()
            .build()
            .unwrap();

        let result = propagate_block_num(plan).unwrap();

        // Aggregate is the top-level node; verify its group key was replaced
        let LogicalPlan::Aggregate(agg) = &result else {
            panic!("expected Aggregate")
        };
        assert!(expr_outputs_block_num(&agg.group_expr[0]));
        assert!(
            !is_block_num_udf(&agg.group_expr[0]),
            "UDF must be replaced in Aggregate"
        );
        assert!(
            result
                .schema()
                .fields()
                .iter()
                .any(|f| f.name() == RESERVED_BLOCK_NUM_COLUMN_NAME)
        );
    }

    #[test]
    fn test_propagate_aggregate_block_num_udf_in_group_key_outer_projection_gets_prepended() {
        // Outer Projection selects only cnt (not block_num()/‌_block_num).
        // After propagation the Aggregate group key is replaced and
        // _block_num is auto-prepended to the Projection.
        use datafusion::functions_aggregate::count::count;
        let plan = LogicalPlanBuilder::from(simple_scan("t"))
            .aggregate(vec![block_num_call()], vec![count(col("id")).alias("cnt")])
            .unwrap()
            .project(vec![col("cnt")])
            .unwrap()
            .build()
            .unwrap();

        let result = propagate_block_num(plan).unwrap();

        let LogicalPlan::Projection(p) = &result else {
            panic!("expected Projection")
        };
        assert_eq!(p.expr.len(), 2, "_block_num + cnt");
        assert!(expr_outputs_block_num(&p.expr[0]));

        // Aggregate's group key should also be replaced
        let LogicalPlan::Aggregate(agg) = p.input.as_ref() else {
            panic!("expected Aggregate under Projection")
        };
        assert!(expr_outputs_block_num(&agg.group_expr[0]));
        assert!(!is_block_num_udf(&agg.group_expr[0]));
    }

    #[test]
    fn test_propagate_aggregate_only_group_by_block_num_no_outer_projection() {
        // SELECT _block_num, COUNT(id) AS cnt FROM t GROUP BY _block_num
        // (Aggregate is the top-level plan, no extra Projection wrapping it)
        // → Aggregate output already has _block_num as first group key; plan unchanged
        use datafusion::functions_aggregate::count::count;
        let plan = LogicalPlanBuilder::from(simple_scan("t"))
            .aggregate(
                vec![col(RESERVED_BLOCK_NUM_COLUMN_NAME)],
                vec![count(col("id")).alias("cnt")],
            )
            .unwrap()
            .build()
            .unwrap();

        let result = propagate_block_num(plan).unwrap();

        assert!(
            result
                .schema()
                .fields()
                .iter()
                .any(|f| f.name() == RESERVED_BLOCK_NUM_COLUMN_NAME),
            "_block_num must be in the output schema"
        );
    }

    #[tokio::test]
    async fn test_propagate_aggregate_block_num_udf_in_select_and_group_key() {
        // SELECT block_num(), COUNT(id) AS cnt FROM t GROUP BY block_num()
        //
        // DataFusion's SQL planner normalises `block_num()` in the outer SELECT to
        // `col("block_num()")` — a plain Column reference to the Aggregate output.
        // `is_block_num_udf_or_normalized` handles this normalised form as well as the
        // raw UDF, so both the Aggregate group key and the Projection expression are replaced.
        //
        // Two-copies: the Projection gets _block_num prepended (system column) plus the
        // in-place replacement keeps the "block_num()" output name.
        //   Projection [ _block_num, _block_num AS "block_num()", cnt ]
        //     Aggregate group=[ _block_num ]
        let plan =
            sql_plan("SELECT block_num(), COUNT(id) AS cnt FROM t GROUP BY block_num()").await;

        let result = propagate_block_num(plan).unwrap();

        let LogicalPlan::Projection(p) = &result else {
            panic!("expected Projection")
        };
        assert_eq!(
            p.expr.len(),
            3,
            "_block_num prepended + block_num() preserved + cnt"
        );
        assert!(
            expr_outputs_block_num(&p.expr[0]),
            "_block_num at position 0"
        );
        assert!(
            matches!(&p.expr[1], Expr::Alias(a) if a.name == BLOCK_NUM_UDF_SCHEMA_NAME),
            "block_num() preserved at position 1"
        );

        let LogicalPlan::Aggregate(agg) = p.input.as_ref() else {
            panic!("expected Aggregate under Projection")
        };
        assert!(expr_outputs_block_num(&agg.group_expr[0]));
        assert!(!is_block_num_udf(&agg.group_expr[0]));
    }

    #[tokio::test]
    async fn test_sql_propagate_udf_in_simple_projection() {
        // SELECT block_num(), id FROM t
        //
        // In a plain SELECT (no GROUP BY), the SQL planner has no aggregate output column
        // named "block_num()" to normalise against, so the UDF is preserved as a
        // ScalarFunction in the Projection's expr list.
        //
        // Two-copies: _block_num prepended (system column) + block_num() preserved in-place.
        let plan = sql_plan("SELECT block_num(), id FROM t").await;
        eprintln!("plan before propagation:\n{}", plan.display_indent());

        let result = propagate_block_num(plan).unwrap();
        eprintln!("plan after propagation:\n{}", result.display_indent());

        let LogicalPlan::Projection(p) = &result else {
            panic!("expected Projection")
        };
        assert_eq!(
            p.expr.len(),
            3,
            "_block_num prepended + block_num() preserved + id"
        );
        assert!(
            expr_outputs_block_num(&p.expr[0]),
            "_block_num at position 0"
        );
        assert!(
            matches!(&p.expr[1], Expr::Alias(a) if a.name == BLOCK_NUM_UDF_SCHEMA_NAME),
            "block_num() preserved at position 1"
        );
        assert!(!is_block_num_udf(&p.expr[1]), "UDF must be replaced");
    }

    #[tokio::test]
    async fn test_sql_propagate_udf_in_projection_over_join() {
        // SELECT block_num(), a.id FROM t a JOIN t b ON a.id = b.id
        //
        // Two-copies: block_num() → greatest(a._block_num, b._block_num); _block_num
        // prepended as system column + block_num() preserved in-place with alias.
        let plan = sql_plan("SELECT block_num(), a.id FROM t a JOIN t b ON a.id = b.id").await;
        eprintln!("plan before propagation:\n{}", plan.display_indent());

        let result = propagate_block_num(plan).unwrap();
        eprintln!("plan after propagation:\n{}", result.display_indent());

        let LogicalPlan::Projection(p) = &result else {
            panic!("expected Projection")
        };
        assert_eq!(
            p.expr.len(),
            3,
            "_block_num prepended + block_num() preserved + a.id"
        );
        assert!(
            expr_outputs_block_num(&p.expr[0]),
            "_block_num at position 0"
        );
        assert!(
            !is_block_num_udf(&p.expr[1]),
            "UDF must be replaced with greatest(...)"
        );
        assert!(
            matches!(&p.expr[1], Expr::Alias(a) if a.name == BLOCK_NUM_UDF_SCHEMA_NAME),
            "block_num() preserved at position 1"
        );
    }

    // ── Distinct ON tests ─────────────────────────────────────────────────────

    #[test]
    fn test_propagate_distinct_on_block_num_in_select_no_prepend() {
        // DISTINCT ON (_block_num) SELECT _block_num, id  (_block_num already in SELECT)
        // → no prepend; select_expr is unchanged
        let plan = LogicalPlanBuilder::from(simple_scan("t"))
            .distinct_on(
                vec![col(RESERVED_BLOCK_NUM_COLUMN_NAME)],
                vec![col(RESERVED_BLOCK_NUM_COLUMN_NAME), col("id")],
                None,
            )
            .unwrap()
            .build()
            .unwrap();

        let result = propagate_block_num(plan).unwrap();

        let LogicalPlan::Distinct(datafusion::logical_expr::Distinct::On(on)) = &result else {
            panic!("expected DistinctOn")
        };
        assert_eq!(on.select_expr.len(), 2);
        assert_eq!(
            on.select_expr
                .iter()
                .filter(|e| expr_outputs_block_num(e))
                .count(),
            1
        );
    }

    #[test]
    fn test_propagate_distinct_on_block_num_not_in_select_gets_prepended() {
        // DISTINCT ON (_block_num) SELECT id, value  (no _block_num in SELECT)
        // → _block_num prepended to select_expr
        let plan = LogicalPlanBuilder::from(simple_scan("t"))
            .distinct_on(
                vec![col(RESERVED_BLOCK_NUM_COLUMN_NAME)],
                vec![col("id"), col("value")],
                None,
            )
            .unwrap()
            .build()
            .unwrap();

        let result = propagate_block_num(plan).unwrap();

        let LogicalPlan::Distinct(datafusion::logical_expr::Distinct::On(on)) = &result else {
            panic!("expected DistinctOn")
        };
        assert_eq!(on.select_expr.len(), 3, "_block_num + id + value");
        assert!(expr_outputs_block_num(&on.select_expr[0]));
        assert!(
            result
                .schema()
                .fields()
                .iter()
                .any(|f| f.name() == RESERVED_BLOCK_NUM_COLUMN_NAME)
        );
    }

    #[test]
    fn test_propagate_distinct_on_block_num_udf_in_on_expr_replaced_and_prepended_to_select() {
        // DISTINCT ON (block_num()) SELECT id, value
        // → block_num() replaced in on_expr (aliased as "block_num()");
        //   _block_num is NOT yet in select_expr so it is prepended
        let plan = LogicalPlanBuilder::from(simple_scan("t"))
            .distinct_on(vec![block_num_call()], vec![col("id"), col("value")], None)
            .unwrap()
            .build()
            .unwrap();

        let result = propagate_block_num(plan).unwrap();

        let LogicalPlan::Distinct(datafusion::logical_expr::Distinct::On(on)) = &result else {
            panic!("expected DistinctOn")
        };
        assert!(!is_block_num_udf(&on.on_expr[0]), "UDF must be replaced in on_expr");
        assert!(expr_outputs_block_num(&on.on_expr[0]), "on_expr references _block_num");
        assert_eq!(on.select_expr.len(), 3, "_block_num prepended: _block_num + id + value");
        assert!(expr_outputs_block_num(&on.select_expr[0]));
    }

    #[test]
    fn test_propagate_distinct_on_block_num_udf_in_select_two_copies() {
        // DISTINCT ON (block_num()) SELECT block_num(), id
        // on_expr: block_num() → _block_num (unaliased sort key)
        // Two-copies in select_expr: _block_num prepended + block_num() preserved.
        // → select_expr = [_block_num, _block_num AS "block_num()", id]
        let plan = LogicalPlanBuilder::from(simple_scan("t"))
            .distinct_on(
                vec![block_num_call()],
                vec![block_num_call(), col("id")],
                None,
            )
            .unwrap()
            .build()
            .unwrap();

        let result = propagate_block_num(plan).unwrap();

        let LogicalPlan::Distinct(datafusion::logical_expr::Distinct::On(on)) = &result else {
            panic!("expected DistinctOn")
        };
        assert_eq!(
            on.select_expr.len(),
            3,
            "_block_num prepended + block_num() preserved + id"
        );
        assert!(
            expr_outputs_block_num(&on.select_expr[0]),
            "_block_num at position 0"
        );
        assert!(
            matches!(&on.select_expr[1], Expr::Alias(a) if a.name == BLOCK_NUM_UDF_SCHEMA_NAME),
            "block_num() preserved at position 1"
        );
        assert!(!is_block_num_udf(&on.on_expr[0]));
        assert!(!is_block_num_udf(&on.select_expr[0]));
    }

    // ── Join + block_num() tests ──────────────────────────────────────────────

    #[test]
    fn test_propagate_block_num_udf_over_join_replaced_with_greatest() {
        // SELECT block_num(), foo.id FROM (foo JOIN bar ON foo.id = bar.id)
        // Two-copies: block_num() → greatest(foo._block_num, bar._block_num) aliased as
        // "block_num()" + _block_num (system column) prepended.
        let plan = LogicalPlanBuilder::from(simple_scan("foo"))
            .join(
                simple_scan("bar"),
                JoinType::Inner,
                (
                    vec![Column::from_qualified_name("foo.id")],
                    vec![Column::from_qualified_name("bar.id")],
                ),
                None,
            )
            .unwrap()
            .project(vec![block_num_call(), col("foo.id")])
            .unwrap()
            .build()
            .unwrap();

        let result = propagate_block_num(plan).unwrap();

        let LogicalPlan::Projection(p) = &result else {
            panic!("expected Projection")
        };
        assert_eq!(
            p.expr.len(),
            3,
            "_block_num prepended + block_num() preserved + foo.id"
        );
        assert!(
            expr_outputs_block_num(&p.expr[0]),
            "_block_num system column at position 0"
        );
        assert!(
            !is_block_num_udf(&p.expr[1]),
            "UDF replaced with greatest(...)"
        );
        assert!(
            matches!(&p.expr[1], Expr::Alias(a) if a.name == BLOCK_NUM_UDF_SCHEMA_NAME),
            "expected greatest(...) AS block_num() at position 1, got {:?}",
            p.expr[1]
        );
    }

    #[test]
    fn test_propagate_distinct_on_block_num_udf_over_join_replaced_with_greatest() {
        // DISTINCT ON (block_num()) SELECT block_num(), foo.id
        // FROM foo JOIN bar ON foo.id = bar.id
        // on_expr: block_num() → greatest(...) AS "_block_num" (unaliased sort key)
        // Two-copies in select_expr: _block_num prepended + block_num() preserved.
        // → select_expr = [greatest(...) AS "_block_num", greatest(...) AS "block_num()", foo.id]
        let plan = LogicalPlanBuilder::from(simple_scan("foo"))
            .join(
                simple_scan("bar"),
                JoinType::Inner,
                (
                    vec![Column::from_qualified_name("foo.id")],
                    vec![Column::from_qualified_name("bar.id")],
                ),
                None,
            )
            .unwrap()
            .distinct_on(
                vec![block_num_call()],
                vec![block_num_call(), col("foo.id")],
                None,
            )
            .unwrap()
            .build()
            .unwrap();

        let result = propagate_block_num(plan).unwrap();

        let LogicalPlan::Distinct(datafusion::logical_expr::Distinct::On(on)) = &result else {
            panic!("expected DistinctOn")
        };
        assert!(!is_block_num_udf(&on.on_expr[0]));
        assert!(expr_outputs_block_num(&on.on_expr[0]), "on_expr references _block_num");
        assert_eq!(
            on.select_expr.len(),
            3,
            "_block_num prepended + block_num() preserved + foo.id"
        );
        assert!(
            expr_outputs_block_num(&on.select_expr[0]),
            "_block_num at position 0"
        );
        assert!(
            matches!(&on.select_expr[1], Expr::Alias(a) if a.name == BLOCK_NUM_UDF_SCHEMA_NAME),
            "block_num() preserved at position 1"
        );
    }

    #[tokio::test]
    async fn test_propagate_block_num_with_qualified_wildcard() {
        // Create two tables that both contain RESERVED_BLOCK_NUM_COLUMN_NAME columns
        let foo_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(RESERVED_BLOCK_NUM_COLUMN_NAME, DataType::UInt64, false),
            Field::new("foo_value", DataType::Utf8, false),
        ]));

        let bar_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(RESERVED_BLOCK_NUM_COLUMN_NAME, DataType::UInt64, false),
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
        let invalid_projection_plan = LogicalPlanBuilder::from(join_plan.clone())
            .project(vec![
                col("foo.id"),
                col(format!("foo.{}", RESERVED_BLOCK_NUM_COLUMN_NAME)),
                col("foo.foo_value"),
            ])
            .unwrap()
            .build()
            .unwrap();

        // Selecting a qualified `_block_num` column (e.g. `foo._block_num`) in a multi-table
        // context (join) is forbidden by `forbid_underscore_prefixed_aliases`. Users should
        // use the `block_num()` sentinel UDF instead to get the correct propagated value.
        let result = propagate_block_num(invalid_projection_plan);
        assert!(
            result.is_err(),
            "selecting a qualified _block_num from a join should be rejected"
        );
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("block_num()"),
            "error message should suggest using block_num() UDF, got: {err_msg}"
        );

        // Project foo.* (now aliasing foo._block_num)
        let projection_plan = LogicalPlanBuilder::from(join_plan)
            .project(vec![
                col("foo.id"),
                col(format!("foo.{}", RESERVED_BLOCK_NUM_COLUMN_NAME)).alias("block_num"),
                col("foo.foo_value"),
            ])
            .unwrap()
            .build()
            .unwrap();

        let transformed_plan = propagate_block_num(projection_plan).unwrap();

        // Check that the plan was transformed (should be a Projection)
        match &transformed_plan {
            LogicalPlan::Projection(projection) => {
                // The first expression should be the RESERVED_BLOCK_NUM_COLUMN_NAME
                assert_eq!(projection.expr.len(), 4);

                // Check if the qualified column was properly aliased
                if let Expr::Alias(alias) = &projection.expr[2] {
                    assert_eq!(alias.name, "block_num", "Should alias to block_num");
                    if let Expr::Column(c) = alias.expr.as_ref() {
                        assert_eq!(
                            c.name, RESERVED_BLOCK_NUM_COLUMN_NAME,
                            "Should reference RESERVED_BLOCK_NUM_COLUMN_NAME (_block_num) column"
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

        // Check the schema to ensure RESERVED_BLOCK_NUM_COLUMN_NAME is present and correctly aliased
        let schema = transformed_plan.schema();
        assert!(
            schema
                .fields()
                .iter()
                .any(|f| f.name() == RESERVED_BLOCK_NUM_COLUMN_NAME),
            "Schema should contain the RESERVED_BLOCK_NUM_COLUMN_NAME field"
        );
    }

    #[test]
    fn prepend_special_block_num_field_with_various_schemas_behaves_correctly() {
        use datafusion::{
            arrow::datatypes::{DataType, Field, Schema},
            common::DFSchema,
        };

        // Test 1: Function adds _block_num field when schema doesn't have it
        let schema = DFSchema::from_unqualified_fields(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Utf8, false),
            ]
            .into(),
            Default::default(),
        )
        .unwrap();

        let result = prepend_special_block_num_field(&schema);

        assert_eq!(result.fields().len(), 3, "Should add _block_num field");
        assert_eq!(
            result.fields()[0].name(),
            RESERVED_BLOCK_NUM_COLUMN_NAME,
            "First field should be _block_num"
        );
        assert_eq!(result.fields()[1].name(), "id");
        assert_eq!(result.fields()[2].name(), "value");

        // Test 2: Function is idempotent (calling twice returns same schema)
        let result2 = prepend_special_block_num_field(&result);

        assert_eq!(
            result2.fields().len(),
            3,
            "Calling again should not add another field"
        );
        assert_eq!(
            result2.fields()[0].name(),
            RESERVED_BLOCK_NUM_COLUMN_NAME,
            "First field should still be _block_num"
        );

        // Test 3: Function skips adding field when _block_num already exists in schema
        let schema_with_block_num = DFSchema::from_unqualified_fields(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new(RESERVED_BLOCK_NUM_COLUMN_NAME, DataType::UInt64, false),
                Field::new("value", DataType::Utf8, false),
            ]
            .into(),
            Default::default(),
        )
        .unwrap();

        let result3 = prepend_special_block_num_field(&schema_with_block_num);

        assert_eq!(
            result3.fields().len(),
            3,
            "Should not add _block_num when it already exists"
        );
        assert!(
            result3
                .fields()
                .iter()
                .any(|f| f.name() == RESERVED_BLOCK_NUM_COLUMN_NAME),
            "Should still contain _block_num field"
        );

        // Test 4: Function skips adding field when qualified _block_num exists (e.g., foo._block_num)
        // This is the critical case mentioned in the comment: different qualifiers should be
        // considered as the same field for the purposes of this function.
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(RESERVED_BLOCK_NUM_COLUMN_NAME, DataType::UInt64, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let qualified_schema = DFSchema::try_from_qualified_schema("foo", &arrow_schema).unwrap();

        let result4 = prepend_special_block_num_field(&qualified_schema);

        assert_eq!(
            result4.fields().len(),
            3,
            "Should not add _block_num when qualified version (foo._block_num) exists"
        );
        assert!(
            result4
                .fields()
                .iter()
                .any(|f| f.name() == RESERVED_BLOCK_NUM_COLUMN_NAME),
            "Should still contain _block_num field"
        );
        // Verify the qualified field is preserved
        let (qualifier, _field) = result4
            .iter()
            .find(|(_, f)| f.name() == RESERVED_BLOCK_NUM_COLUMN_NAME)
            .unwrap();
        assert_eq!(
            qualifier.map(|q| q.to_string()),
            Some("foo".to_string()),
            "Qualified field should retain its qualifier"
        );
    }

    #[tokio::test]
    async fn test_forbid_duplicate_field_names() {
        // Create a logical plan with duplicate field names
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let partition = array::RecordBatch::new_empty(schema.clone());

        let table = Arc::new(MemTable::try_new(schema.clone(), vec![vec![partition]]).unwrap());

        let a_scan = LogicalPlanBuilder::scan("a", provider_as_source(table.clone()), None)
            .unwrap()
            .build()
            .unwrap();
        let b_scan = LogicalPlanBuilder::scan("b", provider_as_source(table), None)
            .unwrap()
            .build()
            .unwrap();

        let plan = LogicalPlanBuilder::from(a_scan)
            .join(
                b_scan,
                JoinType::Inner,
                (
                    vec![Column::from_qualified_name("a.id")],
                    vec![Column::from_qualified_name("b.id")],
                ),
                None,
            )
            .unwrap()
            .project(vec![
                col("a.id"),
                col("a.value"),
                col("b.value"), // This will create a duplicate "value" field
            ])
            .unwrap()
            .build()
            .unwrap();

        let ctx = datafusion::prelude::SessionContext::new();
        let state = ctx.state();
        let physical_plan = datafusion::physical_planner::DefaultPhysicalPlanner::default()
            .create_physical_plan(&plan, &state)
            .await
            .unwrap();
        let result = forbid_duplicate_field_names(&physical_plan, &plan);

        assert!(
            result.is_err(),
            "forbid_duplicate_field_names should fail with duplicate field names"
        );

        let err_msg = result.err().unwrap().to_string();
        assert!(
            err_msg.contains("Duplicate field names detected in plan schema"),
            "Error message should indicate duplicate field names"
        );
    }

    #[test]
    fn test_forbid_underscore_alias_in_projection() {
        let plan = LogicalPlanBuilder::from(simple_scan("t"))
            .project(vec![col("id").alias("_sneaky")])
            .unwrap()
            .build()
            .unwrap();
        let err = forbid_underscore_prefixed_aliases(&plan)
            .unwrap_err()
            .to_string();
        assert!(err.contains("_sneaky"), "error should name the alias");
    }

    #[test]
    fn test_forbid_underscore_alias_in_distinct_on_select() {
        // `value AS "_block_num"` in a DISTINCT ON select-list is the precise gap that
        // previously existed: forbid_underscore_prefixed_aliases only checked Projections,
        // so this alias would have fooled expr_outputs_block_num into skipping the prepend.
        let plan = LogicalPlanBuilder::from(simple_scan("t"))
            .distinct_on(
                vec![col("id")],
                vec![
                    col("id"),
                    col("value").alias(RESERVED_BLOCK_NUM_COLUMN_NAME),
                ],
                None,
            )
            .unwrap()
            .build()
            .unwrap();
        let err = forbid_underscore_prefixed_aliases(&plan)
            .unwrap_err()
            .to_string();
        assert!(err.contains(RESERVED_BLOCK_NUM_COLUMN_NAME));
    }

    #[test]
    fn test_propagate_block_num_through_subquery_alias_in_join() {
        // This test verifies that _block_num is properly propagated through SubqueryAlias nodes
        // when used in JOINs. This simulates the CTE case where a user writes:
        //   WITH test AS (SELECT block_num FROM transactions WHERE value > 0)
        //   SELECT tx.tx_hash, t.block_num FROM test t, transactions tx
        // Without explicit _block_num in the CTE.

        // Create a table with _block_num
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(RESERVED_BLOCK_NUM_COLUMN_NAME, DataType::UInt64, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let batch = array::RecordBatch::new_empty(schema.clone());
        let table = Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap());

        // Create a scan and project WITHOUT _block_num (simulating a CTE that doesn't select it)
        let scan = LogicalPlanBuilder::scan("txs", provider_as_source(table.clone()), None)
            .unwrap()
            .project(vec![col("id"), col("value")]) // Note: no _block_num!
            .unwrap()
            .build()
            .unwrap();

        // Wrap in SubqueryAlias to simulate a CTE
        let cte_alias = SubqueryAliasStruct::try_new(Arc::new(scan), "cte").unwrap();
        let cte_plan = LogicalPlan::SubqueryAlias(cte_alias);

        // Create another scan for the right side of join
        let right_scan = LogicalPlanBuilder::scan("txs2", provider_as_source(table), None)
            .unwrap()
            .build()
            .unwrap();

        // Create a cross join between CTE and the table
        let join_plan = LogicalPlanBuilder::from(cte_plan)
            .join(
                right_scan,
                JoinType::Inner,
                (
                    vec![Column::from_qualified_name("cte.id")],
                    vec![Column::from_qualified_name("txs2.id")],
                ),
                None,
            )
            .unwrap()
            .project(vec![col("cte.value"), col("txs2.value")])
            .unwrap()
            .build()
            .unwrap();

        // This should succeed now that SubqueryAlias properly propagates _block_num
        let result = propagate_block_num(join_plan);
        assert!(
            result.is_ok(),
            "propagate_block_num should succeed for SubqueryAlias in JOIN: {:?}",
            result.err()
        );

        // Verify that the resulting plan has _block_num in the schema
        let plan = result.unwrap();
        assert!(
            plan.schema()
                .fields()
                .iter()
                .any(|f| f.name() == RESERVED_BLOCK_NUM_COLUMN_NAME),
            "Resulting plan should have _block_num in schema"
        );
    }

    // ── Output schema + data tests ────────────────────────────────────────────

    #[tokio::test]
    async fn test_propagation_adds_system_block_num_column() {
        // Propagation adds _block_num as the system column in addition to preserving
        // the user's block_num() output name ("two copies").
        //   before: ["block_num()", "t.id"]
        //   after:  ["_block_num", "block_num()", "t.id"]
        let before = sql_plan("SELECT block_num(), id FROM t").await;
        let after = propagate_block_num(before.clone()).unwrap();
        let before_fields = before.schema().field_names();
        let after_fields = after.schema().field_names();
        // Pre-propagation has the UDF name but not the system column
        assert!(before_fields.contains(&BLOCK_NUM_UDF_SCHEMA_NAME.to_string()));
        assert!(!before_fields.contains(&RESERVED_BLOCK_NUM_COLUMN_NAME.to_string()));
        // Post-propagation has both
        assert!(after_fields.contains(&BLOCK_NUM_UDF_SCHEMA_NAME.to_string()));
        assert!(after_fields.contains(&RESERVED_BLOCK_NUM_COLUMN_NAME.to_string()));
    }

    #[tokio::test]
    async fn test_propagate_output_simple_projection() {
        // Two-copies: _block_num (system) + block_num() (preserved user column) + id.
        let batches = execute_propagated("SELECT block_num(), id FROM t").await;
        let csv = to_csv(&batches);
        assert_eq!(csv, "_block_num,block_num(),id\n5,5,1\n10,10,2\n");
    }

    #[tokio::test]
    async fn test_propagate_output_join() {
        // Two-copies over a self-join: greatest(_block_num, _block_num) appears as both
        // the system _block_num column and the preserved block_num() column.
        let batches =
            execute_propagated("SELECT block_num(), a.id FROM t a JOIN t b ON a.id = b.id").await;
        let csv = to_csv(&batches);
        assert_eq!(csv, "_block_num,block_num(),id\n5,5,1\n10,10,2\n");
    }
}
