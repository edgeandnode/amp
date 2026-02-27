use std::{
    collections::{BTreeMap, BTreeSet},
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
        SubqueryAlias as SubqueryAliasStruct, Union as UnionStruct, expr::physical_name,
    },
    physical_plan::ExecutionPlan,
    prelude::{Expr, col, lit},
    sql::{TableReference as DFTableReference, utils::UNNEST_PLACEHOLDER},
};
use datasets_common::{block_num::RESERVED_BLOCK_NUM_COLUMN_NAME, network_id::NetworkId};

use crate::{
    incrementalizer::{NonIncrementalQueryError, incremental_op_kind},
    sql::TableReference,
};

/// Helper function to create a column reference to `_block_num`
fn block_num_col() -> Expr {
    col(RESERVED_BLOCK_NUM_COLUMN_NAME)
}

/// Aliases with a name starting with `_` are always forbidden, since underscore-prefixed
/// names are reserved for special columns.
pub fn forbid_underscore_prefixed_aliases(plan: &LogicalPlan) -> Result<(), DataFusionError> {
    plan.apply(|node| {
        if let LogicalPlan::Projection(projection) = node {
            for expr in projection.expr.iter() {
                if let Expr::Alias(alias) = expr
                    && alias.name.starts_with('_')
                    && !alias.name.starts_with(UNNEST_PLACEHOLDER) // DF built-in we want to allow
                     {
                        return plan_err!(
                            "projection contains a column alias starting with '_': '{}'. Underscore-prefixed names are reserved. Please rename your column",
                            alias.name
                        );
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

        match node {
            Projection(mut projection) => {
                let block_num_expr = {
                    // Set the `next_block_num_expr` and take the current one.
                    //
                    // Unwrap: `next_block_num_expr` is only `None` at initialization.
                    // When visiting any leaf plan, we unconditionally set it.
                    let mut expr = self.next_block_num_expr.replace(block_num_col()).unwrap();

                    // Use an alias if it would not be redundant
                    if expr != block_num_col() {
                        expr = expr.alias(RESERVED_BLOCK_NUM_COLUMN_NAME)
                    }
                    expr
                };

                // Deal with any existing projection that resolves to `_block_num`
                for existing_expr in projection.expr.iter() {
                    // Unwrap: `physical_name` never errors
                    if physical_name(existing_expr).unwrap() != RESERVED_BLOCK_NUM_COLUMN_NAME {
                        continue;
                    }

                    // If the user already correctly selected `RESERVED_BLOCK_NUM_COLUMN_NAME`, we don't need to modify the projection.
                    // We do a best effort to detect correct selections:
                    // - If the expression is identical to the generated one, it is trivially correct.
                    // - If the input is a single table and the expression is simply `_block_num`, qualified or not, it is also correct.
                    if existing_expr == &block_num_expr {
                        return Ok(Transformed::no(LogicalPlan::Projection(projection)));
                    } else if matches!(existing_expr, Expr::Column(_))
                        && block_num_expr == block_num_col()
                    {
                        // Both the user expression and the generated one are simple column references to `_block_num`.
                        // But they were not equal, probably due to qualifiers. If there is only one input table, we can ignore the qualifier difference.
                        let input_schema = projection.input.schema();
                        let input_qualifiers: BTreeSet<&DFTableReference> =
                            input_schema.iter().filter_map(|x| x.0).collect();

                        if input_qualifiers.len() <= 1 {
                            return Ok(Transformed::no(LogicalPlan::Projection(projection)));
                        }
                    }

                    // But If we cannot be sure that the `_block_num` selection is correct, we reject the query.
                    //
                    // Many cases of this would currently be caught by `fn forbid_underscore_prefixed_aliases`.
                    return Err(df_err(
                        "Invalid select of `_block_num`. To fix this error, alias the column or \
                        if using `*`, consider explicitly selecting the columns you need."
                            .to_string(),
                    ));
                }

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
                Ok(Transformed::no(node))
            }

            TableScan(ref scan) => {
                // We run this before optimizations, so we can assume the projection to be empty
                if scan.projection.is_some() {
                    return Err(df_err(format!("Scan should not have projection: {scan:?}")));
                }
                self.next_block_num_expr = Some(block_num_col());
                Ok(Transformed::no(node))
            }

            // Constants are formally produced "before block 0" but hopefully it's correct enough to assign them 0.
            EmptyRelation(_) | Values(_) => {
                self.next_block_num_expr = Some(lit(0));
                Ok(Transformed::no(node))
            }

            // SubqueryAlias caches its schema - we need to rebuild it to reflect schema changes in its input.
            // When the child projection gets _block_num prepended, the SubqueryAlias must be rebuilt
            // so its cached schema includes _block_num. Otherwise, JOINs will fail to find _block_num.
            SubqueryAlias(subquery_alias) => {
                let rebuilt =
                    SubqueryAliasStruct::try_new(subquery_alias.input, subquery_alias.alias)?;
                Ok(Transformed::yes(LogicalPlan::SubqueryAlias(rebuilt)))
            }

            // These DfTableReferencenodes do not cache schema and are not leaves, so we can leave them as-is
            Filter(_) | Repartition(_) | Subquery(_) | Explain(_) | Analyze(_)
            | DescribeTable(_) | Unnest(_) => Ok(Transformed::no(node)),

            // DISTINCT ON (_block_num) is incrementally valid: the DistinctOn node has a
            // cached schema field that must be rebuilt to include _block_num after propagation.
            Distinct(distinct) => {
                use datafusion::logical_expr::{Distinct as DistinctKind, DistinctOn};

                match distinct {
                    DistinctKind::On(mut on) => {
                        let block_num_expr = {
                            let mut expr =
                                self.next_block_num_expr.replace(block_num_col()).unwrap();
                            if expr != block_num_col() {
                                expr = expr.alias(RESERVED_BLOCK_NUM_COLUMN_NAME)
                            }
                            expr
                        };

                        // If _block_num is already in select_expr, no modification needed
                        for existing_expr in on.select_expr.iter() {
                            if physical_name(existing_expr).unwrap()
                                == RESERVED_BLOCK_NUM_COLUMN_NAME
                            {
                                return Ok(Transformed::no(LogicalPlan::Distinct(
                                    DistinctKind::On(on),
                                )));
                            }
                        }

                        // Prepend _block_num to select_expr and rebuild the cached schema
                        on.select_expr.insert(0, block_num_expr);
                        on.schema = prepend_special_block_num_field(&on.schema);
                        Ok(Transformed::yes(LogicalPlan::Distinct(DistinctKind::On(on))))
                    }
                    DistinctKind::All(_) => Err(df_err(
                        format!("incremental_op_kind should have already rejected Distinct::All: {:?}", op_kind)
                    ))
                }
            }

            // GROUP BY _block_num, ... — _block_num is already the first group key so it
            // appears naturally in the aggregate's output schema. Just update
            // next_block_num_expr for the parent and leave the node unchanged.
            Aggregate(_) => {
                self.next_block_num_expr = Some(block_num_col());
                Ok(Transformed::no(node))
            }

            // These variants would have already errored in `incremental_op_kind` above
            Limit(_) | Sort(_) | Window(_) | RecursiveQuery(_)
            | Statement(_) | Dml(_) | Ddl(_) | Copy(_) | Extension(_) => {
                Err(df_err(
                    format!("incremental_op_kind should have already rejected this node type: {:?}", op_kind)
                ))
            }
        }
    }
}

/// Propagate the `RESERVED_BLOCK_NUM_COLUMN_NAME` column through the logical plan.
pub fn propagate_block_num(plan: LogicalPlan) -> Result<LogicalPlan, DataFusionError> {
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

impl std::fmt::Display for NonIncrementalOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
) -> Result<Vec<DFTableReference>, DataFusionError> {
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
    pub networks: BTreeSet<NetworkId>,
}

impl std::fmt::Display for CrossNetworkJoinInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
    let table_to_network: BTreeMap<DFTableReference, NetworkId> = catalog
        .entries()
        .iter()
        .filter_map(|(physical_table, sql_schema_name)| {
            let network = physical_table.network()?.clone();
            let table_ref = TableReference::Partial {
                schema: Arc::new(sql_schema_name.to_string()),
                table: Arc::new(physical_table.table_name().clone()),
            };
            Some((table_ref.into(), network))
        })
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
