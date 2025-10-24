use std::sync::Arc;

use datafusion::{
    catalog::TableProvider,
    common::{
        DFSchemaRef, JoinType,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter},
    },
    datasource::{TableType, source_as_provider},
    error::DataFusionError,
    logical_expr::{
        EmptyRelation, Join as JoinStruct, LogicalPlan, TableScan, Union as UnionStruct,
    },
    prelude::{col, lit},
};
use thiserror::Error;
use tracing::instrument;

use crate::{
    BlockNum, BoxError, SPECIAL_BLOCK_NUM, catalog::physical::TableSnapshot,
    plan_visitors::NonIncrementalOp,
};

/// Assuming that output table has been synced up to `start - 1`, and that the input tables are immutable (all of our tables currently are), this will return the _incremental version_ of `plan` that computes the microbatch `[start, end]`.
///
/// The supported incremental operators are _linear operators_ (projection, filter, union) and inner joins.
///
/// Algorithm:
/// - Consider the path from a table scan to the root of the query plan. If there are only linear operators in that path, that table scan will be filtered by `start <= _block_num <= end`.
/// - If there are inner joins, then consider the first join in the path. The table is either on the left (L) or right (R) side of the join. The join will be rewritten according to the inner join update rule:
///
/// Δ(L⋈R)=(ΔL⋈R[t−1​])∪(L[t−1]​⋈ΔR)∪(ΔL⋈ΔR)
///
/// Where ΔT is is `T where start <= _block_num <= end`, and T[t−1​] is `T where _block_num < start`.
///
/// ## Special cases
/// - If a join as an `on` condition that is an inequality on `_block_num`, e.g. `l._block_num <= r._block_num`, then Δ(L⋈R)=(L[t−1]​⋈ΔR)∪(ΔL⋈ΔR), a term is optimized away.
///   This is because the inequality can be relaxed to `start <= r_block_num`. More generally, if the `on` clause can be relaxed by a lower start bound, we can push it down and potentially eliminate a term. This is not yet implemented.
/// - If a join has a `r._block_num = l.block_num` condition, then Δ(L⋈R)=ΔL⋈ΔR. These joins may stack with each other and with linear operators, or on top of output of general joins. This special case is not yet implemented.
///
/// ## Further reading
/// - The inner join update formula is well-known and commonly implemented in incremental view maintenance systems. For one academic reference see:
/// - https://sigmodrecord.org/publications/sigmodRecord/2403/pdfs/20_dbsp-budiu.pdf
pub fn incrementalize_plan(
    plan: LogicalPlan,
    start: BlockNum,
    end: BlockNum,
) -> Result<LogicalPlan, BoxError> {
    let mut incrementalizer = Incrementalizer::new(start, end);
    let plan = plan.rewrite(&mut incrementalizer)?.data;
    Ok(plan)
}

/// The range to scan, relative to a given `start` and `end` for the current microbatch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RelationRange {
    Delta,   // The range [start, end]
    History, // The range (, start)
}

/// The incrementalizer is essentially a pushdown rewriter that also applies the inner join update rule.
#[derive(Debug, Copy, Clone)]
struct Incrementalizer {
    // Inputs: The range over the input tables that will be incrementally processed and added to the output table.
    start: BlockNum,
    end: BlockNum,

    // Rewriter state: The range we are currently pushing down
    curr_range: RelationRange,
}

impl Incrementalizer {
    fn new(start: BlockNum, end: BlockNum) -> Self {
        Self {
            start,
            end,
            curr_range: RelationRange::Delta,
        }
    }

    fn with_range(self, range: RelationRange) -> Self {
        Self {
            start: self.start,
            end: self.end,
            curr_range: range,
        }
    }
}

impl TreeNodeRewriter for Incrementalizer {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>, DataFusionError> {
        use LogicalPlan::*;
        use RelationRange::*;

        match incremental_op_kind(&node).map_err(|e| DataFusionError::External(e.into()))? {
            IncrementalOpKind::Linear => Ok(Transformed::no(node)),
            IncrementalOpKind::InnerJoin => {
                let LogicalPlan::Join(join) = node else {
                    unreachable!("IncrementalOpKind::InnerJoin only returned for Join nodes")
                };

                if self.curr_range == History {
                    return Err(DataFusionError::External(
                        "stacked joins not supported".into(),
                    ));
                }

                // Apply the inner join update rule: Δ(L⋈R) = (ΔL⋈R[t−1]) ∪ (L[t−1]⋈ΔR) ∪ (ΔL⋈ΔR)
                let left = join.left.as_ref();
                let right = join.right.as_ref();

                // Term 1: ΔL⋈R[t−1]
                let delta_left = left.clone().rewrite(&mut self.with_range(Delta))?.data;
                let history_right = right.clone().rewrite(&mut self.with_range(History))?.data;
                let term1 = create_join(delta_left, history_right, &join)?;

                // Term 2: L[t−1]⋈ΔR
                let history_left = left.clone().rewrite(&mut self.with_range(History))?.data;
                let delta_right = right.clone().rewrite(&mut self.with_range(Delta))?.data;
                let term2 = create_join(history_left, delta_right, &join)?;

                // Term 3: ΔL⋈ΔR
                let delta_left2 = left.clone().rewrite(&mut self.with_range(Delta))?.data;
                let delta_right2 = right.clone().rewrite(&mut self.with_range(Delta))?.data;
                let term3 = create_join(delta_left2, delta_right2, &join)?;

                // Create 3-way union
                // Use use a literal because constructors would wipe qualifiers
                let union = LogicalPlan::Union(UnionStruct {
                    schema: term1.schema().clone(),
                    inputs: vec![Arc::new(term1), Arc::new(term2), Arc::new(term3)],
                });

                // Jump prevents automatic recursion into union children (we've already rewritten them)
                Ok(Transformed::new(union, true, TreeNodeRecursion::Jump))
            }
            IncrementalOpKind::Table => match node {
                TableScan(table_scan) => {
                    let Ok(table_provider) = source_as_provider(&table_scan.source) else {
                        return Err(DataFusionError::External(
                            "TableSource was not DefaultTableSource".into(),
                        ));
                    };

                    validate_table_provider(table_provider.as_ref())?;

                    let constrained =
                        constrain_by_range(table_scan, self.start, self.end, self.curr_range)?;
                    Ok(Transformed::yes(TableScan(constrained)))
                }

                // A static value has always existed and is never updated.
                // Therefore, it has only history and no delta.
                Values(_) => match self.curr_range {
                    History => Ok(Transformed::no(node)),
                    Delta => Ok(Transformed::yes(EmptyRelation(empty_relation(
                        node.schema().clone(),
                    )))),
                },
                EmptyRelation(_) => Ok(Transformed::no(node)),

                // Panic: We only return `IncrementalOpKind::Table` for the above variants.
                _ => unreachable!("unhandled table node: {:?}", node),
            },
        }
    }

    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>, DataFusionError> {
        Ok(Transformed::no(node))
    }
}

/// Validates that a table provider is suitable for incremental scanning.
fn validate_table_provider(table_provider: &dyn TableProvider) -> Result<(), DataFusionError> {
    if !table_provider.as_any().is::<TableSnapshot>() {
        return Err(DataFusionError::External(
            format!(
                "Unsupported table provider in incremental scan: {:?}",
                table_provider
            )
            .into(),
        ));
    }

    Ok(())
}

/// Adds `where start <= _block_num and _block_num <= end` to the plan and runs filter pushdown.
///
/// This assumes that the `_block_num` column has already been propagated and is therefore
/// present in the schema of `plan`.
#[instrument(skip_all, err)]
fn constrain_by_range(
    mut table_scan: TableScan,
    delta_start: u64,
    delta_end: u64,
    range: RelationRange,
) -> Result<TableScan, BoxError> {
    if table_scan.source.table_type() != TableType::Base
        || table_scan.source.get_logical_plan().is_some()
    {
        // These should not exist in an streamable and optimized plan.
        return Err(format!("non-base table found: {:?}", table_scan).into());
    }

    let predicate = {
        match range {
            // `where start <= _block_num and _block_num <= end`
            RelationRange::Delta => lit(delta_start)
                .lt_eq(col(SPECIAL_BLOCK_NUM))
                .and(col(SPECIAL_BLOCK_NUM).lt_eq(lit(delta_end))),
            // `where _block_num < start`
            RelationRange::History => col(SPECIAL_BLOCK_NUM).lt(lit(delta_start)),
        }
    };

    table_scan.filters.push(predicate);
    Ok(table_scan)
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum NonIncrementalError {
    /// The plan node is invalid for any kind of query
    #[error("invalid operation: {0}")]
    Invalid(String),
    /// The plan node cannot be materialized incrementally due to the given operation
    #[error("query contains non-incremental operation: {0}")]
    NonIncremental(NonIncrementalOp),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncrementalOpKind {
    Linear,
    InnerJoin,
    Table,
}

pub fn incremental_op_kind(node: &LogicalPlan) -> Result<IncrementalOpKind, NonIncrementalError> {
    use IncrementalOpKind::*;
    use LogicalPlan::*;
    use NonIncrementalError::*;

    match node {
        // Entirely unsupported operations.
        Dml(_) | Ddl(_) | Statement(_) | Copy(_) | Extension(_) => {
            Err(NonIncrementalError::Invalid(node.to_string()))
        }

        // Stateless operators
        Projection(_) | Filter(_) | Union(_) | Unnest(_) | Repartition(_) | Subquery(_)
        | SubqueryAlias(_) | DescribeTable(_) | Explain(_) | Analyze(_) => Ok(Linear),

        // Tables
        TableScan(_) | Values(_) | EmptyRelation(_) => Ok(IncrementalOpKind::Table),

        // Joins
        Join(join) => match join.join_type {
            // TODO: detect and split out `l._block_num = r._block_num` joins
            JoinType::Inner => Ok(InnerJoin),

            // Semi-joins are just projections of inner joins
            JoinType::LeftSemi => Ok(InnerJoin),
            JoinType::RightSemi => Ok(InnerJoin),

            // Outer and anti joins are not incremental
            JoinType::Left
            | JoinType::Right
            | JoinType::Full
            | JoinType::LeftAnti
            | JoinType::RightAnti
            | JoinType::LeftMark
            | JoinType::RightMark => Err(NonIncremental(NonIncrementalOp::Join(join.join_type))),
        },

        // Operations that are supported only in batch queries
        Limit(_) => Err(NonIncremental(NonIncrementalOp::Limit)),
        Aggregate(_) => Err(NonIncremental(NonIncrementalOp::Aggregate)),
        Distinct(_) => Err(NonIncremental(NonIncrementalOp::Distinct)),
        Sort(_) => Err(NonIncremental(NonIncrementalOp::Sort)),
        Window(_) => Err(NonIncremental(NonIncrementalOp::Window)),
        RecursiveQuery(_) => Err(NonIncremental(NonIncrementalOp::RecursiveQuery)),
    }
}

fn empty_relation(schema: DFSchemaRef) -> EmptyRelation {
    EmptyRelation {
        produce_one_row: false,
        schema,
    }
}

/// Helper function to recreate a join with new children but preserving the original join configuration.
fn create_join(
    left: LogicalPlan,
    right: LogicalPlan,
    original: &JoinStruct,
) -> Result<LogicalPlan, DataFusionError> {
    JoinStruct::try_new(
        Arc::new(left),
        Arc::new(right),
        original.on.clone(),
        original.filter.clone(),
        original.join_type,
        original.join_constraint,
        original.null_equality,
    )
    .map(LogicalPlan::Join)
}
