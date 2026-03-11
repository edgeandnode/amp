use std::{collections::BTreeMap, sync::Arc};

use datafusion::{
    common::{
        DFSchemaRef, JoinType,
        tree_node::{Transformed, TreeNode as _, TreeNodeRecursion, TreeNodeRewriter},
    },
    datasource::{TableType, source_as_provider},
    error::DataFusionError,
    logical_expr::{
        EmptyRelation, Join as JoinStruct, LogicalPlan, LogicalPlanBuilder, TableScan,
        Union as UnionStruct,
    },
    prelude::{Expr, col, lit},
    sql::TableReference,
};
use datasets_common::{block_num::RESERVED_BLOCK_NUM_COLUMN_NAME, network_id::NetworkId};
use thiserror::Error;
use tracing::instrument;

use crate::{
    BlockNum, catalog::physical::snapshot::QueryableSnapshot, plan_visitors::NonIncrementalOp,
    udfs::block_num::is_block_num_udf,
};

/// Whether to match the pre-propagation `block_num()` UDF only, or also the
/// post-propagation `col("_block_num")` form.
///
/// - `Udf`: only `block_num()` / `col("block_num()")` — for validating raw user queries.
/// - `Propagated`: also `col("_block_num")` — for the incrementalizer which runs after
///   the propagator has replaced `block_num()` with `_block_num`.
#[derive(Clone, Copy)]
pub enum BlockNumForm {
    Udf,
    Propagated,
}

impl BlockNumForm {
    pub fn matches(self, expr: &Expr) -> bool {
        match self {
            BlockNumForm::Udf => is_block_num_udf(expr),
            BlockNumForm::Propagated => {
                matches!(expr, Expr::Column(c) if c.name == RESERVED_BLOCK_NUM_COLUMN_NAME)
            }
        }
    }
}

/// Context for incrementalizing a plan across one or more networks.
///
/// For single-network queries, `network_ranges` has one entry and the lead network
/// is that single network. No `_block_num` projection is applied (identity).
///
/// For multi-network queries, one network is designated as the **lead**. Table scans
/// for the lead network keep their real `_block_num`. Non-lead table scans get a
/// projection that interpolates `_block_num` into the lead network's block range.
#[derive(Debug, Clone)]
pub struct IncrementalContext {
    /// Per-network block ranges for this microbatch.
    /// Maps each network to its `(start, end)` inclusive block range.
    network_ranges: BTreeMap<NetworkId, (BlockNum, BlockNum)>,
    /// The lead network whose `_block_num` defines the output block numbers.
    /// For single-network queries, this is the only network.
    lead_network: NetworkId,
}

impl IncrementalContext {
    /// Creates a context for a single-network query.
    ///
    /// The single network is the lead. No `_block_num` projection is applied.
    pub fn single_network(network: NetworkId, start: BlockNum, end: BlockNum) -> Self {
        Self {
            network_ranges: [(network.clone(), (start, end))].into(),
            lead_network: network,
        }
    }

    /// Creates a context for a multi-network query.
    ///
    /// The `lead_network` determines which network's `_block_num` values are used in the
    /// output. Non-lead networks have their `_block_num` interpolated into the lead
    /// network's block range.
    ///
    /// # Panics
    ///
    /// Panics (in debug builds) if `lead_network` is not present in `network_ranges`.
    pub fn multi_network(
        network_ranges: BTreeMap<NetworkId, (BlockNum, BlockNum)>,
        lead_network: NetworkId,
    ) -> Self {
        debug_assert!(
            network_ranges.contains_key(&lead_network),
            "lead_network {lead_network:?} must be present in network_ranges",
        );
        Self {
            network_ranges,
            lead_network,
        }
    }

    fn is_multi_network(&self) -> bool {
        self.network_ranges.len() > 1
    }

    /// Returns the `(start, end)` inclusive block range for the lead network.
    ///
    /// # Panics
    ///
    /// Panics if `lead_network` is not present in `network_ranges`.
    fn lead_range(&self) -> (BlockNum, BlockNum) {
        self.network_ranges[&self.lead_network]
    }
}

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
    ctx: &IncrementalContext,
    table_networks: &BTreeMap<TableReference, NetworkId>,
) -> Result<LogicalPlan, DataFusionError> {
    let mut incrementalizer = Incrementalizer::new(ctx, table_networks);
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
#[derive(Debug, Clone)]
struct Incrementalizer<'a> {
    ctx: &'a IncrementalContext,
    table_networks: &'a BTreeMap<TableReference, NetworkId>,

    // Rewriter state: The range we are currently pushing down
    curr_range: RelationRange,
}

impl<'a> Incrementalizer<'a> {
    fn new(
        ctx: &'a IncrementalContext,
        table_networks: &'a BTreeMap<TableReference, NetworkId>,
    ) -> Self {
        Self {
            ctx,
            table_networks,
            curr_range: RelationRange::Delta,
        }
    }

    fn with_range(&self, range: RelationRange) -> Self {
        Self {
            ctx: self.ctx,
            table_networks: self.table_networks,
            curr_range: range,
        }
    }
}

impl TreeNodeRewriter for Incrementalizer<'_> {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>, DataFusionError> {
        use LogicalPlan::*;
        use RelationRange::*;

        // The incrementalizer must run after _block_num propagation, so `BlockNumForm::Propagated`
        match incremental_op_kind(&node, BlockNumForm::Propagated)
            .map_err(|e| DataFusionError::External(e.into()))?
        {
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
                    let network =
                        self.table_networks
                            .get(&table_scan.table_name)
                            .ok_or_else(|| {
                                DataFusionError::External(
                                    format!(
                                        "table {:?} not found in table_networks map",
                                        table_scan.table_name
                                    )
                                    .into(),
                                )
                            })?;
                    let &(start, end) = self.ctx.network_ranges.get(network).ok_or_else(|| {
                        DataFusionError::External(
                            format!("no range for network {:?} in IncrementalContext", network)
                                .into(),
                        )
                    })?;

                    let constrained = constrain_by_range(table_scan, start, end, self.curr_range)?;
                    let predicate = range_predicate(start, end, self.curr_range);
                    let mut plan = LogicalPlanBuilder::from(TableScan(constrained))
                        .filter(predicate)?
                        .build()?;

                    // For multi-network queries, non-lead table scans get a projection
                    // that interpolates _block_num into the lead network's block range.
                    // Lead-network table scans keep their real _block_num.
                    if self.ctx.is_multi_network() && *network != self.ctx.lead_network {
                        let (lead_start, lead_end) = self.ctx.lead_range();
                        plan = interpolate_block_num_from_segment(
                            plan, lead_start, lead_end, start, end,
                        )?;
                    }
                    Ok(Transformed::new(plan, true, TreeNodeRecursion::Jump))
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

/// Validates that all table providers in the plan are suitable for incremental scanning.
///
/// This check ensures that only [`QueryableSnapshot`] providers are used, which is required
/// for production execution. It is intentionally separated from the plan rewriting logic in
/// [`incrementalize_plan`] so that the rewriter can be tested with in-memory table providers.
pub fn validate_table_providers(plan: &LogicalPlan) -> Result<(), DataFusionError> {
    use datafusion::common::tree_node::TreeNodeRecursion;

    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            let Ok(provider) = source_as_provider(&scan.source) else {
                return Err(DataFusionError::External(
                    "TableSource was not DefaultTableSource".into(),
                ));
            };
            if !provider.as_any().is::<QueryableSnapshot>() {
                return Err(DataFusionError::External(
                    format!(
                        "Unsupported table provider in incremental scan: {:?}",
                        provider
                    )
                    .into(),
                ));
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

/// Replaces `_block_num` with an interpolated value derived from segment metadata.
///
/// In a multi-network query, each table scan only has access to its own network's
/// `_block_num`. When a scan belongs to a non-lead network, its `_block_num` values
/// are not directly meaningful in the output (which is denominated in the lead
/// network's block numbers). Since the scan does not have access to the lead
/// network's `_block_num` column, we approximate it by linearly interpolating the
/// row's position within its network range onto the lead network's range for the
/// same microbatch. Both ranges are known from segment metadata.
///
/// # Formula
///
/// ```text
/// interpolated(b) = lead_start + (b - net_start) * (lead_end - lead_start) / (net_end - net_start)
/// ```
///
/// When `net_start == net_end` (single block), all rows map to `lead_start`.
///
/// Integer division (floor) is used, so multiple rows may map to the same output
/// block number.
fn interpolate_block_num_from_segment(
    plan: LogicalPlan,
    lead_start: BlockNum,
    lead_end: BlockNum,
    net_start: BlockNum,
    net_end: BlockNum,
) -> Result<LogicalPlan, DataFusionError> {
    let mapped_expr = if net_start == net_end {
        // Single non-lead block: all rows map to lead_start. No casting needed.
        lit(lead_start).alias(RESERVED_BLOCK_NUM_COLUMN_NAME)
    } else {
        // lead_start + (b - net_start) * (lead_end - lead_start) / (net_end - net_start)
        // All values are non-negative: the range filter guarantees _block_num >= net_start,
        // and lead_end >= lead_start, net_end > net_start (single-block handled above).
        let block_num_col = col(RESERVED_BLOCK_NUM_COLUMN_NAME);
        let lead_width = lead_end - lead_start;
        let net_width = net_end - net_start;
        (lit(lead_start) + (block_num_col - lit(net_start)) * lit(lead_width) / lit(net_width))
            .alias(RESERVED_BLOCK_NUM_COLUMN_NAME)
    };

    // Build projection: all fields from the scan, but replace _block_num with the mapped expr.
    let exprs: Vec<Expr> = plan
        .schema()
        .columns()
        .into_iter()
        .map(|c| {
            if c.name == RESERVED_BLOCK_NUM_COLUMN_NAME {
                mapped_expr.clone()
            } else {
                Expr::Column(c)
            }
        })
        .collect();

    LogicalPlanBuilder::from(plan).project(exprs)?.build()
}

/// Adds `where start <= _block_num and _block_num <= end` to the table scan.
///
/// The predicate is pushed into `table_scan.filters` for providers that support filter pushdown
/// (e.g. [`QueryableSnapshot`]). The caller is responsible for wrapping in a `Filter` node if
/// the provider does not support pushdown.
///
/// This assumes that the `_block_num` column has already been propagated and is therefore
/// present in the schema of `plan`.
#[instrument(skip_all, err)]
fn constrain_by_range(
    mut table_scan: TableScan,
    delta_start: u64,
    delta_end: u64,
    range: RelationRange,
) -> Result<TableScan, ConstrainByRangeError> {
    if table_scan.source.table_type() != TableType::Base
        || table_scan.source.get_logical_plan().is_some()
    {
        // These should not exist in an streamable and optimized plan.
        return Err(ConstrainByRangeError(
            table_scan.table_name,
            table_scan.source.table_type(),
        ));
    }

    table_scan
        .filters
        .push(range_predicate(delta_start, delta_end, range));
    Ok(table_scan)
}

/// Builds the block-range predicate for a given range, without modifying a table scan.
fn range_predicate(delta_start: u64, delta_end: u64, range: RelationRange) -> Expr {
    match range {
        RelationRange::Delta => lit(delta_start)
            .lt_eq(col(RESERVED_BLOCK_NUM_COLUMN_NAME))
            .and(col(RESERVED_BLOCK_NUM_COLUMN_NAME).lt_eq(lit(delta_end))),
        RelationRange::History => col(RESERVED_BLOCK_NUM_COLUMN_NAME).lt(lit(delta_start)),
    }
}

/// Error when constraining a table scan by range
///
/// This occurs when attempting to constrain a table scan that is not a base table.
/// Base tables are required for range-based filtering to work correctly.
#[derive(Debug, thiserror::Error)]
#[error("non-base table found: {0:?}")]
struct ConstrainByRangeError(TableReference, TableType);

impl From<ConstrainByRangeError> for DataFusionError {
    fn from(e: ConstrainByRangeError) -> Self {
        DataFusionError::External(e.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum NonIncrementalQueryError {
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

pub fn incremental_op_kind(
    node: &LogicalPlan,
    form: BlockNumForm,
) -> Result<IncrementalOpKind, NonIncrementalQueryError> {
    use IncrementalOpKind::*;
    use LogicalPlan::*;
    use NonIncrementalQueryError::*;

    match node {
        // Entirely unsupported operations.
        Dml(_) | Ddl(_) | Statement(_) | Copy(_) | Extension(_) => {
            Err(NonIncrementalQueryError::Invalid(node.to_string()))
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

        Aggregate(agg) => {
            // An aggregate is incrementally valid when _block_num is the first group-by key.
            // This also handles DISTINCT ON (_block_num, ...) which DataFusion's optimizer
            // rewrites to an Aggregate with _block_num as the first group-by key.
            let first_group_ok = agg.group_expr.first().is_some_and(|e| form.matches(e));
            if first_group_ok {
                Ok(Linear)
            } else {
                Err(NonIncremental(NonIncrementalOp::Aggregate))
            }
        }
        Distinct(distinct) => {
            use datafusion::logical_expr::Distinct as DistinctEnum;
            match distinct {
                // SELECT DISTINCT ON (_block_num, ...) is locally incrementalizable:
                // since on_expr starts with _block_num and microbatches never overlap in
                // _block_num values, deduplication can be applied per microbatch.
                // Also accepts block_num() UDF which will be replaced during propagation.
                DistinctEnum::On(on) => {
                    let first_on_ok = on.on_expr.first().is_some_and(|e| form.matches(e));
                    if first_on_ok {
                        Ok(Linear)
                    } else {
                        Err(NonIncremental(NonIncrementalOp::Distinct))
                    }
                }
                DistinctEnum::All(_) => Err(NonIncremental(NonIncrementalOp::Distinct)),
            }
        }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::{
        arrow::{
            array::{Int32Array, Int64Array, RecordBatch, UInt64Array},
            csv::WriterBuilder,
            datatypes::{DataType, Field, Schema},
        },
        datasource::MemTable,
        logical_expr::ScalarUDF,
        prelude::SessionContext,
    };
    use indoc::indoc;

    use super::*;
    use crate::{plan_visitors::propagate_block_num, udfs::block_num::BlockNumUdf};

    /// Creates a [`SessionContext`] with the `block_num()` UDF registered.
    fn test_session() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(BlockNumUdf::new()));
        ctx
    }

    /// Registers a [`MemTable`] with schema `(_block_num UInt64, id Int32, value Int64)`.
    fn register_test_table(
        ctx: &SessionContext,
        name: &str,
        block_nums: &[u64],
        ids: &[i32],
        values: &[i64],
    ) {
        let schema = Arc::new(Schema::new(vec![
            Field::new(RESERVED_BLOCK_NUM_COLUMN_NAME, DataType::UInt64, false),
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(block_nums.to_vec())),
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(Int64Array::from(values.to_vec())),
            ],
        )
        .expect("record batch should be created from matching arrays");
        let table = Arc::new(
            MemTable::try_new(schema, vec![vec![batch]])
                .expect("MemTable should be created from valid schema and batches"),
        );
        ctx.register_table(name, table)
            .expect("table should be registered in session context");
    }

    /// Returns CSV lines sorted (header stays first) for order-independent comparison.
    fn sorted_csv(csv: &str) -> String {
        let mut lines: Vec<&str> = csv.lines().collect();
        if lines.len() > 1 {
            lines[1..].sort();
        }
        let mut s = lines.join("\n");
        s.push('\n');
        s
    }

    /// Serializes record batches to a CSV string with a header row.
    fn to_csv(batches: &[RecordBatch]) -> String {
        let mut buf = Vec::new();
        {
            let mut writer = WriterBuilder::new().with_header(true).build(&mut buf);
            for batch in batches {
                writer
                    .write(batch)
                    .expect("CSV writer should serialize record batch");
            }
        }
        String::from_utf8(buf).expect("CSV output should be valid UTF-8")
    }

    /// Runs a SQL query through propagation + incrementalization, then executes it.
    async fn execute_incremental(
        ctx: &SessionContext,
        sql: &str,
        incremental_ctx: &IncrementalContext,
        table_networks: &BTreeMap<TableReference, NetworkId>,
    ) -> Vec<RecordBatch> {
        let plan = ctx
            .sql(sql)
            .await
            .expect("SQL should parse and plan successfully")
            .logical_plan()
            .clone();
        let propagated = propagate_block_num(plan).expect("block_num propagation should succeed");
        let incremental = incrementalize_plan(propagated, incremental_ctx, table_networks)
            .expect("incrementalization should succeed");
        ctx.execute_logical_plan(incremental)
            .await
            .expect("logical plan execution should succeed")
            .collect()
            .await
            .expect("result stream should collect successfully")
    }

    #[tokio::test]
    async fn single_network() {
        //* Given
        let ctx = test_session();
        register_test_table(
            &ctx,
            "t",
            &[1, 2, 3, 4, 5],
            &[1, 2, 3, 4, 5],
            &[10, 20, 30, 40, 50],
        );

        let network: NetworkId = "mainnet"
            .parse()
            .expect("mainnet should be a valid NetworkId");
        let incremental_ctx = IncrementalContext::single_network(network.clone(), 3, 4);

        let mut table_networks = BTreeMap::new();
        table_networks.insert(TableReference::bare("t"), network);

        //* When
        let batches = execute_incremental(
            &ctx,
            "SELECT value FROM t",
            &incremental_ctx,
            &table_networks,
        )
        .await;

        //* Then
        // Delta [3, 4]: _block_num passes through unchanged.
        let csv = to_csv(&batches);
        assert_eq!(
            csv,
            indoc! {"
                _block_num,value
                3,30
                4,40
            "}
        );
    }

    #[tokio::test]
    async fn multi_network_union() {
        //* Given
        let ctx = test_session();

        register_test_table(
            &ctx,
            "eth",
            &[100, 101, 102, 103],
            &[1, 2, 3, 4],
            &[10, 20, 30, 40],
        );
        register_test_table(&ctx, "base", &[500, 501, 502], &[5, 6, 7], &[50, 60, 70]);

        let eth_network: NetworkId = "mainnet"
            .parse()
            .expect("mainnet should be a valid NetworkId");
        let base_network: NetworkId = "base".parse().expect("base should be a valid NetworkId");

        let mut network_ranges = BTreeMap::new();
        network_ranges.insert(eth_network.clone(), (100, 103));
        network_ranges.insert(base_network.clone(), (500, 502));
        let incremental_ctx =
            IncrementalContext::multi_network(network_ranges, base_network.clone());

        let mut table_networks = BTreeMap::new();
        table_networks.insert(TableReference::bare("eth"), eth_network);
        table_networks.insert(TableReference::bare("base"), base_network);

        //* When
        let batches = execute_incremental(
            &ctx,
            "SELECT value FROM eth UNION ALL SELECT value FROM base",
            &incremental_ctx,
            &table_networks,
        )
        .await;

        //* Then
        // Eth _block_num interpolated into lead (base) range; base passes through.
        let csv = to_csv(&batches);
        assert_eq!(
            sorted_csv(&csv),
            sorted_csv(indoc! {"
                _block_num,value
                500,10
                500,20
                500,50
                501,30
                501,60
                502,40
                502,70
            "})
        );
    }

    #[tokio::test]
    async fn multi_network_filter() {
        //* Given
        let ctx = test_session();

        register_test_table(
            &ctx,
            "eth",
            &[100, 101, 102, 103],
            &[1, 2, 3, 4],
            &[10, 20, 30, 40],
        );
        register_test_table(&ctx, "base", &[500, 501, 502], &[5, 6, 7], &[50, 60, 70]);

        let eth_network: NetworkId = "mainnet"
            .parse()
            .expect("mainnet should be a valid NetworkId");
        let base_network: NetworkId = "base".parse().expect("base should be a valid NetworkId");

        let mut network_ranges = BTreeMap::new();
        network_ranges.insert(eth_network.clone(), (100, 103));
        network_ranges.insert(base_network.clone(), (500, 502));
        let incremental_ctx =
            IncrementalContext::multi_network(network_ranges, base_network.clone());

        let mut table_networks = BTreeMap::new();
        table_networks.insert(TableReference::bare("eth"), eth_network);
        table_networks.insert(TableReference::bare("base"), base_network);

        //* When
        let batches = execute_incremental(
            &ctx,
            "SELECT value FROM eth WHERE value > 15 \
             UNION ALL \
             SELECT value FROM base WHERE value > 55",
            &incremental_ctx,
            &table_networks,
        )
        .await;

        //* Then
        // User-level filters applied before interpolation.
        let csv = to_csv(&batches);
        assert_eq!(
            sorted_csv(&csv),
            sorted_csv(indoc! {"
                _block_num,value
                500,20
                501,30
                501,60
                502,40
                502,70
            "})
        );
    }

    #[tokio::test]
    async fn multi_network_union_of_aggregates() {
        //* Given
        let ctx = test_session();

        register_test_table(
            &ctx,
            "eth",
            &[100, 100, 101, 102],
            &[1, 2, 3, 4],
            &[10, 20, 30, 40],
        );
        register_test_table(&ctx, "base", &[500, 501], &[5, 6], &[50, 60]);

        let eth_network: NetworkId = "mainnet"
            .parse()
            .expect("mainnet should be a valid NetworkId");
        let base_network: NetworkId = "base".parse().expect("base should be a valid NetworkId");

        let mut network_ranges = BTreeMap::new();
        network_ranges.insert(eth_network.clone(), (100, 102));
        network_ranges.insert(base_network.clone(), (500, 501));
        let incremental_ctx =
            IncrementalContext::multi_network(network_ranges, base_network.clone());

        let mut table_networks = BTreeMap::new();
        table_networks.insert(TableReference::bare("eth"), eth_network);
        table_networks.insert(TableReference::bare("base"), base_network);

        //* When
        let batches = execute_incremental(
            &ctx,
            "SELECT block_num(), SUM(value) FROM eth GROUP BY block_num() \
             UNION ALL \
             SELECT block_num(), SUM(value) FROM base GROUP BY block_num()",
            &incremental_ctx,
            &table_networks,
        )
        .await;

        //* Then
        // Each side aggregated independently; eth grouped by interpolated _block_num.
        let csv = to_csv(&batches);
        assert_eq!(
            sorted_csv(&csv),
            sorted_csv(indoc! {"
                _block_num,block_num(),sum(eth.value)
                500,500,50
                500,500,60
                501,501,40
                501,501,60
            "})
        );
    }

    #[tokio::test]
    async fn multi_network_aggregate_after_union() {
        //* Given
        let ctx = test_session();

        register_test_table(
            &ctx,
            "eth",
            &[100, 100, 101, 102],
            &[1, 2, 3, 4],
            &[10, 20, 30, 40],
        );
        register_test_table(&ctx, "base", &[500, 501], &[5, 6], &[50, 60]);

        let eth_network: NetworkId = "mainnet"
            .parse()
            .expect("mainnet should be a valid NetworkId");
        let base_network: NetworkId = "base".parse().expect("base should be a valid NetworkId");

        let mut network_ranges = BTreeMap::new();
        network_ranges.insert(eth_network.clone(), (100, 102));
        network_ranges.insert(base_network.clone(), (500, 501));
        let incremental_ctx =
            IncrementalContext::multi_network(network_ranges, base_network.clone());

        let mut table_networks = BTreeMap::new();
        table_networks.insert(TableReference::bare("eth"), eth_network);
        table_networks.insert(TableReference::bare("base"), base_network);

        //* When
        let batches = execute_incremental(
            &ctx,
            "SELECT block_num(), SUM(value) FROM ( \
                 SELECT block_num(), value FROM eth \
                 UNION ALL \
                 SELECT block_num(), value FROM base \
             ) sub GROUP BY block_num()",
            &incremental_ctx,
            &table_networks,
        )
        .await;

        //* Then
        // Aggregate over union: both networks grouped by interpolated _block_num.
        let csv = to_csv(&batches);
        assert_eq!(
            sorted_csv(&csv),
            sorted_csv(indoc! {"
                _block_num,block_num(),sum(sub.value)
                500,500,110
                501,501,100
            "})
        );
    }
}
