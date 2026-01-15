use std::{collections::BTreeSet, fmt, sync::Arc};

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
    sql::{TableReference, utils::UNNEST_PLACEHOLDER},
};

use crate::{
    BoxError, SPECIAL_BLOCK_NUM,
    incrementalizer::{NonIncrementalError, incremental_op_kind},
};

/// Helper function to create a column reference to `_block_num`
fn block_num_col() -> Expr {
    col(SPECIAL_BLOCK_NUM)
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
        .find(|(_, field)| field.name() == SPECIAL_BLOCK_NUM)
        .map(|(qualifier, _)| col(Column::new(qualifier.cloned(), SPECIAL_BLOCK_NUM)))
        .ok_or_else(|| df_err(format!("Left side of join missing {SPECIAL_BLOCK_NUM}")))?;

    let right_block_num = right_schema
        .iter()
        .find(|(_, field)| field.name() == SPECIAL_BLOCK_NUM)
        .map(|(qualifier, _)| col(Column::new(qualifier.cloned(), SPECIAL_BLOCK_NUM)))
        .ok_or_else(|| df_err(format!("Right side of join missing {SPECIAL_BLOCK_NUM}")))?;

    Ok(greatest(vec![left_block_num, right_block_num]))
}

/// Rewriter that propagates the `SPECIAL_BLOCK_NUM` column through the logical plan.
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
                        expr = expr.alias(SPECIAL_BLOCK_NUM)
                    }
                    expr
                };

                // Deal with any existing projection that resolves to `_block_num`
                for existing_expr in projection.expr.iter() {
                    // Unwrap: `physical_name` never errors
                    if physical_name(existing_expr).unwrap() != SPECIAL_BLOCK_NUM {
                        continue;
                    }

                    // If the user already correctly selected `SPECIAL_BLOCK_NUM`, we don't need to modify the projection.
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
                        let input_qualifiers: BTreeSet<&TableReference> =
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
            // TODO: Use `Union::try_new` once DF 51 released.
            Union(union) => {
                // Sanity check
                if self.next_block_num_expr != Some(block_num_col()) {
                    return Err(df_err(format!(
                        "unexpected `next_block_num_expr`: {:?}",
                        self.next_block_num_expr
                    )));
                }

                Ok(Transformed::yes(Union(
                    UnionStruct::try_new_with_loose_types(union.inputs)?,
                )))
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

            // These nodes do not cache schema and are not leaves, so we can leave them as-is
            Filter(_) | Repartition(_) | Subquery(_) | Explain(_) | Analyze(_)
            | DescribeTable(_) | Unnest(_) => Ok(Transformed::no(node)),

            // These variants would have already errored in `incremental_op_kind` above
            Limit(_) | Aggregate(_) | Distinct(_) | Sort(_) | Window(_) | RecursiveQuery(_)
            | Statement(_) | Dml(_) | Ddl(_) | Copy(_) | Extension(_) => {
                unreachable!(
                    "incremental_op_kind should have already rejected this node type: {:?}",
                    op_kind
                )
            }
        }
    }
}

/// Propagate the `SPECIAL_BLOCK_NUM` column through the logical plan.
pub fn propagate_block_num(plan: LogicalPlan) -> Result<LogicalPlan, DataFusionError> {
    let mut propagator = BlockNumPropagator::new();
    plan.rewrite(&mut propagator).map(|t| t.data)
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
    let expr = plan
        .schema()
        .iter()
        .filter(|(_, field)| field.name() != SPECIAL_BLOCK_NUM)
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
pub fn is_incremental(plan: &LogicalPlan) -> Result<(), BoxError> {
    let mut err: Option<NonIncrementalError> = None;

    // TODO: Detect unsupported join stacking, possibly by doing a dry run of the incrementalizer.
    plan.exists(|node| match incremental_op_kind(node) {
        Ok(_) => Ok(false),
        Err(e) => {
            err = Some(e);
            Ok(true)
        }
    })?;

    if let Some(err) = err {
        return Err(err.into());
    }

    Ok(())
}

pub fn extract_table_references_from_plan(
    plan: &LogicalPlan,
) -> Result<Vec<TableReference>, BoxError> {
    let mut refs = BTreeSet::new();

    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            refs.insert(scan.table_name.clone());
        }

        Ok(TreeNodeRecursion::Continue)
    })?;

    Ok(refs.into_iter().collect())
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
        .any(|f| f.name() == SPECIAL_BLOCK_NUM)
    {
        return Arc::new(schema.clone());
    }

    let mut new_schema = datafusion::common::DFSchema::from_unqualified_fields(
        Fields::from(vec![Field::new(SPECIAL_BLOCK_NUM, DataType::UInt64, false)]),
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
        let invalid_projection_plan = LogicalPlanBuilder::from(join_plan.clone())
            .project(vec![
                col("foo.id"),
                col(format!("foo.{}", SPECIAL_BLOCK_NUM)),
                col("foo.foo_value"),
            ])
            .unwrap()
            .build()
            .unwrap();

        // Error on incorrect selection of `_block_num`, even if qualified.
        let err = propagate_block_num(invalid_projection_plan).unwrap_err();
        assert!(err.to_string().contains("Invalid select of `_block_num`. To fix this error, alias the column or if using `*`, consider explicitly selecting the columns you need."));

        // Project foo.* (now aliasing foo._block_num)
        let projection_plan = LogicalPlanBuilder::from(join_plan)
            .project(vec![
                col("foo.id"),
                col(format!("foo.{}", SPECIAL_BLOCK_NUM)).alias("block_num"),
                col("foo.foo_value"),
            ])
            .unwrap()
            .build()
            .unwrap();

        let transformed_plan = propagate_block_num(projection_plan).unwrap();

        // Check that the plan was transformed (should be a Projection)
        match &transformed_plan {
            LogicalPlan::Projection(projection) => {
                // The first expression should be the SPECIAL_BLOCK_NUM
                assert_eq!(projection.expr.len(), 4);

                // Check if the qualified column was properly aliased
                if let Expr::Alias(alias) = &projection.expr[2] {
                    assert_eq!(alias.name, "block_num", "Should alias to block_num");
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
            schema
                .fields()
                .iter()
                .any(|f| f.name() == SPECIAL_BLOCK_NUM),
            "Schema should contain the SPECIAL_BLOCK_NUM field"
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
            SPECIAL_BLOCK_NUM,
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
            SPECIAL_BLOCK_NUM,
            "First field should still be _block_num"
        );

        // Test 3: Function skips adding field when _block_num already exists in schema
        let schema_with_block_num = DFSchema::from_unqualified_fields(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new(SPECIAL_BLOCK_NUM, DataType::UInt64, false),
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
                .any(|f| f.name() == SPECIAL_BLOCK_NUM),
            "Should still contain _block_num field"
        );

        // Test 4: Function skips adding field when qualified _block_num exists (e.g., foo._block_num)
        // This is the critical case mentioned in the comment: different qualifiers should be
        // considered as the same field for the purposes of this function.
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(SPECIAL_BLOCK_NUM, DataType::UInt64, false),
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
                .any(|f| f.name() == SPECIAL_BLOCK_NUM),
            "Should still contain _block_num field"
        );
        // Verify the qualified field is preserved
        let (qualifier, _field) = result4
            .iter()
            .find(|(_, f)| f.name() == SPECIAL_BLOCK_NUM)
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
    fn test_propagate_block_num_through_subquery_alias_in_join() {
        // This test verifies that _block_num is properly propagated through SubqueryAlias nodes
        // when used in JOINs. This simulates the CTE case where a user writes:
        //   WITH test AS (SELECT block_num FROM transactions WHERE value > 0)
        //   SELECT tx.tx_hash, t.block_num FROM test t, transactions tx
        // Without explicit _block_num in the CTE.

        // Create a table with _block_num
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(SPECIAL_BLOCK_NUM, DataType::UInt64, false),
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
                .any(|f| f.name() == SPECIAL_BLOCK_NUM),
            "Resulting plan should have _block_num in schema"
        );
    }
}
