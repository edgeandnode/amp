use std::{collections::BTreeSet, fmt, sync::Arc};

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

use crate::{BLOCK_NUM, BoxError, SPECIAL_BLOCK_NUM, internal};

/// Aliases with a name starting with `_` are always forbidden, since underscore-prefixed
/// names are reserved for special columns.
pub fn forbid_underscore_prefixed_aliases(plan: &LogicalPlan) -> Result<(), DataFusionError> {
    plan.apply(|node| {
        if let LogicalPlan::Projection(projection) = node {
            for expr in projection.expr.iter() {
                if let Expr::Alias(alias) = expr
                    && alias.name.starts_with('_') {
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
    let expr = plan
        .schema()
        .iter()
        .filter(|(_, field)| field.name() != SPECIAL_BLOCK_NUM)
        .map(Expr::from)
        .collect::<Vec<_>>();

    let builder = LogicalPlanBuilder::from(plan);

    builder.project(expr)?.build()
}

/// Adds `where start <= _block_num and _block_num <= end` to the plan and runs filter pushdown.
///
/// This assumes that the `_block_num` column has already been propagated and is therefore
/// present in the schema of `plan`.
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

/// Reasons why a logical plan cannot be materialized incrementally
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub enum NonIncrementalOp {
    /// Limit requires counting rows across batches
    Limit,
    /// Aggregations need state management
    Aggregate,
    /// Distinct operations need global deduplication
    Distinct,
    /// Joins need to maintain state between batches
    Join,
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
            Join => write!(f, "Join"),
            Sort => write!(f, "Sort"),
            Window => write!(f, "Window"),
            RecursiveQuery => write!(f, "RecursiveQuery"),
        }
    }
}

/// Result of checking if a logical plan can be materialized incrementally
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncrementalCheck {
    /// The plan can be materialized incrementally
    Incremental,
    /// The plan cannot be materialized incrementally due to the given operation
    NonIncremental(NonIncrementalOp),
}

impl IncrementalCheck {
    /// Returns `true` if the plan can be materialized incrementally
    pub fn is_incremental(&self) -> bool {
        matches!(self, IncrementalCheck::Incremental)
    }
}

/// Can the given logical plan be sync'ed incrementally?
pub fn is_incremental(plan: &LogicalPlan) -> Result<IncrementalCheck, BoxError> {
    use LogicalPlan::*;

    let mut non_incremental_op = None;
    let mut err: Option<BoxError> = None;

    plan.exists(|node| {
        match node {
            // Entirely unsupported operations.
            Dml(_) | Ddl(_) | Statement(_) | Copy(_) | Extension(_) => {
                err = Some(format!("unsupported operation in query: {}", node.display()).into());
                Ok(true)
            }

            // Stateless operators
            Projection(_) | Filter(_) | Union(_) | Unnest(_) | Repartition(_) | TableScan(_)
            | EmptyRelation(_) | Values(_) | Subquery(_) | SubqueryAlias(_) | DescribeTable(_)
            | Explain(_) | Analyze(_) => Ok(false),

            // Non-incremental but supported operations
            Limit(_) => {
                non_incremental_op = Some(NonIncrementalOp::Limit);
                Ok(true)
            }
            Aggregate(_) => {
                non_incremental_op = Some(NonIncrementalOp::Aggregate);
                Ok(true)
            }
            Distinct(_) => {
                non_incremental_op = Some(NonIncrementalOp::Distinct);
                Ok(true)
            }
            Join(_) => {
                non_incremental_op = Some(NonIncrementalOp::Join);
                Ok(true)
            }
            Sort(_) => {
                non_incremental_op = Some(NonIncrementalOp::Sort);
                Ok(true)
            }
            Window(_) => {
                non_incremental_op = Some(NonIncrementalOp::Window);
                Ok(true)
            }
            RecursiveQuery(_) => {
                non_incremental_op = Some(NonIncrementalOp::RecursiveQuery);
                Ok(true)
            }
        }
    })?;

    if let Some(err) = err {
        return Err(err);
    }

    Ok(match non_incremental_op {
        None => IncrementalCheck::Incremental,
        Some(op) => IncrementalCheck::NonIncremental(op),
    })
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
        expr: vec![col(SPECIAL_BLOCK_NUM).sort(true, false)],
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
                    assert_eq!(alias.name, SPECIAL_BLOCK_NUM, "Should alias to _block_num");
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
}
