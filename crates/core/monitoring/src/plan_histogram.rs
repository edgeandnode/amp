//! Logical plan histogram collection for query analysis.
//!
//! This module provides utilities for walking DataFusion logical plans and
//! collecting statistics about the types of nodes present. This is useful for
//! understanding query complexity and for metrics/monitoring.

use std::collections::HashMap;

use datafusion::{
    common::tree_node::{TreeNode, TreeNodeRecursion},
    logical_expr::LogicalPlan,
};

/// Collects a histogram of logical plan node types.
///
/// This walks a DataFusion logical plan tree and counts how many of each
/// node type appears. The histogram maps node type names to counts.
///
/// # Example
///
/// ```ignore
/// use datafusion::logical_expr::LogicalPlan;
/// use monitoring::plan_histogram::collect_plan_histogram;
///
/// let plan: LogicalPlan = ...; // your plan
/// let histogram = collect_plan_histogram(&plan);
///
/// // histogram might contain:
/// // {"TableScan": 2, "Filter": 3, "Projection": 1, "Join": 1}
/// ```
pub fn collect_plan_histogram(plan: &LogicalPlan) -> HashMap<String, usize> {
    let mut histogram = HashMap::new();

    // Use DataFusion's TreeNode apply() to traverse the plan
    let _ = plan.apply(|node| {
        // Get the node type name
        let node_type = match node {
            LogicalPlan::Projection(_) => "Projection",
            LogicalPlan::Filter(_) => "Filter",
            LogicalPlan::Window(_) => "Window",
            LogicalPlan::Aggregate(_) => "Aggregate",
            LogicalPlan::Sort(_) => "Sort",
            LogicalPlan::Join(_) => "Join",
            LogicalPlan::Repartition(_) => "Repartition",
            LogicalPlan::Union(_) => "Union",
            LogicalPlan::TableScan(_) => "TableScan",
            LogicalPlan::EmptyRelation(_) => "EmptyRelation",
            LogicalPlan::Subquery(_) => "Subquery",
            LogicalPlan::SubqueryAlias(_) => "SubqueryAlias",
            LogicalPlan::Limit(_) => "Limit",
            LogicalPlan::Statement(_) => "Statement",
            LogicalPlan::Values(_) => "Values",
            LogicalPlan::Explain(_) => "Explain",
            LogicalPlan::Analyze(_) => "Analyze",
            LogicalPlan::Extension(_) => "Extension",
            LogicalPlan::Distinct(_) => "Distinct",
            LogicalPlan::Dml(_) => "Dml",
            LogicalPlan::Ddl(_) => "Ddl",
            LogicalPlan::Copy(_) => "Copy",
            LogicalPlan::DescribeTable(_) => "DescribeTable",
            LogicalPlan::Unnest(_) => "Unnest",
            LogicalPlan::RecursiveQuery(_) => "RecursiveQuery",
        };

        // Record this node type
        *histogram.entry(node_type.to_owned()).or_insert(0) += 1;

        // Continue visiting children
        Ok(TreeNodeRecursion::Continue)
    });

    histogram
}

/// Get a summary string of the most common node types.
///
/// Returns a formatted string showing the top N most common node types.
/// Useful for logging/display purposes.
///
/// # Example
///
/// ```ignore
/// let histogram = collect_plan_histogram(&plan);
/// let summary = summarize_histogram(&histogram, 3);
/// // Returns something like: "TableScan:2, Filter:3, Join:1"
/// ```
pub fn summarize_histogram(histogram: &HashMap<String, usize>, top_n: usize) -> String {
    let mut items: Vec<_> = histogram.iter().collect();
    items.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));

    items
        .iter()
        .take(top_n)
        .map(|(k, v)| format!("{}:{}", k, v))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Get the total number of nodes in the plan.
pub fn total_nodes(histogram: &HashMap<String, usize>) -> usize {
    histogram.values().sum()
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::*;

    use super::*;

    #[tokio::test]
    async fn test_simple_plan_histogram() {
        let ctx = SessionContext::new();

        // Create a simple plan: SELECT * FROM table WHERE x > 10
        let plan = ctx
            .sql("SELECT * FROM (VALUES (1, 2), (3, 4)) AS t(a, b) WHERE a > 1")
            .await
            .unwrap()
            .into_unoptimized_plan();

        let histogram = collect_plan_histogram(&plan);

        // Should have at least a Projection, Filter, and Values node
        assert!(histogram.contains_key("Projection"));
        assert!(histogram.contains_key("Filter"));
        assert!(histogram.contains_key("Values"));

        let total = total_nodes(&histogram);
        assert!(total >= 3, "Expected at least 3 nodes, got {}", total);
    }

    #[tokio::test]
    async fn test_join_plan_histogram() {
        let ctx = SessionContext::new();

        // Create a join plan
        let plan = ctx
            .sql(
                "SELECT t1.a, t2.b
                 FROM (VALUES (1, 2)) AS t1(a, b)
                 JOIN (VALUES (1, 3)) AS t2(a, b)
                 ON t1.a = t2.a",
            )
            .await
            .unwrap()
            .into_unoptimized_plan();

        let histogram = collect_plan_histogram(&plan);

        // Should have a Join node
        assert!(
            histogram.contains_key("Join"),
            "Expected Join node in histogram"
        );

        let summary = summarize_histogram(&histogram, 3);
        assert!(!summary.is_empty(), "Summary should not be empty");
    }

    #[tokio::test]
    async fn test_aggregate_plan_histogram() {
        let ctx = SessionContext::new();

        // Create an aggregate plan
        let plan = ctx
            .sql("SELECT a, COUNT(*) FROM (VALUES (1, 2), (1, 3), (2, 4)) AS t(a, b) GROUP BY a")
            .await
            .unwrap()
            .into_unoptimized_plan();

        let histogram = collect_plan_histogram(&plan);

        // Should have an Aggregate node
        assert!(
            histogram.contains_key("Aggregate"),
            "Expected Aggregate node in histogram"
        );
    }

    #[test]
    fn test_summarize_histogram() {
        let mut histogram = HashMap::new();
        histogram.insert("TableScan".to_string(), 5);
        histogram.insert("Filter".to_string(), 3);
        histogram.insert("Projection".to_string(), 2);
        histogram.insert("Join".to_string(), 1);

        let summary = summarize_histogram(&histogram, 2);
        // Should show the top 2 most common
        assert!(summary.contains("TableScan:5"));
        assert!(summary.contains("Filter:3"));
        assert!(!summary.contains("Join"));
    }

    #[test]
    fn test_total_nodes() {
        let mut histogram = HashMap::new();
        histogram.insert("TableScan".to_string(), 2);
        histogram.insert("Filter".to_string(), 3);
        histogram.insert("Join".to_string(), 1);

        assert_eq!(total_nodes(&histogram), 6);
    }
}
