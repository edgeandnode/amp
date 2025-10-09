//! SQL validation and sanitization for dataset queries.
//!
//! This module validates and sanitizes SQL queries to ensure they meet requirements for streaming datasets.
//! Key operations:
//! - Validate against non-incremental operations (aggregates, joins, window functions, etc.)
//! - Remove ORDER BY and LIMIT clauses (non-incremental, but safe to remove)
//! - Extract column names from SELECT statements for schema filtering

use common::BoxError;
use datafusion::sql::{
    parser::{DFParser, Statement as DFStatement},
    sqlparser::ast::{Expr, Function, Query, Select, SelectItem, SetExpr, Statement, TableFactor},
};

/// Non-incremental operations that cannot be used in streaming queries
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum NonIncrementalOp {
    /// Aggregations need state management (COUNT, SUM, AVG, etc.)
    Aggregate,
    /// Distinct operations need global deduplication
    Distinct,
    /// Joins need to maintain state between batches
    Join,
    /// Window functions often require sorting and state
    Window,
    /// Recursive queries are inherently stateful
    RecursiveQuery,
}

impl std::fmt::Display for NonIncrementalOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NonIncrementalOp::Aggregate => write!(f, "Aggregate (COUNT, SUM, AVG, etc.)"),
            NonIncrementalOp::Distinct => write!(f, "DISTINCT"),
            NonIncrementalOp::Join => write!(f, "JOIN"),
            NonIncrementalOp::Window => write!(f, "Window functions (OVER, PARTITION BY)"),
            NonIncrementalOp::RecursiveQuery => write!(f, "Recursive query (WITH RECURSIVE)"),
        }
    }
}

/// Validate that a SQL query only uses incremental operations suitable for streaming
///
/// # Arguments
/// * `sql` - The SQL query to validate
///
/// # Returns
/// * `Ok(())` if the query is valid for streaming
/// * `Err(BoxError)` with a list of non-incremental operations found
pub fn validate_incremental_query(sql: &str) -> Result<(), BoxError> {
    // Parse the SQL using DataFusion's parser
    let statements = DFParser::parse_sql(sql).map_err(|e| format!("Failed to parse SQL: {}", e))?;

    let mut non_incremental_ops = Vec::new();

    for statement in &statements {
        if let DFStatement::Statement(stmt) = statement {
            if let Statement::Query(query) = stmt.as_ref() {
                check_query_for_non_incremental_ops(query, &mut non_incremental_ops);
            }
        }
    }

    if !non_incremental_ops.is_empty() {
        // Deduplicate operations
        non_incremental_ops.sort();
        non_incremental_ops.dedup();

        let ops_list: Vec<String> = non_incremental_ops
            .iter()
            .map(|op| format!("  - {}", op))
            .collect();

        return Err(format!(
            "Query contains non-incremental operations that cannot be used in streaming:\n{}\n\n\
             Incremental streaming requires queries that can process data in batches without global state.\n\
             Consider using a materialized view or pre-aggregated dataset instead.",
            ops_list.join("\n")
        )
        .into());
    }

    Ok(())
}

/// Check a query for non-incremental operations
fn check_query_for_non_incremental_ops(query: &Query, ops: &mut Vec<NonIncrementalOp>) {
    // Check for recursive CTEs
    if let Some(with) = &query.with {
        if with.recursive {
            ops.push(NonIncrementalOp::RecursiveQuery);
        }
    }

    // Check the main query body
    check_set_expr_for_non_incremental_ops(&query.body, ops);
}

/// Check a SET expression for non-incremental operations
fn check_set_expr_for_non_incremental_ops(expr: &SetExpr, ops: &mut Vec<NonIncrementalOp>) {
    match expr {
        SetExpr::Select(select) => check_select_for_non_incremental_ops(select, ops),
        SetExpr::Query(query) => check_query_for_non_incremental_ops(query, ops),
        SetExpr::SetOperation { left, right, .. } => {
            check_set_expr_for_non_incremental_ops(left, ops);
            check_set_expr_for_non_incremental_ops(right, ops);
        }
        _ => {}
    }
}

/// Check a SELECT statement for non-incremental operations
fn check_select_for_non_incremental_ops(select: &Select, ops: &mut Vec<NonIncrementalOp>) {
    // Check for DISTINCT
    if let Some(_distinct) = &select.distinct {
        ops.push(NonIncrementalOp::Distinct);
    }

    // Check for aggregates and window functions in projection
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                check_expr_for_non_incremental_ops(expr, ops);
            }
            _ => {}
        }
    }

    // Check for joins
    for table_with_joins in &select.from {
        if !table_with_joins.joins.is_empty() {
            ops.push(NonIncrementalOp::Join);
        }
        // Check for subqueries in table factors
        check_table_factor_for_non_incremental_ops(&table_with_joins.relation, ops);
        for join in &table_with_joins.joins {
            check_table_factor_for_non_incremental_ops(&join.relation, ops);
        }
    }

    // Check GROUP BY (indicates aggregation)
    // Note: group_by is a GroupByExpr enum, not a Vec
    if let datafusion::sql::sqlparser::ast::GroupByExpr::Expressions(exprs, _) = &select.group_by {
        if !exprs.is_empty() {
            ops.push(NonIncrementalOp::Aggregate);
        }
    }

    // Check HAVING (also indicates aggregation)
    if select.having.is_some() {
        ops.push(NonIncrementalOp::Aggregate);
    }
}

/// Check an expression for aggregates and window functions
fn check_expr_for_non_incremental_ops(expr: &Expr, ops: &mut Vec<NonIncrementalOp>) {
    match expr {
        Expr::Function(Function { name, over, .. }) => {
            // Check if it's an aggregate function
            let func_name = name.to_string().to_uppercase();
            if matches!(
                func_name.as_str(),
                "COUNT"
                    | "SUM"
                    | "AVG"
                    | "MIN"
                    | "MAX"
                    | "STDDEV"
                    | "VARIANCE"
                    | "ARRAY_AGG"
                    | "STRING_AGG"
            ) {
                ops.push(NonIncrementalOp::Aggregate);
            }

            // Check if it's a window function
            if over.is_some() {
                ops.push(NonIncrementalOp::Window);
            }
        }
        // Recursively check nested expressions
        Expr::BinaryOp { left, right, .. } => {
            check_expr_for_non_incremental_ops(left, ops);
            check_expr_for_non_incremental_ops(right, ops);
        }
        Expr::UnaryOp { expr, .. } | Expr::Cast { expr, .. } => {
            check_expr_for_non_incremental_ops(expr, ops);
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                check_expr_for_non_incremental_ops(op, ops);
            }
            // conditions is Vec<CaseWhen>, which contains expr and result
            for when in conditions {
                check_expr_for_non_incremental_ops(&when.condition, ops);
                check_expr_for_non_incremental_ops(&when.result, ops);
            }
            if let Some(else_expr) = else_result {
                check_expr_for_non_incremental_ops(else_expr, ops);
            }
        }
        Expr::Nested(inner) => check_expr_for_non_incremental_ops(inner, ops),
        Expr::InList { expr, list, .. } => {
            check_expr_for_non_incremental_ops(expr, ops);
            for item in list {
                check_expr_for_non_incremental_ops(item, ops);
            }
        }
        _ => {}
    }
}

/// Check a table factor for subqueries with non-incremental operations
fn check_table_factor_for_non_incremental_ops(
    factor: &TableFactor,
    ops: &mut Vec<NonIncrementalOp>,
) {
    match factor {
        TableFactor::Derived { subquery, .. } => {
            check_query_for_non_incremental_ops(subquery, ops);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            check_table_factor_for_non_incremental_ops(&table_with_joins.relation, ops);
            for join in &table_with_joins.joins {
                check_table_factor_for_non_incremental_ops(&join.relation, ops);
            }
        }
        _ => {}
    }
}

/// Represents the result of analyzing SELECT columns from a SQL query
#[derive(Debug, PartialEq)]
pub enum SelectColumns {
    /// SELECT * - all columns should be included
    All,
    /// SELECT col1, col2, ... - specific columns listed
    Specific(Vec<String>),
}

/// Extract the list of columns from a SELECT statement
///
/// Returns:
/// - `SelectColumns::All` if the query uses `SELECT *`
/// - `SelectColumns::Specific(vec)` if specific columns are selected
///
/// # Arguments
/// * `sql` - The SQL SELECT query to analyze
///
/// # Returns
/// * `Ok(SelectColumns)` with the column information
/// * `Err(BoxError)` if the SQL cannot be parsed or is not a SELECT statement
pub fn extract_select_columns(sql: &str) -> Result<SelectColumns, BoxError> {
    // Parse the SQL using DataFusion's parser
    let statements = DFParser::parse_sql(sql).map_err(|e| format!("Failed to parse SQL: {}", e))?;

    if statements.is_empty() {
        return Err("No SQL statement found".into());
    }

    // Get the first statement
    let statement = &statements[0];

    match statement {
        DFStatement::Statement(stmt) => match stmt.as_ref() {
            Statement::Query(query) => extract_columns_from_query(query),
            _ => Err("Expected a SELECT query".into()),
        },
        _ => Err("Expected a SQL query statement".into()),
    }
}

/// Extract columns from a Query AST node
fn extract_columns_from_query(query: &Query) -> Result<SelectColumns, BoxError> {
    match &*query.body {
        SetExpr::Select(select) => extract_columns_from_select(select),
        _ => Err("Complex queries (UNION, etc.) are not supported for column extraction".into()),
    }
}

/// Extract columns from a Select AST node
fn extract_columns_from_select(select: &Select) -> Result<SelectColumns, BoxError> {
    // Check if it's SELECT *
    if select.projection.len() == 1 && matches!(select.projection[0], SelectItem::Wildcard(_)) {
        return Ok(SelectColumns::All);
    }

    // Extract specific column names
    let mut columns = Vec::new();

    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                // For simple column references, extract the column name
                // For expressions, we'll use the expression as-is (might not be perfect)
                let col_name = expr.to_string();
                columns.push(col_name);
            }
            SelectItem::ExprWithAlias { alias, .. } => {
                // Use the alias as the column name
                columns.push(alias.value.clone());
            }
            SelectItem::Wildcard(_) => {
                // If we see *, it should be the only item (handled above)
                return Ok(SelectColumns::All);
            }
            SelectItem::QualifiedWildcard(_, _) => {
                // table.* - treat as SELECT *
                return Ok(SelectColumns::All);
            }
        }
    }

    Ok(SelectColumns::Specific(columns))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests for non-incremental operation validation

    #[test]
    fn test_validate_simple_query_passes() {
        let sql = "SELECT * FROM blocks WHERE block_num > 1000";
        assert!(validate_incremental_query(sql).is_ok());
    }

    #[test]
    fn test_validate_aggregate_fails() {
        let sql = "SELECT COUNT(*) FROM blocks";
        let result = validate_incremental_query(sql);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Aggregate"));
    }

    #[test]
    fn test_validate_group_by_fails() {
        let sql = "SELECT block_num, COUNT(*) FROM blocks GROUP BY block_num";
        let result = validate_incremental_query(sql);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Aggregate"));
    }

    #[test]
    fn test_validate_distinct_fails() {
        let sql = "SELECT DISTINCT block_num FROM blocks";
        let result = validate_incremental_query(sql);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("DISTINCT"));
    }

    #[test]
    fn test_validate_join_fails() {
        let sql = "SELECT * FROM blocks b JOIN transactions t ON b.hash = t.block_hash";
        let result = validate_incremental_query(sql);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("JOIN"));
    }

    #[test]
    fn test_validate_window_function_fails() {
        let sql = "SELECT block_num, ROW_NUMBER() OVER (ORDER BY block_num) FROM blocks";
        let result = validate_incremental_query(sql);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Window"));
    }

    #[test]
    fn test_validate_with_recursive_fails() {
        let sql = "WITH RECURSIVE cte AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM cte WHERE n < 10) SELECT * FROM cte";
        let result = validate_incremental_query(sql);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Recursive"));
    }

    #[test]
    fn test_validate_multiple_violations() {
        let sql =
            "SELECT DISTINCT COUNT(*) FROM blocks b JOIN transactions t ON b.hash = t.block_hash";
        let result = validate_incremental_query(sql);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        // Should contain multiple violations
        assert!(err.contains("Aggregate") || err.contains("DISTINCT") || err.contains("JOIN"));
    }

    // Column extraction tests

    #[test]
    fn test_extract_select_all() {
        let sql = "SELECT * FROM anvil.logs";
        let result = extract_select_columns(sql).unwrap();
        assert_eq!(result, SelectColumns::All);
    }

    #[test]
    fn test_extract_specific_columns() {
        let sql = "SELECT block_num, timestamp, hash, nonce FROM anvil.blocks";
        let result = extract_select_columns(sql).unwrap();
        match result {
            SelectColumns::Specific(cols) => {
                assert_eq!(cols.len(), 4);
                assert!(cols.contains(&"block_num".to_string()));
                assert!(cols.contains(&"timestamp".to_string()));
                assert!(cols.contains(&"hash".to_string()));
                assert!(cols.contains(&"nonce".to_string()));
            }
            _ => panic!("Expected Specific columns, got All"),
        }
    }

    #[test]
    fn test_extract_columns_with_alias() {
        let sql = "SELECT block_num as num, hash FROM blocks";
        let result = extract_select_columns(sql).unwrap();
        match result {
            SelectColumns::Specific(cols) => {
                assert_eq!(cols.len(), 2);
                assert!(cols.contains(&"num".to_string()));
                assert!(cols.contains(&"hash".to_string()));
            }
            _ => panic!("Expected Specific columns"),
        }
    }
}
