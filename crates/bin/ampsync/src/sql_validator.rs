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
    sqlparser::ast::{
        Expr, Function, Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins,
    },
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

/// Sanitize a SQL query for use in a streaming dataset.
///
/// # Sanitization Rules
/// - Removes ORDER BY clauses recursively (non-incremental but safe to remove)
/// - Removes LIMIT clauses (non-incremental but safe to remove)
///
/// # Arguments
/// * `sql` - The SQL query to sanitize
///
/// # Returns
/// * `Ok(String)` containing sanitized SQL with ORDER BY and LIMIT removed
/// * `Err(BoxError)` if the SQL cannot be parsed
pub fn sanitize_sql(sql: &str) -> Result<String, BoxError> {
    // Parse the SQL using DataFusion's parser
    let mut statements =
        DFParser::parse_sql(sql).map_err(|e| format!("Failed to parse SQL: {}", e))?;

    // Remove ORDER BY and LIMIT from each statement
    for statement in &mut statements {
        remove_non_incremental_safe_ops(statement);
    }

    // Convert back to SQL string
    let sanitized = statements
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
        .join("; ");

    Ok(sanitized)
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

/// Remove ORDER BY and LIMIT clauses from a statement (recursively, in-place)
fn remove_non_incremental_safe_ops(statement: &mut DFStatement) {
    match statement {
        DFStatement::Statement(stmt) => {
            // Unwrap the inner sqlparser Statement from Box
            match stmt.as_mut() {
                Statement::Query(query) => {
                    remove_query_non_incremental_safe_ops(query);
                }
                // Other statement types (CREATE, INSERT, etc.) don't need modification
                _ => {}
            }
        }
        // DataFusion-specific statements don't need modification
        _ => {}
    }
}

/// Remove ORDER BY and LIMIT from a query (recursively)
fn remove_query_non_incremental_safe_ops(query: &mut Query) {
    // Remove the main query's ORDER BY and LIMIT
    query.order_by = None;
    query.limit = None;

    // Recursively process subqueries
    match &mut *query.body {
        SetExpr::Select(select) => {
            // Process FROM clause for subqueries
            for table in &mut select.from {
                remove_table_with_joins_non_incremental_safe_ops(table);
            }
        }
        SetExpr::Query(subquery) => {
            remove_query_non_incremental_safe_ops(subquery);
        }
        SetExpr::SetOperation { left, right, .. } => {
            remove_set_expr_non_incremental_safe_ops(left);
            remove_set_expr_non_incremental_safe_ops(right);
        }
        _ => {}
    }
}

/// Remove ORDER BY and LIMIT from a SET expression
fn remove_set_expr_non_incremental_safe_ops(expr: &mut SetExpr) {
    match expr {
        SetExpr::Select(select) => {
            for table in &mut select.from {
                remove_table_with_joins_non_incremental_safe_ops(table);
            }
        }
        SetExpr::Query(query) => {
            remove_query_non_incremental_safe_ops(query);
        }
        SetExpr::SetOperation { left, right, .. } => {
            remove_set_expr_non_incremental_safe_ops(left);
            remove_set_expr_non_incremental_safe_ops(right);
        }
        _ => {}
    }
}

/// Remove ORDER BY and LIMIT from tables with joins
fn remove_table_with_joins_non_incremental_safe_ops(table: &mut TableWithJoins) {
    // Process main table
    remove_table_factor_non_incremental_safe_ops(&mut table.relation);

    // Process joined tables
    for join in &mut table.joins {
        remove_table_factor_non_incremental_safe_ops(&mut join.relation);
    }
}

/// Remove ORDER BY and LIMIT from a table factor (subqueries)
fn remove_table_factor_non_incremental_safe_ops(table: &mut TableFactor) {
    match table {
        TableFactor::Derived { subquery, .. } => {
            remove_query_non_incremental_safe_ops(subquery);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            remove_table_with_joins_non_incremental_safe_ops(table_with_joins);
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

    #[test]
    fn test_simple_query_unchanged() {
        let sql = "SELECT * FROM blocks";
        let result = sanitize_sql(sql).unwrap();
        // Should be unchanged (except for whitespace normalization)
        assert!(result.contains("SELECT * FROM blocks"));
        assert!(!result.to_uppercase().contains("ORDER BY"));
    }

    #[test]
    fn test_query_with_where_unchanged() {
        let sql = "SELECT * FROM blocks WHERE block_num > 1000";
        let result = sanitize_sql(sql).unwrap();
        assert!(result.contains("WHERE"));
        assert!(result.contains("block_num > 1000"));
        assert!(!result.to_uppercase().contains("ORDER BY"));
    }

    #[test]
    fn test_remove_simple_order_by() {
        let sql = "SELECT * FROM blocks ORDER BY block_num";
        let result = sanitize_sql(sql).unwrap();
        // Should have ORDER BY removed
        assert!(result.contains("SELECT * FROM blocks"));
        assert!(!result.to_uppercase().contains("ORDER BY"));
    }

    #[test]
    fn test_remove_order_by_desc() {
        let sql = "SELECT * FROM anvil.block ORDER BY block_num DESC";
        let result = sanitize_sql(sql).unwrap();
        // Should have ORDER BY DESC removed
        assert!(result.contains("SELECT * FROM anvil.block"));
        assert!(!result.to_uppercase().contains("ORDER BY"));
        assert!(!result.to_uppercase().contains("DESC"));
    }

    #[test]
    fn test_remove_order_by_in_subquery() {
        let sql = "SELECT * FROM (SELECT * FROM blocks ORDER BY block_num) AS b";
        let result = sanitize_sql(sql).unwrap();
        // Should have ORDER BY removed from subquery
        assert!(result.contains("SELECT * FROM"));
        assert!(result.contains("blocks"));
        assert!(!result.to_uppercase().contains("ORDER BY"));
    }

    #[test]
    fn test_subquery_without_order_by_unchanged() {
        let sql = "SELECT * FROM (SELECT * FROM blocks WHERE block_num > 1000) AS b";
        let result = sanitize_sql(sql).unwrap();
        assert!(result.contains("WHERE"));
        assert!(result.contains("block_num > 1000"));
        assert!(!result.to_uppercase().contains("ORDER BY"));
    }

    #[test]
    fn test_join_unchanged() {
        let sql = "SELECT * FROM blocks b JOIN transactions t ON b.hash = t.block_hash";
        let result = sanitize_sql(sql).unwrap();
        assert!(result.to_uppercase().contains("JOIN"));
        assert!(!result.to_uppercase().contains("ORDER BY"));
    }

    #[test]
    fn test_multiple_order_by_clauses() {
        let sql = "SELECT * FROM (SELECT * FROM blocks ORDER BY block_num) AS b ORDER BY timestamp";
        let result = sanitize_sql(sql).unwrap();
        // Both ORDER BY clauses should be removed
        assert!(!result.to_uppercase().contains("ORDER BY"));
    }

    #[test]
    fn test_complex_query_with_order_by() {
        let sql = r#"
            SELECT a.*, b.count
            FROM (SELECT * FROM blocks WHERE block_num > 100 ORDER BY block_num) AS a
            JOIN (SELECT block_hash, COUNT(*) as count FROM transactions GROUP BY block_hash) AS b
            ON a.hash = b.block_hash
            ORDER BY a.block_num DESC
        "#;
        let result = sanitize_sql(sql).unwrap();
        // All ORDER BY clauses should be removed
        assert!(!result.to_uppercase().contains("ORDER BY"));
        // But other parts should remain
        assert!(result.to_uppercase().contains("JOIN"));
        assert!(result.to_uppercase().contains("WHERE"));
        assert!(result.to_uppercase().contains("GROUP BY"));
    }

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

    // Tests for LIMIT removal

    #[test]
    fn test_remove_limit() {
        let sql = "SELECT * FROM blocks LIMIT 100";
        let result = sanitize_sql(sql).unwrap();
        assert!(result.contains("SELECT * FROM blocks"));
        assert!(!result.to_uppercase().contains("LIMIT"));
    }

    #[test]
    fn test_remove_limit_with_order_by() {
        let sql = "SELECT * FROM blocks ORDER BY block_num LIMIT 100";
        let result = sanitize_sql(sql).unwrap();
        assert!(result.contains("SELECT * FROM blocks"));
        assert!(!result.to_uppercase().contains("ORDER BY"));
        assert!(!result.to_uppercase().contains("LIMIT"));
    }

    #[test]
    fn test_remove_limit_in_subquery() {
        let sql = "SELECT * FROM (SELECT * FROM blocks LIMIT 50) AS b";
        let result = sanitize_sql(sql).unwrap();
        assert!(result.contains("SELECT * FROM"));
        assert!(result.contains("blocks"));
        assert!(!result.to_uppercase().contains("LIMIT"));
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

    #[test]
    fn test_union_with_order_by() {
        let sql = "SELECT * FROM blocks WHERE block_num < 100 UNION SELECT * FROM blocks WHERE block_num > 1000 ORDER BY block_num";
        let result = sanitize_sql(sql).unwrap();
        assert!(result.to_uppercase().contains("UNION"));
        assert!(!result.to_uppercase().contains("ORDER BY"));
    }
}
