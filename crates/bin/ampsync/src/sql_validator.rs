//! SQL sanitization for dataset queries.
//!
//! This module sanitizes SQL queries to ensure they meet requirements for streaming datasets.
//! Key sanitization:
//! - Remove ORDER BY clauses (non-incremental queries are not supported)

use common::BoxError;
use datafusion::sql::{
    parser::{DFParser, Statement as DFStatement},
    sqlparser::ast::{Query, SetExpr, Statement, TableFactor, TableWithJoins},
};

/// Sanitize a SQL query for use in a streaming dataset.
///
/// # Sanitization Rules
/// - Removes ORDER BY clauses recursively (non-incremental queries not supported)
///
/// # Arguments
/// * `sql` - The SQL query to sanitize
///
/// # Returns
/// * `Ok(String)` containing either the original SQL (if valid) or sanitized SQL with ORDER BY removed
/// * `Err(BoxError)` if the SQL cannot be parsed
pub fn sanitize_sql(sql: &str) -> Result<String, BoxError> {
    // Parse the SQL using DataFusion's parser
    let mut statements =
        DFParser::parse_sql(sql).map_err(|e| format!("Failed to parse SQL: {}", e))?;

    // Remove ORDER BY from each statement
    for statement in &mut statements {
        remove_order_by(statement);
    }

    // Convert back to SQL string
    let sanitized = statements
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
        .join("; ");

    Ok(sanitized)
}

/// Remove ORDER BY clauses from a statement (recursively, in-place)
fn remove_order_by(statement: &mut DFStatement) {
    match statement {
        DFStatement::Statement(stmt) => {
            // Unwrap the inner sqlparser Statement from Box
            match stmt.as_mut() {
                Statement::Query(query) => {
                    remove_query_order_by(query);
                }
                // Other statement types (CREATE, INSERT, etc.) don't need modification
                _ => {}
            }
        }
        // DataFusion-specific statements don't need ORDER BY removal
        _ => {}
    }
}

/// Remove ORDER BY from a query (recursively)
fn remove_query_order_by(query: &mut Query) {
    // Remove the main query's ORDER BY
    query.order_by = None;

    // Recursively process subqueries
    match &mut *query.body {
        SetExpr::Select(select) => {
            // Process FROM clause for subqueries
            for table in &mut select.from {
                remove_table_with_joins_order_by(table);
            }
        }
        SetExpr::Query(subquery) => {
            remove_query_order_by(subquery);
        }
        SetExpr::SetOperation { left, right, .. } => {
            remove_set_expr_order_by(left);
            remove_set_expr_order_by(right);
        }
        _ => {}
    }
}

/// Remove ORDER BY from a SET expression
fn remove_set_expr_order_by(expr: &mut SetExpr) {
    match expr {
        SetExpr::Select(select) => {
            for table in &mut select.from {
                remove_table_with_joins_order_by(table);
            }
        }
        SetExpr::Query(query) => {
            remove_query_order_by(query);
        }
        SetExpr::SetOperation { left, right, .. } => {
            remove_set_expr_order_by(left);
            remove_set_expr_order_by(right);
        }
        _ => {}
    }
}

/// Remove ORDER BY from tables with joins
fn remove_table_with_joins_order_by(table: &mut TableWithJoins) {
    remove_table_factor_order_by(&mut table.relation);

    // Process joined tables
    for join in &mut table.joins {
        remove_table_factor_order_by(&mut join.relation);
    }
}

/// Remove ORDER BY from a table factor (subqueries)
fn remove_table_factor_order_by(table: &mut TableFactor) {
    match table {
        TableFactor::Derived { subquery, .. } => {
            remove_query_order_by(subquery);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            remove_table_with_joins_order_by(table_with_joins);
        }
        _ => {}
    }
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

    #[test]
    fn test_union_with_order_by() {
        let sql = "SELECT * FROM blocks WHERE block_num < 100 UNION SELECT * FROM blocks WHERE block_num > 1000 ORDER BY block_num";
        let result = sanitize_sql(sql).unwrap();
        assert!(result.to_uppercase().contains("UNION"));
        assert!(!result.to_uppercase().contains("ORDER BY"));
    }
}
