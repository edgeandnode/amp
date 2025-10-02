//! SQL validation for dataset queries.
//!
//! This module validates SQL queries to ensure they meet requirements for streaming datasets.
//! Key validation:
//! - Reject queries with ORDER BY clauses (non-incremental queries are not supported)

use common::BoxError;
use datafusion::sql::{
    parser::{DFParser, Statement as DFStatement},
    sqlparser::ast::{Query, SetExpr, Statement, TableFactor, TableWithJoins},
};

/// Validate a SQL query for use in a streaming dataset.
///
/// # Validation Rules
/// - Queries with ORDER BY clauses are rejected (non-incremental queries not supported)
///
/// # Arguments
/// * `sql` - The SQL query to validate
///
/// # Returns
/// * `Ok(())` if the query is valid
/// * `Err(BoxError)` with a description of the validation error
pub fn validate_sql(sql: &str) -> Result<(), BoxError> {
    // Parse the SQL using DataFusion's parser
    let statements = DFParser::parse_sql(sql).map_err(|e| format!("Failed to parse SQL: {}", e))?;

    // Check each statement for ORDER BY clauses
    for statement in statements {
        check_order_by(&statement)?;
    }

    Ok(())
}

/// Check if a statement contains an ORDER BY clause (recursively)
fn check_order_by(statement: &DFStatement) -> Result<(), BoxError> {
    match statement {
        DFStatement::Statement(stmt) => {
            // Unwrap the inner sqlparser Statement
            match stmt.as_ref() {
                Statement::Query(query) => {
                    check_query_order_by(query)?;
                }
                // Other statement types (CREATE, INSERT, etc.) are allowed
                _ => {}
            }
        }
        // DataFusion-specific statements don't need ORDER BY validation
        _ => {}
    }

    Ok(())
}

/// Check if a query contains an ORDER BY clause
fn check_query_order_by(query: &Query) -> Result<(), BoxError> {
    // Check the main query's ORDER BY
    if query.order_by.is_some() {
        return Err(
            "ORDER BY clauses are not supported in streaming queries (non-incremental queries not allowed)"
                .into(),
        );
    }

    // Recursively check subqueries
    match &*query.body {
        SetExpr::Select(select) => {
            // Check FROM clause for subqueries
            for table in &select.from {
                check_table_with_joins(table)?;
            }
        }
        SetExpr::Query(subquery) => {
            check_query_order_by(subquery)?;
        }
        SetExpr::SetOperation { left, right, .. } => {
            check_set_expr(&**left)?;
            check_set_expr(&**right)?;
        }
        _ => {}
    }

    Ok(())
}

/// Check a SET expression for ORDER BY
fn check_set_expr(expr: &SetExpr) -> Result<(), BoxError> {
    match expr {
        SetExpr::Select(select) => {
            for table in &select.from {
                check_table_with_joins(table)?;
            }
        }
        SetExpr::Query(query) => {
            check_query_order_by(query)?;
        }
        SetExpr::SetOperation { left, right, .. } => {
            check_set_expr(&**left)?;
            check_set_expr(&**right)?;
        }
        _ => {}
    }

    Ok(())
}

/// Check a table with joins for subqueries
fn check_table_with_joins(table: &TableWithJoins) -> Result<(), BoxError> {
    check_table_factor(&table.relation)?;

    // Check joined tables
    for join in &table.joins {
        check_table_factor(&join.relation)?;
    }

    Ok(())
}

/// Check a table factor for subqueries with ORDER BY
fn check_table_factor(table: &TableFactor) -> Result<(), BoxError> {
    match table {
        TableFactor::Derived { subquery, .. } => {
            check_query_order_by(subquery)?;
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            check_table_with_joins(table_with_joins)?;
        }
        _ => {}
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_simple_query() {
        let sql = "SELECT * FROM blocks";
        assert!(validate_sql(sql).is_ok());
    }

    #[test]
    fn test_valid_query_with_where() {
        let sql = "SELECT * FROM blocks WHERE block_num > 1000";
        assert!(validate_sql(sql).is_ok());
    }

    #[test]
    fn test_reject_order_by() {
        let sql = "SELECT * FROM blocks ORDER BY block_num";
        let result = validate_sql(sql);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("ORDER BY"));
    }

    #[test]
    fn test_reject_order_by_in_subquery() {
        let sql = "SELECT * FROM (SELECT * FROM blocks ORDER BY block_num) AS b";
        let result = validate_sql(sql);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("ORDER BY"));
    }

    #[test]
    fn test_valid_subquery_without_order_by() {
        let sql = "SELECT * FROM (SELECT * FROM blocks WHERE block_num > 1000) AS b";
        assert!(validate_sql(sql).is_ok());
    }

    #[test]
    fn test_valid_join() {
        let sql = "SELECT * FROM blocks b JOIN transactions t ON b.hash = t.block_hash";
        assert!(validate_sql(sql).is_ok());
    }
}
