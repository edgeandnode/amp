//! SELECT statement validation for dataset authoring.
//!
//! This module validates that SQL model files contain exactly one SELECT statement,
//! rejecting CTAS, DDL, and DML statements.
//!
//! # SELECT Requirements
//!
//! For derived datasets, SQL model files must:
//! - Contain exactly one `SELECT ...` statement
//! - Not use CTAS (`CREATE TABLE ... AS SELECT`)
//! - Not use DDL (`CREATE TABLE`, `CREATE INDEX`, etc.)
//! - Not use DML (`INSERT`, `UPDATE`, `DELETE`)
//!
//! # Example
//!
//! ```sql
//! -- Valid SELECT
//! SELECT block_num, tx_hash, from_address, to_address, value
//! FROM source.transactions
//! WHERE value > 0
//! ```
//!
//! # Note
//!
//! Table names are derived from file names via model discovery, not from SQL content.

use datafusion::sql::{parser::Statement, sqlparser::ast};
use datasets_common::table_name::TableName;

/// Validated SELECT information extracted from a SQL statement.
#[derive(Debug, Clone)]
pub struct SelectInfo {
    /// The table name (from discovery, not SQL).
    pub table_name: TableName,
    /// The SELECT query.
    pub query: Box<ast::Query>,
}

/// Errors that occur when validating SELECT statements.
#[derive(Debug, thiserror::Error)]
pub enum SelectValidationError {
    /// The SQL statement is not a SELECT query.
    ///
    /// This error occurs when the parsed SQL is a DDL statement (CREATE TABLE, etc.),
    /// DML statement (INSERT, UPDATE, DELETE), or other non-query statement.
    /// Dataset models must contain only SELECT queries.
    #[error("expected SELECT statement, found {found}")]
    NotSelect {
        /// Description of what statement type was found.
        found: String,
    },
}

/// Validates that a statement is a SELECT query and extracts the query.
///
/// This function validates that the parsed statement is a pure SELECT query,
/// rejecting CTAS, DDL, and DML statements. The table name is provided externally
/// from model discovery (based on filename), not extracted from the SQL.
///
/// # Arguments
///
/// * `stmt` - A parsed DataFusion statement to validate.
/// * `table_name` - The table name from model discovery (file stem).
///
/// # Returns
///
/// * `Ok(SelectInfo)` - The statement is a valid SELECT query.
/// * `Err(SelectValidationError::NotSelect)` - The statement is not a SELECT.
///
/// # Examples
///
/// ```ignore
/// use common::sql::parse;
/// use dataset_authoring::query::validate_select;
/// use datasets_common::table_name::TableName;
///
/// let sql = "SELECT * FROM source.txs".parse().unwrap();
/// let stmt = parse(&sql).unwrap();
/// let table_name: TableName = "transfers".parse().unwrap();
/// let select_info = validate_select(&stmt, table_name).unwrap();
/// assert_eq!(select_info.table_name.as_str(), "transfers");
/// ```
pub fn validate_select(
    stmt: &Statement,
    table_name: TableName,
) -> Result<SelectInfo, SelectValidationError> {
    match stmt {
        Statement::Statement(inner) => match inner.as_ref() {
            ast::Statement::Query(query) => Ok(SelectInfo {
                table_name,
                query: query.clone(),
            }),
            ast::Statement::CreateTable(_) => Err(SelectValidationError::NotSelect {
                found: "CREATE TABLE".to_string(),
            }),
            ast::Statement::Insert(_) => Err(SelectValidationError::NotSelect {
                found: "INSERT".to_string(),
            }),
            ast::Statement::Update { .. } => Err(SelectValidationError::NotSelect {
                found: "UPDATE".to_string(),
            }),
            ast::Statement::Delete(_) => Err(SelectValidationError::NotSelect {
                found: "DELETE".to_string(),
            }),
            ast::Statement::CreateView { .. } => Err(SelectValidationError::NotSelect {
                found: "CREATE VIEW".to_string(),
            }),
            ast::Statement::CreateIndex(_) => Err(SelectValidationError::NotSelect {
                found: "CREATE INDEX".to_string(),
            }),
            ast::Statement::AlterTable { .. } => Err(SelectValidationError::NotSelect {
                found: "ALTER TABLE".to_string(),
            }),
            ast::Statement::Drop { .. } => Err(SelectValidationError::NotSelect {
                found: "DROP".to_string(),
            }),
            ast::Statement::Truncate { .. } => Err(SelectValidationError::NotSelect {
                found: "TRUNCATE".to_string(),
            }),
            other => Err(SelectValidationError::NotSelect {
                found: statement_kind_description(other),
            }),
        },
        Statement::CreateExternalTable(_) => Err(SelectValidationError::NotSelect {
            found: "CREATE EXTERNAL TABLE".to_string(),
        }),
        Statement::CopyTo(_) => Err(SelectValidationError::NotSelect {
            found: "COPY TO".to_string(),
        }),
        Statement::Explain(_) => Err(SelectValidationError::NotSelect {
            found: "EXPLAIN".to_string(),
        }),
        Statement::Reset(_) => Err(SelectValidationError::NotSelect {
            found: "RESET".to_string(),
        }),
    }
}

/// Returns a human-readable description of a sqlparser Statement kind.
fn statement_kind_description(stmt: &ast::Statement) -> String {
    // Extract the variant name from Debug output
    format!("{:?}", std::mem::discriminant(stmt))
        .split('(')
        .next()
        .unwrap_or("unknown statement")
        .to_string()
}

#[cfg(test)]
mod tests {
    use common::sql::parse;
    use datasets_derived::sql_str::SqlStr;

    use super::*;

    /// Helper to parse SQL into a Statement
    fn parse_sql(sql: &str) -> Statement {
        let sql_str: SqlStr = sql.parse().expect("SQL string should be valid");
        parse(&sql_str).expect("SQL should parse successfully")
    }

    /// Helper to create a table name for tests
    fn test_table_name(name: &str) -> TableName {
        name.parse().expect("table name should be valid")
    }

    mod valid_select_tests {
        use super::*;

        #[test]
        fn valid_simple_select() {
            //* Given
            let stmt = parse_sql("SELECT id, name FROM t");
            let table_name = test_table_name("my_table");

            //* When
            let result = validate_select(&stmt, table_name);

            //* Then
            let info = result.expect("should validate successfully");
            assert_eq!(
                info.table_name.as_str(),
                "my_table",
                "table name should match provided name"
            );
        }

        #[test]
        fn valid_select_star() {
            //* Given
            let stmt = parse_sql("SELECT * FROM source");
            let table_name = test_table_name("transfers");

            //* When
            let result = validate_select(&stmt, table_name);

            //* Then
            let info = result.expect("should validate successfully");
            assert_eq!(info.table_name.as_str(), "transfers");
        }

        #[test]
        fn valid_select_with_where() {
            //* Given
            let stmt = parse_sql("SELECT * FROM t WHERE x > 0");
            let table_name = test_table_name("filtered");

            //* When
            let result = validate_select(&stmt, table_name);

            //* Then
            result.expect("should validate SELECT with WHERE");
        }

        #[test]
        fn valid_select_with_joins() {
            //* Given
            let stmt = parse_sql(
                "SELECT t.block_num, t.tx_hash, b.timestamp \
                 FROM source.transactions t \
                 JOIN source.blocks b ON t.block_num = b.number",
            );
            let table_name = test_table_name("joined");

            //* When
            let result = validate_select(&stmt, table_name);

            //* Then
            let info = result.expect("should validate SELECT with JOIN");
            assert_eq!(info.table_name.as_str(), "joined");
        }

        #[test]
        fn valid_select_with_cte() {
            //* Given
            let stmt = parse_sql(
                "WITH filtered AS (SELECT * FROM source WHERE value > 0) \
                 SELECT * FROM filtered",
            );
            let table_name = test_table_name("result");

            //* When
            let result = validate_select(&stmt, table_name);

            //* Then
            let info = result.expect("should validate SELECT with CTE");
            assert_eq!(info.table_name.as_str(), "result");
        }

        #[test]
        fn valid_select_with_subquery() {
            //* Given
            let stmt = parse_sql(
                "SELECT * FROM (SELECT id, value FROM source WHERE value > 100) AS filtered",
            );
            let table_name = test_table_name("subquery_result");

            //* When
            let result = validate_select(&stmt, table_name);

            //* Then
            result.expect("should validate SELECT with subquery");
        }

        #[test]
        fn valid_select_with_union() {
            //* Given
            let stmt =
                parse_sql("SELECT id, name FROM table_a UNION ALL SELECT id, name FROM table_b");
            let table_name = test_table_name("combined");

            //* When
            let result = validate_select(&stmt, table_name);

            //* Then
            result.expect("should validate SELECT with UNION");
        }
    }

    mod reject_ctas_tests {
        use super::*;

        #[test]
        fn rejects_simple_ctas() {
            //* Given
            let stmt = parse_sql("CREATE TABLE transfers AS SELECT * FROM source");
            let table_name = test_table_name("transfers");

            //* When
            let result = validate_select(&stmt, table_name);

            //* Then
            let err = result.expect_err("should reject CTAS");
            assert!(
                matches!(&err, SelectValidationError::NotSelect { found } if found == "CREATE TABLE"),
                "expected NotSelect error for CREATE TABLE, got: {:?}",
                err
            );
        }

        #[test]
        fn rejects_ctas_with_complex_select() {
            //* Given
            let stmt = parse_sql(
                "CREATE TABLE filtered AS \
                 SELECT block_num, tx_hash FROM source.transactions WHERE value > 0",
            );
            let table_name = test_table_name("filtered");

            //* When
            let result = validate_select(&stmt, table_name);

            //* Then
            let err = result.expect_err("should reject CTAS with complex SELECT");
            assert!(
                matches!(&err, SelectValidationError::NotSelect { found } if found == "CREATE TABLE"),
                "expected NotSelect error, got: {:?}",
                err
            );
        }
    }

    mod reject_ddl_tests {
        use super::*;

        #[test]
        fn rejects_create_table() {
            //* Given
            let stmt = parse_sql("CREATE TABLE test (id INT, name VARCHAR)");
            let table_name = test_table_name("test");

            //* When
            let result = validate_select(&stmt, table_name);

            //* Then
            let err = result.expect_err("should reject CREATE TABLE");
            assert!(
                matches!(&err, SelectValidationError::NotSelect { found } if found == "CREATE TABLE"),
                "expected NotSelect error, got: {:?}",
                err
            );
        }

        #[test]
        fn rejects_create_view() {
            //* Given
            let stmt = parse_sql("CREATE VIEW test AS SELECT * FROM source");
            let table_name = test_table_name("test");

            //* When
            let result = validate_select(&stmt, table_name);

            //* Then
            let err = result.expect_err("should reject CREATE VIEW");
            assert!(
                matches!(&err, SelectValidationError::NotSelect { found } if found == "CREATE VIEW"),
                "expected NotSelect error, got: {:?}",
                err
            );
        }

        #[test]
        fn rejects_create_index() {
            //* Given
            let stmt = parse_sql("CREATE INDEX idx ON test (id)");
            let table_name = test_table_name("test");

            //* When
            let result = validate_select(&stmt, table_name);

            //* Then
            let err = result.expect_err("should reject CREATE INDEX");
            assert!(
                matches!(&err, SelectValidationError::NotSelect { found } if found == "CREATE INDEX"),
                "expected NotSelect error, got: {:?}",
                err
            );
        }

        #[test]
        fn rejects_drop() {
            //* Given
            let stmt = parse_sql("DROP TABLE test");
            let table_name = test_table_name("test");

            //* When
            let result = validate_select(&stmt, table_name);

            //* Then
            let err = result.expect_err("should reject DROP");
            assert!(
                matches!(&err, SelectValidationError::NotSelect { found } if found == "DROP"),
                "expected NotSelect error, got: {:?}",
                err
            );
        }
    }

    mod reject_dml_tests {
        use super::*;

        #[test]
        fn rejects_insert() {
            //* Given
            let stmt = parse_sql("INSERT INTO test VALUES (1, 'a')");
            let table_name = test_table_name("test");

            //* When
            let result = validate_select(&stmt, table_name);

            //* Then
            let err = result.expect_err("should reject INSERT");
            assert!(
                matches!(&err, SelectValidationError::NotSelect { found } if found == "INSERT"),
                "expected NotSelect error, got: {:?}",
                err
            );
        }

        #[test]
        fn rejects_update() {
            //* Given
            let stmt = parse_sql("UPDATE test SET name = 'b' WHERE id = 1");
            let table_name = test_table_name("test");

            //* When
            let result = validate_select(&stmt, table_name);

            //* Then
            let err = result.expect_err("should reject UPDATE");
            assert!(
                matches!(&err, SelectValidationError::NotSelect { found } if found == "UPDATE"),
                "expected NotSelect error, got: {:?}",
                err
            );
        }

        #[test]
        fn rejects_delete() {
            //* Given
            let stmt = parse_sql("DELETE FROM test WHERE id = 1");
            let table_name = test_table_name("test");

            //* When
            let result = validate_select(&stmt, table_name);

            //* Then
            let err = result.expect_err("should reject DELETE");
            assert!(
                matches!(&err, SelectValidationError::NotSelect { found } if found == "DELETE"),
                "expected NotSelect error, got: {:?}",
                err
            );
        }

        #[test]
        fn rejects_truncate() {
            //* Given
            let stmt = parse_sql("TRUNCATE TABLE test");
            let table_name = test_table_name("test");

            //* When
            let result = validate_select(&stmt, table_name);

            //* Then
            let err = result.expect_err("should reject TRUNCATE");
            assert!(
                matches!(&err, SelectValidationError::NotSelect { found } if found == "TRUNCATE"),
                "expected NotSelect error, got: {:?}",
                err
            );
        }
    }

    mod other_statement_tests {
        use super::*;

        #[test]
        fn rejects_explain() {
            //* Given
            let stmt = parse_sql("EXPLAIN SELECT * FROM test");
            let table_name = test_table_name("test");

            //* When
            let result = validate_select(&stmt, table_name);

            //* Then
            let err = result.expect_err("should reject EXPLAIN");
            assert!(
                matches!(&err, SelectValidationError::NotSelect { found } if found == "EXPLAIN"),
                "expected NotSelect error, got: {:?}",
                err
            );
        }
    }
}
