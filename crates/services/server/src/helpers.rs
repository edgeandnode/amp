use datafusion::{
    logical_expr::sqlparser::{ast, ast::Expr},
    sql,
};

/// `SETTINGS` key that enables streaming mode.
const STREAM_SETTING: &str = "stream";

/// Check whether a parsed statement contains `SETTINGS stream = true`.
///
/// ```sql
/// SELECT * FROM eth_firehose.blocks SETTINGS stream = true
/// ```
pub fn is_streaming(stmt: &sql::parser::Statement) -> bool {
    let sql::parser::Statement::Statement(box_stmt) = stmt else {
        return false;
    };
    let ast::Statement::Query(query) = box_stmt.as_ref() else {
        return false;
    };
    let Some(settings) = &query.settings else {
        return false;
    };
    let Some(setting) = settings
        .iter()
        .find(|s| s.key.value.eq_ignore_ascii_case(STREAM_SETTING))
    else {
        return false;
    };
    let Expr::Value(v) = &setting.value else {
        return false;
    };
    let ast::Value::Boolean(is_streaming) = v.value else {
        return false;
    };

    is_streaming
}

#[cfg(test)]
mod tests {
    use common::sql_str::SqlStr;

    use super::*;

    #[test]
    fn is_streaming_with_stream_true_returns_true() {
        //* Given
        let stmt = parse_stmt("SELECT * FROM test SETTINGS stream = true");

        //* When
        let result = is_streaming(&stmt);

        //* Then
        assert!(result, "stream = true should be detected as streaming");
    }

    #[test]
    fn is_streaming_with_uppercase_stream_true_returns_true() {
        //* Given
        let stmt = parse_stmt("SELECT * FROM test SETTINGS STREAM = True");

        //* When
        let result = is_streaming(&stmt);

        //* Then
        assert!(
            result,
            "case-insensitive STREAM = True should be detected as streaming"
        );
    }

    #[test]
    fn is_streaming_with_numeric_value_returns_false() {
        //* Given
        let stmt = parse_stmt("SELECT * FROM test SETTINGS STREAM = 1");

        //* When
        let result = is_streaming(&stmt);

        //* Then
        assert!(
            !result,
            "numeric value 1 should not be detected as streaming"
        );
    }

    #[test]
    fn is_streaming_with_subquery_and_stream_true_returns_true() {
        //* Given
        let stmt = parse_stmt("SELECT * FROM (select * from test) as t SETTINGS stream = true");

        //* When
        let result = is_streaming(&stmt);

        //* Then
        assert!(
            result,
            "subquery with stream = true should be detected as streaming"
        );
    }

    #[test]
    fn is_streaming_without_settings_returns_false() {
        //* Given
        let stmt = parse_stmt("SELECT * FROM test");

        //* When
        let result = is_streaming(&stmt);

        //* Then
        assert!(!result, "query without SETTINGS should not be streaming");
    }

    fn parse_stmt(sql: &str) -> sql::parser::Statement {
        let sql_str: SqlStr = sql.parse().expect("sql should be valid SqlStr");
        common::sql::parse(&sql_str).expect("sql should parse successfully")
    }
}
