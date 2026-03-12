use datafusion::{
    logical_expr::sqlparser::{ast, ast::Expr},
    sql,
};
use datasets_common::network_id::NetworkId;

/// `SETTINGS` key that enables streaming mode.
const STREAM_SETTING: &str = "stream";

/// `SETTINGS` key that specifies the lead network for multi-network streaming queries.
const LEAD_NETWORK_SETTING: &str = "lead_network";

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

/// Extract the `lead_network` setting from a parsed statement, if present.
///
/// Returns `None` if the setting is absent, not a string, or not a valid network identifier.
///
/// ```sql
/// SELECT * FROM eth UNION ALL SELECT * FROM base
/// SETTINGS stream = true, lead_network = 'mainnet'
/// ```
pub fn lead_network_setting(stmt: &sql::parser::Statement) -> Option<NetworkId> {
    let sql::parser::Statement::Statement(box_stmt) = stmt else {
        return None;
    };
    let ast::Statement::Query(query) = box_stmt.as_ref() else {
        return None;
    };
    let settings = query.settings.as_ref()?;
    let setting = settings
        .iter()
        .find(|s| s.key.value.eq_ignore_ascii_case(LEAD_NETWORK_SETTING))?;
    let Expr::Value(v) = &setting.value else {
        return None;
    };
    match &v.value {
        ast::Value::SingleQuotedString(s) => s.parse().ok(),
        _ => None,
    }
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

    #[test]
    fn lead_network_with_valid_string_returns_some() {
        //* Given
        let stmt =
            parse_stmt("SELECT * FROM test SETTINGS stream = true, lead_network = 'mainnet'");

        //* When
        let result = lead_network_setting(&stmt);

        //* Then
        let expected: NetworkId = "mainnet"
            .parse()
            .expect("mainnet should be a valid NetworkId");
        assert_eq!(result, Some(expected));
    }

    #[test]
    fn lead_network_without_setting_returns_none() {
        //* Given
        let stmt = parse_stmt("SELECT * FROM test SETTINGS stream = true");

        //* When
        let result = lead_network_setting(&stmt);

        //* Then
        assert!(result.is_none(), "missing lead_network should return None");
    }

    #[test]
    fn lead_network_with_non_string_value_returns_none() {
        //* Given
        let stmt = parse_stmt("SELECT * FROM test SETTINGS lead_network = 42");

        //* When
        let result = lead_network_setting(&stmt);

        //* Then
        assert!(
            result.is_none(),
            "numeric lead_network value should return None"
        );
    }

    fn parse_stmt(sql: &str) -> sql::parser::Statement {
        let sql_str: SqlStr = sql.parse().expect("sql should be valid SqlStr");
        common::sql::parse(&sql_str).expect("sql should parse successfully")
    }
}
