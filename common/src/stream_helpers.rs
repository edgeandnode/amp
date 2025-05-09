use datafusion::logical_expr::sqlparser::ast::Value;
use datafusion::sql::parser::Statement;

pub fn cdc_pg_channel(dataset_name: &str) -> String {
    format!("cdc:{}", dataset_name)
}

// Returns true if the query is a streaming query, ending with "SETTINGS stream = true"
// E.g. "SELECT * FROM eth_firehose.blocks SETTINGS stream = true"
pub fn is_streaming(stmt: &Statement) -> bool {
    match stmt {
        Statement::Statement(box_stmt) => match box_stmt.as_ref() {
            datafusion::sql::sqlparser::ast::Statement::Query(box_query) => {
                match box_query.as_ref() {
                    datafusion::sql::sqlparser::ast::Query {
                        settings: Some(settings),
                        ..
                    } => settings
                        .iter()
                        .find(|&s| {
                            s.key.value.to_lowercase() == "stream"
                                && s.value == Value::Boolean(true)
                        })
                        .is_some(),
                    _ => false,
                }
            }
            _ => false,
        },
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_context::parse_sql;

    #[test]
    fn test_cdc_pg_channel() {
        let channel = cdc_pg_channel("test");
        assert_eq!(channel, "cdc:test");
    }

    #[test]
    fn test_is_streaming() {
        let queries = vec![
            ("SELECT * FROM test SETTINGS stream = true", true),
            ("SELECT * FROM test SETTINGS STREAM = True", true),
            ("SELECT * FROM test SETTINGS STREAM = 1", false),
            (
                "SELECT * FROM (select * from test) as t SETTINGS stream = true",
                true,
            ),
            ("SELECT * FROM test", false),
        ];
        for (sql, expected) in queries {
            let stmt = parse_sql(&sql).unwrap();
            assert_eq!(is_streaming(&stmt), expected);
        }
    }
}
