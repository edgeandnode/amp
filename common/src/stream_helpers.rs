use datafusion::{logical_expr::sqlparser::ast::Value, sql::parser::Statement};
use metadata_db::LocationId;

pub fn change_tracking_pg_channel(location_id: LocationId) -> String {
    format!("change-tracking-physical-table-id:{}", location_id)
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
        let channel = change_tracking_pg_channel(12345);
        assert_eq!(channel, "change-tracking-physical-table-id:12345");
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
