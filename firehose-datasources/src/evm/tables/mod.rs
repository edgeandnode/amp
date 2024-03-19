pub mod blocks;
pub mod calls;
pub mod logs;
pub mod transactions;

use common::{create_table_at, Table};
use datafusion::logical_expr::LogicalPlan;

pub fn all() -> Vec<Table> {
    vec![
        blocks::table(),
        transactions::table(),
        calls::table(),
        logs::table(),
    ]
}

pub fn create_at(namespace: String, location: String) -> Vec<LogicalPlan> {
    all()
        .into_iter()
        .map(|table| create_table_at(table, namespace.clone(), location.clone()))
        .collect()
}
