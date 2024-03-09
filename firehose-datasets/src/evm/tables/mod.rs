mod block_header;
mod call;
mod log;
mod transaction;

pub use block_header::BlockHeader;
pub use call::Call;
use common::{create_table_at, Table};
use datafusion::logical_expr::LogicalPlan;
pub use log::Log;
pub use transaction::Transaction;

fn all_tables() -> Vec<Table> {
    vec![
        block_header::table(),
        transaction::table(),
        call::table(),
        log::table(),
    ]
}

pub fn create_evm_tables_at(namespace: String, location: String) -> Vec<LogicalPlan> {
    all_tables()
        .into_iter()
        .map(|table| create_table_at(table, namespace.clone(), location.clone()))
        .collect()
}
