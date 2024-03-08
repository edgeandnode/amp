mod block_header;
mod call;
mod log;
mod transaction_trace;

pub use block_header::BlockHeader;
pub use call::Call;
use common::create_table_at;
use datafusion::logical_expr::LogicalPlan;
pub use log::Log;
pub use transaction_trace::TransactionTrace;

pub fn create_evm_tables_at(location: String) -> Vec<LogicalPlan> {
    vec![
        create_table_at::<BlockHeader>(location.clone()),
        create_table_at::<TransactionTrace>(location.clone()),
        create_table_at::<Call>(location.clone()),
        create_table_at::<Log>(location),
    ]
}
