pub mod calls;
pub mod transactions;

use common::Table;

pub fn all() -> Vec<Table> {
    vec![
        common::evm::tables::blocks::table(),
        transactions::table(),
        calls::table(),
        common::evm::tables::logs::table(),
    ]
}
