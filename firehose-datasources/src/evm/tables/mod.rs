pub mod blocks;
pub mod calls;
pub mod logs;
pub mod transactions;

use common::Table;

pub fn all() -> Vec<Table> {
    vec![
        blocks::table(),
        transactions::table(),
        calls::table(),
        logs::table(),
    ]
}
