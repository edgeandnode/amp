pub mod calls;
pub mod transactions;

use datasets_common::dataset::Table;

pub fn all(network: &str) -> Vec<Table> {
    vec![
        common::evm::tables::blocks::table(network.to_string()),
        transactions::table(network.to_string()),
        calls::table(network.to_string()),
        common::evm::tables::logs::table(network.to_string()),
    ]
}
