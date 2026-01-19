pub mod transactions;

pub fn all(network: &str) -> Vec<datasets_common::dataset::Table> {
    vec![
        common::evm::tables::blocks::table(network.to_string()),
        transactions::table(network.to_string()),
        common::evm::tables::logs::table(network.to_string()),
    ]
}
