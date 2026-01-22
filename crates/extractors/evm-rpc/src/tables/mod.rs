pub mod transactions;

use datasets_raw::evm::tables::{blocks, logs};

pub fn all(network: &str) -> Vec<datasets_common::dataset::Table> {
    vec![
        blocks::table(network.to_string()),
        transactions::table(network.to_string()),
        logs::table(network.to_string()),
    ]
}
