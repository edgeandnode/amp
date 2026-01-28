pub mod transactions;

use datasets_common::network_id::NetworkId;
use datasets_raw::evm::tables::{blocks, logs};

pub fn all(network: &NetworkId) -> Vec<datasets_common::dataset::Table> {
    vec![
        blocks::table(network.clone()),
        transactions::table(network.clone()),
        logs::table(network.clone()),
    ]
}
