pub mod calls;
pub mod transactions;

use datasets_common::{dataset::Table, network_id::NetworkId};
use datasets_raw::evm::tables::{blocks, logs};

pub fn all(network: &NetworkId) -> Vec<Table> {
    vec![
        blocks::table(network.clone()),
        transactions::table(network.clone()),
        calls::table(network.clone()),
        logs::table(network.clone()),
    ]
}
