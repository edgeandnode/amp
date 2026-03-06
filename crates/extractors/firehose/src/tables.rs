use amp_providers_firehose::tables::{calls, transactions};
use datasets_common::network_id::NetworkId;
use datasets_raw::{
    dataset::Table,
    evm::tables::{blocks, logs},
};

pub fn all(network: &NetworkId) -> Vec<Table> {
    vec![
        blocks::table(network.clone()),
        transactions::table(network.clone()),
        calls::table(network.clone()),
        logs::table(network.clone()),
    ]
}
