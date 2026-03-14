use amp_providers_solana::tables::{
    block_headers, block_rewards, instructions, messages, transactions,
};
use datasets_common::network_id::NetworkId;
use datasets_raw::dataset::Table;

pub fn all(network: &NetworkId) -> Vec<Table> {
    vec![
        block_headers::table(network.clone()),
        block_rewards::table(network.clone()),
        transactions::table(network.clone()),
        messages::table(network.clone()),
        instructions::table(network.clone()),
    ]
}
