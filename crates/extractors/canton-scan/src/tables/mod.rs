//! Canton table definitions for AMP raw datasets.
//!
//! Defines Arrow schemas and row builders for Canton ledger data tables.
//! Table names and schemas match the CSV data samples from canton-network-validator.
//!
//! ## Tables
//!
//! - `transactions` - Transaction metadata (01_transactions.csv)
//! - `contracts_created` - Contract creation events (03_contracts_created.csv)
//! - `choices_exercised` - Choice exercise events (04_choices_exercised.csv)
//! - `mining_rounds` - Mining round lifecycle (05_mining_rounds.csv)

pub mod choices_exercised;
pub mod contracts_created;
pub mod mining_rounds;
pub mod transactions;

use datasets_common::network_id::NetworkId;

/// Returns all Canton tables for a given network.
pub fn all(network: &NetworkId) -> Vec<datasets_common::dataset::Table> {
    vec![
        transactions::table(network.clone()),
        contracts_created::table(network.clone()),
        choices_exercised::table(network.clone()),
        mining_rounds::table(network.clone()),
    ]
}
