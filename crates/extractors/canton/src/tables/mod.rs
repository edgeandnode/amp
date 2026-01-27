//! Canton table definitions for AMP raw datasets.
//!
//! Uses schemas from canton-schema crate directly via typed-arrow.

pub mod contracts_archived;
pub mod contracts_created;
pub mod transactions;

use datasets_common::dataset::Table;

/// Returns all Canton tables for a given network.
pub fn all(network: &str) -> Vec<Table> {
    vec![
        transactions::table(network.to_string()),
        contracts_created::table(network.to_string()),
        contracts_archived::table(network.to_string()),
    ]
}
