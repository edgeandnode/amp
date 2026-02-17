//! Canton contracts_created table definition.
//!
//! The contracts_created table contains contract creation events from the Canton ledger.
//! This corresponds to the `03_contracts_created.csv` data sample.
//!
//! CSV columns: contract_id, template_id, package_name, created_at, signatories, observers

use std::sync::{Arc, LazyLock};

use datasets_common::{
    block_num::RESERVED_BLOCK_NUM_COLUMN_NAME, block_range::BlockRange, dataset::Table,
    network_id::NetworkId,
};
use datasets_raw::{
    arrow::{ArrayRef, DataType, Field, Schema, SchemaRef, StringBuilder, UInt64Builder},
    rows::{TableRowError, TableRows},
};

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(schema()));

/// Table name for contracts_created
pub const TABLE_NAME: &str = "contracts_created";

/// Create the contracts_created table definition for a network.
pub fn table(network: NetworkId) -> Table {
    let name = TABLE_NAME.parse().expect("table name is valid");
    Table::new(
        name,
        SCHEMA.clone(),
        network,
        vec!["contract_id".to_string()],
    )
}

/// Arrow schema for the contracts_created table.
/// Matches CSV: contract_id, template_id, package_name, created_at, signatories, observers
fn schema() -> Schema {
    Schema::new(vec![
        // AMP partition key (required) - we use a hash of contract_id or sequence number
        Field::new(RESERVED_BLOCK_NUM_COLUMN_NAME, DataType::UInt64, false),
        // Contract ID (unique identifier for this contract instance)
        Field::new("contract_id", DataType::Utf8, false),
        // Full template_id string (package_id:Module.Name:EntityName)
        Field::new("template_id", DataType::Utf8, false),
        // Package name (e.g., "splice-amulet")
        Field::new("package_name", DataType::Utf8, false),
        // Creation timestamp
        Field::new("created_at", DataType::Utf8, false),
        // Parties who signed this contract (semicolon-separated in CSV)
        Field::new("signatories", DataType::Utf8, false),
        // Parties who can observe this contract (semicolon-separated in CSV)
        Field::new("observers", DataType::Utf8, true),
    ])
}

/// A contract creation event from the Canton ledger.
/// Corresponds to a row in 03_contracts_created.csv
#[derive(Debug, Default, Clone)]
pub struct ContractCreated {
    /// Block number (derived from offset or sequence)
    pub block_num: u64,
    /// Contract ID (unique identifier)
    pub contract_id: String,
    /// Full template_id (package_id:Module.Name:EntityName)
    pub template_id: String,
    /// Package name
    pub package_name: String,
    /// Creation timestamp
    pub created_at: String,
    /// Parties who signed this contract (semicolon-separated)
    pub signatories: String,
    /// Parties who can observe this contract (semicolon-separated)
    pub observers: Option<String>,
}

/// Builder for constructing Arrow arrays from ContractCreated records.
pub struct ContractCreatedRowsBuilder {
    block_num: UInt64Builder,
    contract_id: StringBuilder,
    template_id: StringBuilder,
    package_name: StringBuilder,
    created_at: StringBuilder,
    signatories: StringBuilder,
    observers: StringBuilder,
}

impl ContractCreatedRowsBuilder {
    /// Create a new builder with the given capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            block_num: UInt64Builder::with_capacity(capacity),
            contract_id: StringBuilder::with_capacity(capacity, capacity * 128),
            template_id: StringBuilder::with_capacity(capacity, capacity * 128),
            package_name: StringBuilder::with_capacity(capacity, capacity * 64),
            created_at: StringBuilder::with_capacity(capacity, capacity * 32),
            signatories: StringBuilder::with_capacity(capacity, capacity * 256),
            observers: StringBuilder::with_capacity(capacity, capacity * 256),
        }
    }

    /// Append a contract created record to the builder.
    pub fn append(&mut self, row: &ContractCreated) {
        self.block_num.append_value(row.block_num);
        self.contract_id.append_value(&row.contract_id);
        self.template_id.append_value(&row.template_id);
        self.package_name.append_value(&row.package_name);
        self.created_at.append_value(&row.created_at);
        self.signatories.append_value(&row.signatories);
        self.observers.append_option(row.observers.as_deref());
    }

    /// Build TableRows from the accumulated data.
    pub fn build(mut self, range: BlockRange) -> Result<TableRows, TableRowError> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.block_num.finish()),
            Arc::new(self.contract_id.finish()),
            Arc::new(self.template_id.finish()),
            Arc::new(self.package_name.finish()),
            Arc::new(self.created_at.finish()),
            Arc::new(self.signatories.finish()),
            Arc::new(self.observers.finish()),
        ];

        TableRows::new(table(range.network.clone()), range, columns)
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::B256;

    use super::*;

    #[test]
    fn default_to_arrow() {
        let contract = ContractCreated::default();
        let rows = {
            let mut builder = ContractCreatedRowsBuilder::with_capacity(1);
            builder.append(&contract);
            builder
                .build(BlockRange {
                    numbers: 0..=0,
                    network: "canton-test".parse().expect("valid network id"),
                    hash: B256::ZERO,
                    prev_hash: B256::ZERO,
                })
                .unwrap()
        };
        // 7 columns: _block_num, contract_id, template_id, package_name, created_at, signatories, observers
        assert_eq!(rows.rows.num_columns(), 7);
        assert_eq!(rows.rows.num_rows(), 1);
    }
}
