//! Canton choices_exercised table definition.
//!
//! The choices_exercised table contains choice exercise events from the Canton ledger.
//! This corresponds to the `04_choices_exercised.csv` data sample.
//!
//! CSV columns: event_id, contract_id, template_id, choice, consuming, acting_parties, effective_at

use std::sync::{Arc, LazyLock};

use datasets_common::{
    block_num::RESERVED_BLOCK_NUM_COLUMN_NAME, block_range::BlockRange, dataset::Table,
    network_id::NetworkId,
};
use datasets_raw::{
    arrow::{
        ArrayRef, BooleanBuilder, DataType, Field, Schema, SchemaRef, StringBuilder, UInt64Builder,
    },
    rows::{TableRowError, TableRows},
};

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(schema()));

/// Table name for choices_exercised
pub const TABLE_NAME: &str = "choices_exercised";

/// Create the choices_exercised table definition for a network.
pub fn table(network: NetworkId) -> Table {
    let name = TABLE_NAME.parse().expect("table name is valid");
    Table::new(
        name,
        SCHEMA.clone(),
        network,
        vec!["event_id".to_string(), "contract_id".to_string()],
    )
}

/// Arrow schema for the choices_exercised table.
/// Matches CSV: event_id, contract_id, template_id, choice, consuming, acting_parties, effective_at
fn schema() -> Schema {
    Schema::new(vec![
        // AMP partition key (required)
        Field::new(RESERVED_BLOCK_NUM_COLUMN_NAME, DataType::UInt64, false),
        // Event ID (unique identifier for this exercise event)
        Field::new("event_id", DataType::Utf8, false),
        // Contract ID being exercised
        Field::new("contract_id", DataType::Utf8, false),
        // Full template_id (package_id:Module.Name:EntityName)
        Field::new("template_id", DataType::Utf8, false),
        // Choice name being exercised
        Field::new("choice", DataType::Utf8, false),
        // Whether this exercise consumes (archives) the contract
        Field::new("consuming", DataType::Boolean, false),
        // Acting parties who exercised this choice (semicolon-separated in CSV)
        Field::new("acting_parties", DataType::Utf8, false),
        // Effective timestamp (ledger time when choice took effect)
        Field::new("effective_at", DataType::Utf8, false),
    ])
}

/// A choice exercise event from the Canton ledger.
/// Corresponds to a row in 04_choices_exercised.csv
#[derive(Debug, Default, Clone)]
pub struct ChoiceExercised {
    /// Block number (derived from offset or sequence)
    pub block_num: u64,
    /// Event ID (unique identifier)
    pub event_id: String,
    /// Contract ID being exercised
    pub contract_id: String,
    /// Full template_id (package_id:Module.Name:EntityName)
    pub template_id: String,
    /// Choice name being exercised
    pub choice: String,
    /// Whether this exercise consumes (archives) the contract
    pub consuming: bool,
    /// Acting parties who exercised this choice (semicolon-separated)
    pub acting_parties: String,
    /// Effective timestamp
    pub effective_at: String,
}

/// Builder for constructing Arrow arrays from ChoiceExercised records.
pub struct ChoiceExercisedRowsBuilder {
    block_num: UInt64Builder,
    event_id: StringBuilder,
    contract_id: StringBuilder,
    template_id: StringBuilder,
    choice: StringBuilder,
    consuming: BooleanBuilder,
    acting_parties: StringBuilder,
    effective_at: StringBuilder,
}

impl ChoiceExercisedRowsBuilder {
    /// Create a new builder with the given capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            block_num: UInt64Builder::with_capacity(capacity),
            event_id: StringBuilder::with_capacity(capacity, capacity * 64),
            contract_id: StringBuilder::with_capacity(capacity, capacity * 128),
            template_id: StringBuilder::with_capacity(capacity, capacity * 128),
            choice: StringBuilder::with_capacity(capacity, capacity * 64),
            consuming: BooleanBuilder::with_capacity(capacity),
            acting_parties: StringBuilder::with_capacity(capacity, capacity * 256),
            effective_at: StringBuilder::with_capacity(capacity, capacity * 32),
        }
    }

    /// Append a choice exercised record to the builder.
    pub fn append(&mut self, row: &ChoiceExercised) {
        self.block_num.append_value(row.block_num);
        self.event_id.append_value(&row.event_id);
        self.contract_id.append_value(&row.contract_id);
        self.template_id.append_value(&row.template_id);
        self.choice.append_value(&row.choice);
        self.consuming.append_value(row.consuming);
        self.acting_parties.append_value(&row.acting_parties);
        self.effective_at.append_value(&row.effective_at);
    }

    /// Build TableRows from the accumulated data.
    pub fn build(mut self, range: BlockRange) -> Result<TableRows, TableRowError> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.block_num.finish()),
            Arc::new(self.event_id.finish()),
            Arc::new(self.contract_id.finish()),
            Arc::new(self.template_id.finish()),
            Arc::new(self.choice.finish()),
            Arc::new(self.consuming.finish()),
            Arc::new(self.acting_parties.finish()),
            Arc::new(self.effective_at.finish()),
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
        let choice = ChoiceExercised::default();
        let rows = {
            let mut builder = ChoiceExercisedRowsBuilder::with_capacity(1);
            builder.append(&choice);
            builder
                .build(BlockRange {
                    numbers: 0..=0,
                    network: "canton-test".parse().expect("valid network id"),
                    hash: B256::ZERO,
                    prev_hash: B256::ZERO,
                })
                .unwrap()
        };
        // 8 columns: _block_num, event_id, contract_id, template_id, choice, consuming, acting_parties, effective_at
        assert_eq!(rows.rows.num_columns(), 8);
        assert_eq!(rows.rows.num_rows(), 1);
    }
}
