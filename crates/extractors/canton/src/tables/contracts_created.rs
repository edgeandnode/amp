//! Canton contracts_created table definition.
//!
//! Uses ContractCreatedRecord schema from canton-schema directly (includes _block_num).

use canton_schema::ContractCreatedRecord;
use datasets_common::dataset::Table;
use typed_arrow::schema::SchemaMeta;

pub const TABLE_NAME: &str = "contracts_created";

pub fn table(network: String) -> Table {
    let name = TABLE_NAME.parse().expect("table name is valid");
    Table::new(
        name,
        ContractCreatedRecord::schema(),
        network,
        vec!["offset".to_string()],
    )
}
