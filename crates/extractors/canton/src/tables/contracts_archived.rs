//! Canton contracts_archived table definition.
//!
//! Uses ContractArchivedRecord schema from canton-schema directly (includes _block_num).

use canton_schema::ContractArchivedRecord;
use datasets_common::dataset::Table;
use typed_arrow::schema::SchemaMeta;

pub const TABLE_NAME: &str = "contracts_archived";

pub fn table(network: String) -> Table {
    let name = TABLE_NAME.parse().expect("table name is valid");
    Table::new(
        name,
        ContractArchivedRecord::schema(),
        network,
        vec!["offset".to_string()],
    )
}
