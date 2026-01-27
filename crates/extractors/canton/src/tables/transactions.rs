//! Canton transactions table definition.
//!
//! Uses TransactionRecord schema from canton-schema directly (includes _block_num).

use canton_schema::TransactionRecord;
use datasets_common::dataset::Table;
use typed_arrow::schema::SchemaMeta;

pub const TABLE_NAME: &str = "transactions";

pub fn table(network: String) -> Table {
    let name = TABLE_NAME.parse().expect("table name is valid");
    Table::new(
        name,
        TransactionRecord::schema(),
        network,
        vec!["offset".to_string()],
    )
}
