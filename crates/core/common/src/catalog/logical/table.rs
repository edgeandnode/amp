use std::fmt;

use datasets_common::{dataset::Table, hash_reference::HashReference, table_name::TableName};

use crate::sql::TableReference;

/// A table that holds a reference to its dataset.
#[derive(Debug, Clone)]
pub struct LogicalTable {
    /// The dataset reference portion of SQL table references.
    ///
    /// SQL table references have the format `<dataset_ref>.<table>` (e.g., `anvil_rpc.blocks`).
    /// This field stores the string form of the `<dataset_ref>` portion - the schema under
    /// which this table is registered in the catalog and referenced in SQL queries.
    sql_table_ref_schema: String,
    dataset_reference: HashReference,
    table: Table,
}

impl LogicalTable {
    pub fn new(
        sql_table_ref_schema: String,
        dataset_reference: HashReference,
        table: Table,
    ) -> Self {
        Self {
            table,
            sql_table_ref_schema,
            dataset_reference,
        }
    }

    pub fn table(&self) -> &Table {
        &self.table
    }

    pub fn table_ref(&self) -> TableReference {
        TableReference::partial(self.sql_table_ref_schema.clone(), self.table.name().clone())
    }

    pub fn dataset_reference(&self) -> &HashReference {
        &self.dataset_reference
    }

    /// Bare table name
    pub fn name(&self) -> &TableName {
        self.table.name()
    }

    /// Returns the dataset reference portion of SQL table references.
    ///
    /// SQL table references have the format `<dataset_ref>.<table>` (e.g., `anvil_rpc.blocks`).
    /// This returns the string form of the `<dataset_ref>` portion - the schema under which
    /// this table is registered in the catalog and referenced in SQL queries.
    pub fn sql_table_ref_schema(&self) -> &str {
        &self.sql_table_ref_schema
    }
}

impl fmt::Display for LogicalTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.table_ref())
    }
}
