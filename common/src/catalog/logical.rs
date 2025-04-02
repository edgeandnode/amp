use datafusion::arrow::datatypes::SchemaRef;

use crate::BLOCK_NUM;

/// Identifies a dataset and its data schema.
#[derive(Clone, Debug)]
pub struct Dataset {
    pub kind: String,
    pub name: String,
    pub tables: Vec<Table>,
}

impl Dataset {
    pub fn tables(&self) -> &[Table] {
        &self.tables
    }
}

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub struct Table {
    /// Bare table name.
    pub name: String,
    pub schema: SchemaRef,
    pub network: Option<String>,
}

impl Table {
    pub fn is_meta(&self) -> bool {
        self.name.starts_with("__")
    }

    /// Column names by which this table is naturally sorted.
    pub fn sorted_by(&self) -> Vec<String> {
        // Don't bother with order for meta tables.
        if self.is_meta() {
            return vec![];
        }

        // Leveraging `order_exprs` can optimize away sorting for many query plans.
        //
        // TODO:
        // - Make this less hardcoded to handle non-blockchain data.
        // - Have a consistency check that the data really is sorted.
        // - Do we want to address and leverage https://github.com/apache/arrow-datafusion/issues/4177?
        vec![BLOCK_NUM.to_string(), "timestamp".to_string()]
    }
}
