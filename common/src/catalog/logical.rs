use datafusion::arrow::datatypes::SchemaRef;
use serde::Serialize;

use crate::BLOCK_NUM;

/// Identifies a dataset and its data schema.
#[derive(Clone, Debug, Serialize)]
pub struct Dataset {
    pub kind: String,
    pub name: String,
    pub tables: Vec<Table>,
}

#[derive(Clone, Debug)]
pub struct DatasetWithProvider {
    pub dataset: Dataset,
    pub provider: Option<String>,
}

impl Dataset {
    pub fn tables(&self) -> &[Table] {
        &self.tables
    }
}

#[derive(Clone, Hash, PartialEq, Eq, Debug, Serialize)]
pub struct Table {
    /// Bare table name.
    pub name: String,
    pub schema: SchemaRef,
    pub network: Option<String>,
}

impl Table {
    /// Column names by which this table is naturally sorted.
    pub fn sorted_by(&self) -> Vec<String> {
        // Leveraging `order_exprs` can optimize away sorting for many query plans.
        //
        // TODO:
        // - Make this less hardcoded to handle non-blockchain data.
        // - Have a consistency check that the data really is sorted.
        // - Do we want to address and leverage https://github.com/apache/arrow-datafusion/issues/4177?
        vec![BLOCK_NUM.to_string(), "timestamp".to_string()]
    }
}
