use std::sync::Arc;

use datafusion::{arrow::datatypes::DataType, logical_expr::ScalarUDF};
use datasets_common::dataset::Table;
use serde::Deserialize;

pub mod for_admin_api;
pub mod for_dump;
pub mod for_manifest_validation;
pub mod for_query;
pub mod table;

pub use table::LogicalTable;

#[derive(Debug, Clone, Deserialize)]
pub struct Function {
    pub name: String,

    // TODO: Support SQL type names, see https://datafusion.apache.org/user-guide/sql/data_types.html
    pub input_types: Vec<DataType>,
    pub output_type: DataType,
    pub source: FunctionSource,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FunctionSource {
    pub source: Arc<str>,
    pub filename: String,
}

#[derive(Clone, Debug)]
pub struct LogicalCatalog {
    pub tables: Vec<LogicalTable>,
    /// UDFs specific to the datasets corresponding to the resolved tables.
    pub udfs: Vec<ScalarUDF>,
}

impl LogicalCatalog {
    pub fn from_tables<'a>(tables: impl Iterator<Item = &'a LogicalTable>) -> Self {
        Self {
            tables: tables.cloned().collect(),
            udfs: Vec::new(),
        }
    }

    pub fn empty() -> Self {
        Self {
            tables: Vec::new(),
            udfs: Vec::new(),
        }
    }
}
