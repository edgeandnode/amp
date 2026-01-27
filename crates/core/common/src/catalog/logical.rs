use datafusion::logical_expr::ScalarUDF;

pub mod for_admin_api;
pub mod for_dump;
pub mod for_manifest_validation;
pub mod for_query;
pub mod table;

pub use table::LogicalTable;

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
