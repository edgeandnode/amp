use datafusion::{
    arrow::datatypes::SchemaRef,
    logical_expr::{col, Expr},
};

use crate::BLOCK_NUM;

/// Identifies a dataset and its data schema.
#[derive(Clone, Debug)]
pub struct Dataset {
    pub name: String,
    pub network: String,
    pub tables: Vec<Table>,
}

impl Dataset {
    pub fn tables(&self) -> &[Table] {
        &self.tables
    }

    pub fn meta_tables(&self) -> Vec<Table> {
        crate::meta_tables::tables()
    }
}

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub struct Table {
    pub name: String,
    pub schema: SchemaRef,
}

impl Table {
    pub fn is_meta(&self) -> bool {
        self.name.starts_with("__")
    }

    pub fn order_exprs(&self) -> Vec<Vec<Expr>> {
        // Don't bother with order for meta tables.
        if self.is_meta() {
            return vec![];
        }

        // Leveraging `order_exprs` can optimize away sorting for many query plans.
        // TODO:
        // - Make this less hardcoded to handle non-blockchain data.
        // - Have a consistency check that the data really is sorted.
        // - Do we want to address and leverage https://github.com/apache/arrow-datafusion/issues/4177?
        vec![
            vec![col(BLOCK_NUM).sort(true, false)],
            vec![col("timestamp").sort(true, false)],
        ]
    }
}
