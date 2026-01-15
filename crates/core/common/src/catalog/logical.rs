use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
};

use datafusion::{
    arrow::datatypes::{DataType, SchemaRef},
    logical_expr::{ScalarUDF, async_udf::AsyncScalarUDF},
};
use datasets_common::{
    deps::{alias::DepAlias, reference::DepReference},
    hash_reference::HashReference,
    table_name::TableName,
};
use js_runtime::isolate_pool::IsolatePool;
use serde::Deserialize;

use crate::{BlockNum, SPECIAL_BLOCK_NUM, js_udf::JsUdf, sql::TableReference};

/// Identifies a dataset and its data schema.
#[derive(Clone, Debug)]
pub struct Dataset {
    pub reference: HashReference,
    pub dependencies: BTreeMap<DepAlias, DepReference>,
    pub kind: String,
    pub network: Option<String>,
    pub start_block: Option<BlockNum>,
    pub finalized_blocks_only: bool,
    pub tables: Vec<Table>,
    pub functions: Vec<Function>,
}

impl Dataset {
    pub fn tables(&self) -> &[Table] {
        &self.tables
    }

    /// Returns a specific JS function by name from this dataset.
    ///
    /// This implements lazy loading by only instantiating the requested function,
    /// rather than creating all functions in the dataset.
    pub fn function_by_name(
        &self,
        schema: String,
        name: &str,
        isolate_pool: IsolatePool,
    ) -> Option<ScalarUDF> {
        self.functions.iter().find(|f| f.name == name).map(|f| {
            AsyncScalarUDF::new(Arc::new(JsUdf::new(
                isolate_pool,
                schema,
                f.source.source.clone(),
                f.source.filename.clone().into(),
                f.name.clone().into(),
                f.input_types.clone(),
                f.output_type.clone(),
            )))
            .into_scalar_udf()
        })
    }

    pub fn reference(&self) -> &HashReference {
        &self.reference
    }
}

#[derive(Clone, Hash, PartialEq, Eq, Debug, Deserialize)]
pub struct Table {
    /// Bare table name.
    name: TableName,
    schema: SchemaRef,
    network: String,
    sorted_by: BTreeSet<String>,
}

impl Table {
    pub fn new(
        name: TableName,
        schema: SchemaRef,
        network: String,
        sorted_by: Vec<String>,
    ) -> Self {
        let mut sorted_by: BTreeSet<String> = sorted_by.into_iter().collect();
        sorted_by.insert(SPECIAL_BLOCK_NUM.to_string());
        Self {
            name,
            schema,
            network,
            sorted_by,
        }
    }

    pub fn name(&self) -> &TableName {
        &self.name
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn network(&self) -> &str {
        &self.network
    }

    /// Column names by which this table is naturally sorted.
    pub fn sorted_by(&self) -> &BTreeSet<String> {
        &self.sorted_by
    }
}

/// A table that holds a reference to its dataset.
#[derive(Debug, Clone)]
pub struct ResolvedTable {
    table: Table,
    /// The dataset reference portion of SQL table references.
    ///
    /// SQL table references have the format `<dataset_ref>.<table>` (e.g., `anvil_rpc.blocks`).
    /// This field stores the string form of the `<dataset_ref>` portion - the schema under
    /// which this table is registered in the catalog and referenced in SQL queries.
    sql_table_ref_schema: String,
    dataset_reference: HashReference,
    dataset_start_block: Option<BlockNum>,
}

impl fmt::Display for ResolvedTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.table_ref())
    }
}

impl ResolvedTable {
    pub fn new(
        table: Table,
        sql_table_ref_schema: String,
        dataset_reference: HashReference,
        dataset_start_block: Option<BlockNum>,
    ) -> Self {
        Self {
            table,
            sql_table_ref_schema,
            dataset_reference,
            dataset_start_block,
        }
    }

    pub fn table(&self) -> &Table {
        &self.table
    }

    pub fn table_ref(&self) -> TableReference {
        TableReference::partial(self.sql_table_ref_schema.clone(), self.table.name.clone())
    }

    pub fn dataset_reference(&self) -> &HashReference {
        &self.dataset_reference
    }

    pub fn dataset_start_block(&self) -> Option<BlockNum> {
        self.dataset_start_block
    }

    /// Bare table name
    pub fn name(&self) -> &TableName {
        &self.table.name
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
    pub tables: Vec<ResolvedTable>,
    /// UDFs specific to the datasets corresponding to the resolved tables.
    pub udfs: Vec<ScalarUDF>,
}

impl LogicalCatalog {
    pub fn from_tables<'a>(tables: impl Iterator<Item = &'a ResolvedTable>) -> Self {
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
