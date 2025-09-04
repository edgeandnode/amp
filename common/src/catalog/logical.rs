use std::{fmt, sync::Arc};

use datafusion::{
    arrow::datatypes::{DataType, SchemaRef},
    logical_expr::{ScalarUDF, async_udf::AsyncScalarUDF},
    sql::TableReference,
};
use js_runtime::isolate_pool::IsolatePool;
use serde::Deserialize;

use crate::{BLOCK_NUM, BlockNum, BoxError, SPECIAL_BLOCK_NUM, js_udf::JsUdf, manifest::Version};

/// Identifies a dataset and its data schema.
#[derive(Clone, Debug, Deserialize)]
pub struct Dataset {
    pub kind: String,
    pub network: String,
    pub name: String,
    pub version: Option<Version>,
    pub start_block: Option<BlockNum>,
    pub tables: Vec<Table>,
    pub functions: Vec<Function>,
}

impl Dataset {
    pub fn tables(&self) -> &[Table] {
        &self.tables
    }

    pub fn resolved_tables(self: &Arc<Self>) -> impl Iterator<Item = ResolvedTable> + '_ {
        self.tables
            .iter()
            .map(move |table| ResolvedTable::new(table.clone(), self.clone()).unwrap())
    }

    /// Returns the JS functions defined in this dataset.
    ///
    /// JS functions need a V8 isolate pool order to be executed.
    pub fn functions(
        &self,
        isolate_pool: IsolatePool,
    ) -> impl Iterator<Item = AsyncScalarUDF> + '_ {
        self.functions.iter().map(move |f| {
            AsyncScalarUDF::new(Arc::new(JsUdf::new(
                isolate_pool.clone(),
                &self.name,
                f.source.source.clone(),
                f.source.filename.clone().into(),
                f.name.clone().into(),
                f.input_types.clone(),
                f.output_type.clone(),
            )))
        })
    }

    pub fn dataset_version(&self) -> Option<String> {
        self.version.as_ref().map(|v| v.0.to_string())
    }

    pub fn to_identifier(&self) -> String {
        match self.kind.as_str() {
            "manifest" => format!(
                "{}__{}",
                self.name,
                self.version
                    .as_ref()
                    .map(|v| v.to_underscore_version())
                    .unwrap_or_default()
            ),
            _ => self.name.clone(),
        }
    }
}

#[derive(Clone, Hash, PartialEq, Eq, Debug, Deserialize)]
pub struct Table {
    /// Bare table name.
    name: String,
    schema: SchemaRef,
    network: String,
}

impl Table {
    pub fn new(name: String, schema: SchemaRef, network: String) -> Self {
        Self {
            name,
            schema,
            network,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn network(&self) -> &str {
        &self.network
    }

    /// Column names by which this table is naturally sorted.
    pub const fn sorted_by(&self) -> &[&str] {
        // Leveraging `order_exprs` can optimize away sorting for many query plans.
        //
        // TODO:
        // - Make this less hardcoded to handle non-blockchain data.
        // - Have a consistency check that the data really is sorted.
        // - Do we want to address and leverage https://github.com/apache/arrow-datafusion/issues/4177?
        &[SPECIAL_BLOCK_NUM, BLOCK_NUM, "timestamp"]
    }
}

/// A table that holds a reference to its dataset.
#[derive(Debug, Clone)]
pub struct ResolvedTable {
    table: Table,
    dataset: Arc<Dataset>,
    table_ref: TableReference,
}

impl fmt::Display for ResolvedTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.table_ref)
    }
}

impl ResolvedTable {
    /// Errors if the table name is invalid.
    pub fn new(table: Table, dataset: Arc<Dataset>) -> Result<Self, BoxError> {
        validate_name(table.name())?;

        let table_ref = TableReference::partial(dataset.name.clone(), table.name().to_string());
        Ok(Self {
            table,
            dataset,
            table_ref,
        })
    }

    pub fn table(&self) -> &Table {
        &self.table
    }

    pub fn dataset(&self) -> &Arc<Dataset> {
        &self.dataset
    }

    pub fn dataset_version(&self) -> Option<String> {
        self.dataset.dataset_version()
    }

    pub fn table_ref(&self) -> &TableReference {
        &self.table_ref
    }

    pub fn update_table_ref(&mut self, table_ref: TableReference) -> &mut Self {
        self.table_ref = table_ref;
        self
    }

    /// Bare table name
    pub fn name(&self) -> &str {
        &self.table.name
    }

    pub fn catalog_schema(&self) -> &str {
        // Unwrap: This is always constructed with a schema.
        self.table_ref.schema().unwrap()
    }

    pub fn network(&self) -> &str {
        &self.table.network
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.table.schema
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

fn validate_name(name: &str) -> Result<(), BoxError> {
    if let Some(c) = name
        .chars()
        .find(|&c| !(c.is_ascii_lowercase() || c == '_' || c.is_numeric()))
    {
        return Err(format!(
            "names must be lowercase and contain only letters, underscores, and numbers, \
             the name: '{name}' is not allowed because it contains the character '{c}'"
        )
        .into());
    }

    Ok(())
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
