use std::{fmt, sync::Arc};

use async_udf::functions::AsyncScalarUDF;
use datafusion::{
    arrow::datatypes::{DataType, Schema, SchemaRef},
    logical_expr::ScalarUDF,
    sql::TableReference,
};
use js_runtime::isolate_pool::IsolatePool;

use crate::{js_udf::JsUdf, BoxError, SPECIAL_BLOCK_NUM};

/// Identifies a dataset and its data schema.
#[derive(Clone, Debug)]
pub struct Dataset {
    pub kind: String,
    pub name: String,
    pub tables: Vec<Table>,
    pub functions: Vec<Function>,
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
}

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub struct Table {
    /// Bare table name.
    pub name: String,
    pub schema: SchemaRef,
    pub network: String,
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
        vec![SPECIAL_BLOCK_NUM.to_string(), "timestamp".to_string()]
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
        validate_name(&table.name)?;

        let table_ref = TableReference::partial(dataset.name.clone(), table.name.clone());
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

    pub fn table_ref(&self) -> &TableReference {
        &self.table_ref
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

    pub fn with_schema(self, schema: Schema) -> Self {
        Self {
            table: Table {
                name: self.table.name,
                schema: schema.into(),
                network: self.table.network,
            },
            dataset: self.dataset,
            table_ref: self.table_ref,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Function {
    pub name: String,

    // TODO: Support SQL type names, see https://datafusion.apache.org/user-guide/sql/data_types.html
    pub input_types: Vec<DataType>,
    pub output_type: DataType,
    pub source: FunctionSource,
}

#[derive(Debug, Clone)]
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
