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
    partial_reference::PartialReference,
    table_name::TableName,
};
use js_runtime::isolate_pool::IsolatePool;
use serde::Deserialize;

use crate::{BlockNum, BoxError, SPECIAL_BLOCK_NUM, js_udf::JsUdf, sql::TableReference};

/// Identifies a dataset and its data schema.
#[derive(Clone, Debug)]
pub struct Dataset {
    pub manifest_hash: datasets_common::hash::Hash,
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

    /// Resolved tables serve two purposes:
    /// 1. Associate a table with its dataset.
    /// 2. Associate the table with a `TableReference`
    ///    - If no reference is provided, the table reference will be a bare table name.
    ///
    /// TODO: Separate a mandatory full `Reference` from a `TableReference` alias.
    pub fn resolved_tables(
        self: &Arc<Self>,
        dataset_ref: PartialReference,
    ) -> impl Iterator<Item = ResolvedTable> + '_ {
        self.tables.iter().map(move |table| {
            let table_ref = TableReference::partial(dataset_ref.to_string(), table.name().clone());
            ResolvedTable::new(table_ref, table.clone(), self.clone())
        })
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

    pub fn manifest_hash(&self) -> &datasets_common::hash::Hash {
        &self.manifest_hash
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
    ) -> Result<Self, BoxError> {
        let mut sorted_by: BTreeSet<String> = sorted_by.into_iter().collect();
        sorted_by.insert(SPECIAL_BLOCK_NUM.to_string());
        Ok(Self {
            name,
            schema,
            network,
            sorted_by,
        })
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
    dataset: Arc<Dataset>,
    table_ref: TableReference,
}

impl fmt::Display for ResolvedTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.table_ref)
    }
}

impl ResolvedTable {
    pub fn new(table_ref: TableReference, table: Table, dataset: Arc<Dataset>) -> Self {
        Self {
            table,
            dataset,
            table_ref,
        }
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
    pub fn name(&self) -> &TableName {
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
