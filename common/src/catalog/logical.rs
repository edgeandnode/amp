use std::sync::Arc;

use async_udf::functions::AsyncScalarUDF;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use js_runtime::isolate_pool::IsolatePool;
use serde::Serialize;

use crate::{js_udf::JsUdf, BLOCK_NUM};

/// Identifies a dataset and its data schema.
#[derive(Clone, Debug, Serialize)]
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
                f.output_type.clone(),
            )))
        })
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

#[derive(Debug, Clone, Serialize)]
pub struct Function {
    pub name: String,

    // TODO: Support SQL type names, see https://datafusion.apache.org/user-guide/sql/data_types.html
    pub input_types: Vec<DataType>,
    pub output_type: DataType,
    pub source: FunctionSource,
}

#[derive(Debug, Clone, Serialize)]
pub struct FunctionSource {
    pub source: Arc<str>,
    pub filename: String,
}
