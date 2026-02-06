use std::{collections::BTreeMap, sync::Arc};

use datafusion::logical_expr::{ScalarUDF, async_udf::AsyncScalarUDF};
use datasets_common::{
    block_num::BlockNum, dataset::Table, dataset_kind_str::DatasetKindStr,
    hash_reference::HashReference,
};
use js_runtime::{isolate_pool::IsolatePool, js_udf::JsUdf};

use crate::{
    DerivedDatasetKind,
    deps::{DepAlias, DepReference},
    function::Function,
};

pub struct Dataset {
    pub(crate) tables: Vec<Table>,
    pub(crate) start_block: Option<BlockNum>,
    pub(crate) kind: DerivedDatasetKind,
    pub(crate) dependencies: BTreeMap<DepAlias, DepReference>,
    pub(crate) functions: Vec<Function>,
    pub(crate) reference: HashReference,
    pub(crate) finalized_blocks_only: bool,
}

impl Dataset {
    /// Creates a new Dataset instance.
    ///
    /// This is used by the `common::datasets_derived::dataset()` function to construct
    /// the Dataset after processing the manifest.
    pub fn new(
        reference: HashReference,
        dependencies: BTreeMap<DepAlias, DepReference>,
        kind: DerivedDatasetKind,
        start_block: Option<BlockNum>,
        finalized_blocks_only: bool,
        tables: Vec<Table>,
        functions: Vec<Function>,
    ) -> Self {
        Self {
            reference,
            dependencies,
            kind,
            start_block,
            finalized_blocks_only,
            tables,
            functions,
        }
    }

    /// Returns the dependencies of this derived dataset.
    ///
    /// The map keys are aliases used to reference dependencies in SQL queries,
    /// and values are references to the dependent datasets.
    pub fn dependencies(&self) -> &BTreeMap<DepAlias, DepReference> {
        &self.dependencies
    }

    /// Looks up a user-defined function by name.
    ///
    /// Returns the [`ScalarUDF`] for the function if found. This is used
    /// for derived datasets that define custom JavaScript functions.
    ///
    /// Returns `None` if the function name is not found.
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
}

impl datasets_common::dataset::Dataset for Dataset {
    fn reference(&self) -> &HashReference {
        &self.reference
    }

    fn kind(&self) -> DatasetKindStr {
        self.kind.into()
    }

    fn tables(&self) -> &[Table] {
        &self.tables
    }

    fn start_block(&self) -> Option<BlockNum> {
        self.start_block
    }

    fn finalized_blocks_only(&self) -> bool {
        self.finalized_blocks_only
    }
}
