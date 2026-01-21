use std::{collections::BTreeMap, sync::Arc};

use common::Function;
use datafusion::logical_expr::{ScalarUDF, async_udf::AsyncScalarUDF};
use datasets_common::{
    dataset::{BlockNum, Table},
    deps::{alias::DepAlias, reference::DepReference},
    hash_reference::HashReference,
    raw_dataset_kind::RawDatasetKind,
    udf::{IsolatePool, JsUdf},
};

use crate::DerivedDatasetKind;

pub struct Dataset {
    pub(crate) tables: Vec<Table>,
    pub(crate) start_block: Option<BlockNum>,
    pub(crate) kind: DerivedDatasetKind,
    pub(crate) dependencies: BTreeMap<DepAlias, DepReference>,
    pub(crate) functions: Vec<Function>,
    pub(crate) reference: HashReference,
    pub(crate) network: Option<String>,
    pub(crate) finalized_blocks_only: bool,
}

impl datasets_common::dataset::Dataset for Dataset {
    fn tables(&self) -> &[Table] {
        &self.tables
    }

    fn start_block(&self) -> Option<BlockNum> {
        self.start_block
    }

    fn kind(&self) -> RawDatasetKind {
        self.kind.into()
    }

    fn dependencies(&self) -> &BTreeMap<DepAlias, DepReference> {
        &self.dependencies
    }

    fn reference(&self) -> &HashReference {
        &self.reference
    }

    fn network(&self) -> Option<&String> {
        self.network.as_ref()
    }

    fn finalized_blocks_only(&self) -> bool {
        self.finalized_blocks_only
    }

    fn as_dataset_with_functions(
        &self,
    ) -> Option<&dyn datasets_common::dataset::DatasetWithFunctions> {
        Some(self)
    }
}

impl datasets_common::dataset::DatasetWithFunctions for Dataset {
    fn function_by_name(
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
