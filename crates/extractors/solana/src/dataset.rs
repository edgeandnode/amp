use std::collections::BTreeMap;

use datasets_common::{
    dataset::{BlockNum, Table},
    deps::{alias::DepAlias, reference::DepReference},
    hash_reference::HashReference,
    raw_dataset_kind::RawDatasetKind,
};

use crate::SolanaDatasetKind;

pub struct Dataset {
    pub(crate) tables: Vec<Table>,
    pub(crate) start_block: Option<BlockNum>,
    pub(crate) kind: SolanaDatasetKind,
    pub(crate) dependencies: BTreeMap<DepAlias, DepReference>,
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
        None
    }
}
