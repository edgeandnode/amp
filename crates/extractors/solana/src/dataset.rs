use datasets_common::{
    dataset::{BlockNum, Table},
    dataset_kind_str::DatasetKindStr,
    hash_reference::HashReference,
};

use crate::SolanaDatasetKind;

pub struct Dataset {
    pub(crate) tables: Vec<Table>,
    pub(crate) start_block: Option<BlockNum>,
    pub(crate) kind: SolanaDatasetKind,
    pub(crate) reference: HashReference,
    pub(crate) network: Option<String>,
    pub(crate) finalized_blocks_only: bool,
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

    fn network(&self) -> Option<&String> {
        self.network.as_ref()
    }

    fn start_block(&self) -> Option<BlockNum> {
        self.start_block
    }

    fn finalized_blocks_only(&self) -> bool {
        self.finalized_blocks_only
    }
}
