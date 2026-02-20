use datasets_common::{
    block_num::BlockNum, dataset::Table as DatasetTable, dataset_kind_str::DatasetKindStr,
    hash_reference::HashReference,
};

use crate::dataset_kind::FirehoseDatasetKind;

pub struct Dataset {
    pub(crate) tables: Vec<DatasetTable>,
    pub(crate) start_block: Option<BlockNum>,
    pub(crate) kind: FirehoseDatasetKind,
    pub(crate) reference: HashReference,
    pub(crate) finalized_blocks_only: bool,
}

impl datasets_common::dataset::Dataset for Dataset {
    fn reference(&self) -> &HashReference {
        &self.reference
    }

    fn kind(&self) -> DatasetKindStr {
        self.kind.into()
    }

    fn tables(&self) -> &[DatasetTable] {
        &self.tables
    }

    fn start_block(&self) -> Option<BlockNum> {
        self.start_block
    }

    fn finalized_blocks_only(&self) -> bool {
        self.finalized_blocks_only
    }
}
