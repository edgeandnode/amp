//! Concrete raw dataset type.

use datasets_common::{
    block_num::BlockNum, dataset::Table, dataset_kind_str::DatasetKindStr,
    hash_reference::HashReference, network_id::NetworkId,
};

/// A [`Dataset`](datasets_common::dataset::Dataset) backed by a raw blockchain data source.
///
/// # Invariants
///
/// - The dataset contains at least one table.
/// - All tables belong to the same network.
pub struct Dataset {
    network: NetworkId,
    tables: Vec<Table>,
    start_block: Option<BlockNum>,
    kind: DatasetKindStr,
    reference: HashReference,
    finalized_blocks_only: bool,
}

impl Dataset {
    /// Creates a new raw dataset.
    ///
    /// # Panics
    ///
    /// Panics if `tables` is empty or if any table's network differs from `network`.
    pub fn new(
        reference: HashReference,
        kind: DatasetKindStr,
        network: NetworkId,
        tables: Vec<Table>,
        start_block: Option<BlockNum>,
        finalized_blocks_only: bool,
    ) -> Self {
        assert!(
            !tables.is_empty(),
            "raw dataset must have at least one table"
        );
        assert!(
            tables.iter().all(|t| t.network() == &network),
            "all tables must belong to the same network as the dataset"
        );
        Self {
            reference,
            kind,
            network,
            tables,
            start_block,
            finalized_blocks_only,
        }
    }

    /// Returns the network this dataset belongs to.
    pub fn network(&self) -> &NetworkId {
        &self.network
    }
}

impl datasets_common::dataset::Dataset for Dataset {
    fn reference(&self) -> &HashReference {
        &self.reference
    }

    fn kind(&self) -> DatasetKindStr {
        self.kind.clone()
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
