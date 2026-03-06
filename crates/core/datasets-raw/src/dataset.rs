//! Concrete raw dataset and table types.

use std::{collections::BTreeSet, sync::Arc};

use arrow::datatypes::SchemaRef;
use datasets_common::{
    block_num::{BlockNum, RESERVED_BLOCK_NUM_COLUMN_NAME},
    dataset::Table as TableTrait,
    dataset_kind_str::DatasetKindStr,
    hash_reference::HashReference,
    network_id::NetworkId,
    table_name::TableName,
};

/// A [`Dataset`](datasets_common::dataset::Dataset) backed by a raw blockchain data source.
///
/// # Invariants
///
/// - The dataset contains at least one table.
/// - All tables belong to the same network.
pub struct Dataset {
    network: NetworkId,
    tables: Vec<Arc<dyn TableTrait>>,
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
            tables.iter().all(|t| t.network_ref() == &network),
            "all tables must belong to the same network as the dataset"
        );

        let tables: Vec<Arc<dyn TableTrait>> = tables
            .into_iter()
            .map(|t| Arc::new(t) as Arc<dyn TableTrait>)
            .collect();
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

    fn tables(&self) -> &[Arc<dyn TableTrait>] {
        &self.tables
    }

    fn table_names(&self) -> Vec<TableName> {
        self.tables.iter().map(|t| t.name().clone()).collect()
    }

    fn get_table(&self, name: &TableName) -> Option<&Arc<dyn TableTrait>> {
        self.tables.iter().find(|t| t.name() == name)
    }

    fn has_table(&self, name: &TableName) -> bool {
        self.tables.iter().any(|t| t.name() == name)
    }

    fn start_block(&self) -> Option<BlockNum> {
        self.start_block
    }

    fn finalized_blocks_only(&self) -> bool {
        self.finalized_blocks_only
    }
}

/// A table definition for a raw dataset, always associated with a network.
#[derive(Clone, Debug)]
pub struct Table {
    name: TableName,
    schema: SchemaRef,
    network: NetworkId,
    sorted_by: BTreeSet<String>,
}

impl Table {
    /// Creates a new raw table definition. Automatically adds `_block_num` to `sorted_by`.
    pub fn new(
        name: TableName,
        schema: SchemaRef,
        network: NetworkId,
        sorted_by: Vec<String>,
    ) -> Self {
        let mut sorted_by: BTreeSet<String> = sorted_by.into_iter().collect();
        sorted_by.insert(RESERVED_BLOCK_NUM_COLUMN_NAME.to_string());
        Self {
            name,
            schema,
            network,
            sorted_by,
        }
    }

    /// Returns the network this raw table belongs to.
    pub fn network_ref(&self) -> &NetworkId {
        &self.network
    }
}

impl TableTrait for Table {
    fn name(&self) -> &TableName {
        &self.name
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn network(&self) -> Option<&NetworkId> {
        Some(&self.network)
    }

    fn sorted_by(&self) -> &BTreeSet<String> {
        &self.sorted_by
    }
}
