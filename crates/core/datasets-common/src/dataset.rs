//! Dataset trait and related types.
//!
//! # Downcasting
//!
//! Both [`Dataset`] and [`Table`] support downcasting via the [`downcast_rs`] crate.
//! This allows converting trait objects back to concrete types.
//!
//! Available methods on `dyn Dataset` and `dyn Table`:
//! - `is::<T>()` - Check if the trait object wraps type `T`
//! - `downcast_ref::<T>()` - Get `Option<&T>`
//! - `downcast::<T>()` - Convert `Box<dyn _>` to `Result<Box<T>, Box<dyn _>>`
//! - `downcast_arc::<T>()` - Convert `Arc<dyn _>` to `Result<Arc<T>, Arc<dyn _>>`

use std::{collections::BTreeSet, sync::Arc};

use datafusion::arrow::datatypes::SchemaRef;
use downcast_rs::{DowncastSync, impl_downcast};

use crate::{
    block_num::BlockNum, dataset_kind_str::DatasetKindStr, hash_reference::HashReference,
    network_id::NetworkId, table_name::TableName,
};

/// Core trait representing a dataset definition.
///
/// A dataset is a collection of tables with associated metadata, including its kind
/// (e.g., `evm-rpc`, `solana`, `derived`), dependencies on other datasets, and optional
/// user-defined functions. This trait provides a unified interface for accessing dataset
/// properties across different dataset types.
///
/// Implementations of this trait are created by extractor crates (e.g., `evm-rpc-datasets`,
/// `solana-datasets`) and the `datasets-derived` crate for derived datasets.
///
/// This trait extends [`DowncastSync`] to enable downcasting `dyn Dataset` to concrete types.
pub trait Dataset: DowncastSync {
    /// Returns the hash reference uniquely identifying this dataset version.
    fn reference(&self) -> &HashReference;

    /// Returns the kind of this dataset (e.g., `evm-rpc`, `solana`, `derived`).
    fn kind(&self) -> DatasetKindStr;

    /// Returns the tables defined in this dataset.
    fn tables(&self) -> &[Arc<dyn Table>];

    /// Returns the names of all tables in this dataset.
    fn table_names(&self) -> Vec<TableName>;

    /// Finds a table by name, returning a reference to its `Arc`.
    fn get_table(&self, name: &TableName) -> Option<&Arc<dyn Table>>;

    /// Returns `true` if the dataset contains a table with the given name.
    fn has_table(&self, name: &TableName) -> bool;

    /// Returns the starting block number for this dataset, if specified.
    ///
    /// When `Some`, extraction should begin from this block. When `None`, the default
    /// starting block for the network is used.
    fn start_block(&self) -> Option<BlockNum>;

    /// Returns whether this dataset should only process finalized blocks.
    ///
    /// When `true`, extraction will wait for blocks to be finalized before processing.
    fn finalized_blocks_only(&self) -> bool;
}

// Implement downcasting for `Dataset`.
impl_downcast!(sync Dataset);

/// Trait representing a table definition within a dataset.
///
/// A table consists of a name, an Arrow schema defining its columns, an optional
/// network association, and metadata about its natural sort order. Tables are the
/// primary data containers within datasets and are used by the query engine for
/// planning and execution.
///
/// Concrete implementations exist in `datasets-raw` (with network) and
/// `datasets-derived` (without network).
///
/// This trait extends [`DowncastSync`] to enable downcasting `dyn Table` to concrete types.
pub trait Table: DowncastSync + std::fmt::Debug {
    /// Returns the bare table name (without schema or dataset prefix).
    fn name(&self) -> &TableName;

    /// Returns the Arrow schema defining this table's columns and types.
    fn schema(&self) -> &SchemaRef;

    /// Returns the network this table is associated with, if any.
    ///
    /// Raw tables always have a network; derived tables return `None`.
    fn network(&self) -> Option<&NetworkId> {
        None
    }

    /// Returns column names by which this table is naturally sorted. Always includes `_block_num`.
    fn sorted_by(&self) -> &BTreeSet<String>;
}

// Implement downcasting for `Table`.
impl_downcast!(sync Table);
