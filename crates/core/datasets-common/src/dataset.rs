//! Dataset trait and related types.
//!
//! # Downcasting
//!
//! The `Dataset` trait supports downcasting via the [`downcast_rs`] crate. This allows
//! converting `&dyn Dataset` back to a concrete type like `&solana::Dataset`.
//!
//! Available methods on `dyn Dataset`:
//! - `is::<T>()` - Check if the trait object wraps type `T`
//! - `downcast_ref::<T>()` - Get `Option<&T>`
//! - `downcast::<T>()` - Convert `Box<dyn Dataset>` to `Result<Box<T>, Box<dyn Dataset>>`
//! - `downcast_arc::<T>()` - Convert `Arc<dyn Dataset>` to `Result<Arc<T>, Arc<dyn Dataset>>`

use std::collections::BTreeSet;

use datafusion::arrow::datatypes::SchemaRef;
use downcast_rs::{DowncastSync, impl_downcast};

use crate::{
    dataset_kind_str::DatasetKindStr, hash_reference::HashReference, table_name::TableName,
};

pub const SPECIAL_BLOCK_NUM: &str = "_block_num";
pub type BlockNum = u64;

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
    fn tables(&self) -> &[Table];

    /// Returns the network this dataset is associated with, if applicable.
    ///
    /// For example, `"mainnet"`, `"sepolia"`, etc. Returns `None` for datasets
    /// that are not network-specific.
    fn network(&self) -> Option<&String>;

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

/// Represents a table definition within a dataset.
///
/// A table consists of a name, an Arrow schema defining its columns, the network
/// it belongs to, and metadata about its natural sort order. Tables are the primary
/// data containers within datasets and are used by the query engine for planning
/// and execution.
///
/// # Sort Order
///
/// Every table is naturally sorted by at least the `_block_num` column, which is
/// automatically added to the `sorted_by` set during construction. This ensures
/// consistent block-ordered data access across all tables.
#[derive(Clone, Hash, PartialEq, Eq, Debug, serde::Deserialize)]
pub struct Table {
    /// Bare table name (e.g., "blocks", "transactions").
    name: TableName,
    /// Arrow schema defining the table's columns and their data types.
    schema: SchemaRef,
    /// Network identifier (e.g., "mainnet", "sepolia").
    network: String,
    /// Column names by which this table is naturally sorted.
    sorted_by: BTreeSet<String>,
}

impl Table {
    /// Creates a new table definition. Automatically adds `_block_num` to `sorted_by`.
    pub fn new(
        name: TableName,
        schema: SchemaRef,
        network: String,
        sorted_by: Vec<String>,
    ) -> Self {
        let mut sorted_by: BTreeSet<String> = sorted_by.into_iter().collect();
        sorted_by.insert(SPECIAL_BLOCK_NUM.to_string());
        Self {
            name,
            schema,
            network,
            sorted_by,
        }
    }

    /// Returns the bare table name (without schema or dataset prefix).
    pub fn name(&self) -> &TableName {
        &self.name
    }

    /// Returns the Arrow schema defining this table's columns and types.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Returns the network this table is associated with (e.g., `"mainnet"`).
    pub fn network(&self) -> &str {
        &self.network
    }

    /// Returns column names by which this table is naturally sorted. Always includes `_block_num`.
    pub fn sorted_by(&self) -> &BTreeSet<String> {
        &self.sorted_by
    }
}
