use std::collections::{BTreeMap, BTreeSet};

use datafusion::{arrow::datatypes::SchemaRef, logical_expr::ScalarUDF};
use js_runtime::isolate_pool::IsolatePool;

use crate::{
    dataset_kind_str::DatasetKindStr,
    deps::{alias::DepAlias, reference::DepReference},
    hash_reference::HashReference,
    table_name::TableName,
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
pub trait Dataset: Sync + Send + 'static {
    /// Returns the tables defined in this dataset.
    fn tables(&self) -> &[Table];

    /// Returns the starting block number for this dataset, if specified.
    ///
    /// When `Some`, extraction should begin from this block. When `None`, the default
    /// starting block for the network is used.
    fn start_block(&self) -> Option<BlockNum>;

    /// Returns the kind of this dataset (e.g., `evm-rpc`, `solana`, `derived`).
    fn kind(&self) -> DatasetKindStr;

    /// Returns the dependencies of this dataset on other datasets.
    ///
    /// The map keys are aliases used to reference dependencies in SQL queries,
    /// and values are references to the dependent datasets.
    fn dependencies(&self) -> &BTreeMap<DepAlias, DepReference>;

    /// Returns the hash reference uniquely identifying this dataset version.
    fn reference(&self) -> &HashReference;

    /// Returns the network this dataset is associated with, if applicable.
    ///
    /// For example, `"mainnet"`, `"sepolia"`, etc. Returns `None` for datasets
    /// that are not network-specific.
    fn network(&self) -> Option<&String>;

    /// Returns whether this dataset should only process finalized blocks.
    ///
    /// When `true`, extraction will wait for blocks to be finalized before processing.
    fn finalized_blocks_only(&self) -> bool;

    /// Returns this dataset as a [`DatasetWithFunctions`] if it supports user-defined functions.
    ///
    /// Only derived datasets support user-defined functions. Raw extractor datasets
    /// (e.g., evm-rpc, firehose) return `None`.
    fn as_dataset_with_functions(&self) -> Option<&dyn DatasetWithFunctions>;
}

pub trait DatasetWithFunctions: Dataset {
    /// Looks up a user-defined function by name.
    ///
    /// Returns the [`ScalarUDF`] for the function if found. This is primarily used
    /// by derived datasets that define custom JavaScript functions.
    ///
    /// Returns `None` for datasets that don't support user-defined functions
    /// or if the function name is not found.
    fn function_by_name(
        &self,
        schema: String,
        name: &str,
        isolate_pool: IsolatePool,
    ) -> Option<ScalarUDF>;
}

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
    /// Creates a new table definition.
    ///
    /// # Arguments
    ///
    /// * `name` - The bare table name (e.g., "blocks", "transactions")
    /// * `schema` - The Arrow schema defining the table's columns and types
    /// * `network` - The network identifier (e.g., "mainnet", "sepolia")
    /// * `sorted_by` - Column names by which the table data is naturally sorted
    ///
    /// # Note
    ///
    /// The special `_block_num` column is automatically added to `sorted_by`
    /// to ensure all tables maintain block-level ordering.
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

    /// Returns the bare table name.
    ///
    /// This is the unqualified name without any schema or dataset prefix.
    pub fn name(&self) -> &TableName {
        &self.name
    }

    /// Returns the Arrow schema defining this table's structure.
    ///
    /// The schema includes all column definitions with their data types
    /// and nullability constraints.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Returns the network this table is associated with.
    ///
    /// Examples: `"mainnet"`, `"sepolia"`, `"solana-mainnet"`.
    pub fn network(&self) -> &str {
        &self.network
    }

    /// Returns the column names by which this table is naturally sorted.
    ///
    /// This information is used by the query optimizer to avoid unnecessary
    /// sort operations when the requested order matches the natural order.
    /// Always includes `_block_num`.
    pub fn sorted_by(&self) -> &BTreeSet<String> {
        &self.sorted_by
    }
}
