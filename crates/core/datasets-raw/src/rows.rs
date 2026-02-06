//! Raw table row types for dataset extraction.

use arrow::{
    array::{ArrayRef, AsArray as _, RecordBatch},
    datatypes::UInt64Type,
};
use datasets_common::{
    block_num::{BlockNum, RESERVED_BLOCK_NUM_COLUMN_NAME},
    block_range::BlockRange,
    dataset::Table,
};

pub struct Rows(Vec<TableRows>);

impl Rows {
    pub fn new(rows: Vec<TableRows>) -> Self {
        assert!(!rows.is_empty());
        assert!(rows.iter().skip(1).all(|r| r.range == rows[0].range));
        Self(rows)
    }

    pub fn block_num(&self) -> BlockNum {
        self.0[0].block_num()
    }
}

impl IntoIterator for Rows {
    type Item = TableRows;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// A record batch associated with a single block of chain data, for populating raw datasets.
pub struct TableRows {
    pub table: Table,
    pub rows: RecordBatch,
    pub range: BlockRange,
}

impl TableRows {
    pub fn new(
        table: Table,
        range: BlockRange,
        columns: Vec<ArrayRef>,
    ) -> Result<Self, TableRowError> {
        let table_name = table.name().to_string();
        let schema = table.schema().clone();
        let rows = RecordBatch::try_new(schema, columns)?;
        Self::check_invariants(&range, &rows).map_err(|source| TableRowError::Invariants {
            table: table_name,
            source,
        })?;
        Ok(TableRows { table, rows, range })
    }

    pub fn block_num(&self) -> BlockNum {
        self.range.start()
    }

    fn check_invariants(
        range: &BlockRange,
        rows: &RecordBatch,
    ) -> Result<(), CheckInvariantsError> {
        if range.start() != range.end() {
            return Err(CheckInvariantsError::InvalidBlockRange);
        }
        if rows.num_rows() == 0 {
            return Ok(());
        }

        let block_nums = rows
            .column_by_name(RESERVED_BLOCK_NUM_COLUMN_NAME)
            .ok_or(CheckInvariantsError::MissingBlockNumColumn)?;
        let block_nums = block_nums
            .as_primitive_opt::<UInt64Type>()
            .ok_or(CheckInvariantsError::InvalidBlockNumColumnType)?;

        // Unwrap: `rows` is not empty.
        let start = arrow::compute::kernels::aggregate::min(block_nums).unwrap();
        let end = arrow::compute::kernels::aggregate::max(block_nums).unwrap();
        if start != range.start() {
            return Err(CheckInvariantsError::UnexpectedBlockNum(start));
        };
        if end != range.start() {
            return Err(CheckInvariantsError::UnexpectedBlockNum(end));
        };

        Ok(())
    }
}

/// Errors that occur when validating table row invariants.
///
/// These errors represent violations of structural requirements for raw dataset tables,
/// such as block range consistency and required column presence/types.
#[derive(Debug, thiserror::Error)]
pub enum CheckInvariantsError {
    /// Block range does not contain exactly one block number
    ///
    /// This occurs when the block range start and end differ, violating the requirement
    /// that TableRows must represent data from a single block.
    ///
    /// Raw dataset tables are organized by individual blocks, and each TableRows instance
    /// must contain data from exactly one block number.
    #[error("block range must contain a single block number")]
    InvalidBlockRange,

    /// Required `_block_num` column is missing from the record batch
    ///
    /// This occurs when the Arrow RecordBatch does not contain the special `_block_num`
    /// column that tracks which block each row belongs to.
    ///
    /// All raw dataset tables require the `_block_num` column for block-level partitioning
    /// and filtering operations.
    #[error("missing _block_num column")]
    MissingBlockNumColumn,

    /// The `_block_num` column has incorrect data type
    ///
    /// This occurs when the `_block_num` column exists but is not of type UInt64.
    ///
    /// The `_block_num` column must be UInt64 to properly represent blockchain block numbers.
    #[error("_block_num column is not uint64")]
    InvalidBlockNumColumnType,

    /// Row contains block number that doesn't match the expected range
    ///
    /// This occurs when one or more rows have a `_block_num` value that differs from
    /// the block number specified in the range. All rows must have the same block number
    /// matching the range's single block.
    ///
    /// This check validates data consistency between the block range metadata and the
    /// actual block numbers in the row data.
    #[error("contains unexpected block_num: {0}")]
    UnexpectedBlockNum(BlockNum),
}

/// Errors that occur when creating TableRows instances.
///
/// This error type is used by `TableRows::new()` and represents failures during
/// record batch construction and validation.
#[derive(Debug, thiserror::Error)]
pub enum TableRowError {
    /// Failed to construct Arrow RecordBatch from columns
    ///
    /// This occurs when Arrow cannot create a valid RecordBatch from the provided
    /// columns and schema. The underlying Arrow error provides specific details.
    ///
    /// Common causes:
    /// - Column count doesn't match schema field count
    /// - Column types don't match schema types
    /// - Column lengths are inconsistent
    /// - Invalid array data or corrupted memory buffers
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),

    /// Table rows violate structural invariants
    ///
    /// This occurs when the constructed RecordBatch violates requirements for raw
    /// dataset tables, such as missing required columns, incorrect block numbers,
    /// or invalid block ranges.
    ///
    /// The source error provides specific details about which invariant was violated.
    #[error("malformed table {table}: {source}")]
    Invariants {
        table: String,
        #[source]
        source: CheckInvariantsError,
    },
}
