//! Keeps track of block ranges that have already been scanned. This is an optimization to skip
//! scanned ranges when resuming a write process.
//!
//! This cannot be inferred from tables themselves, because tables can be sparse and not have data
//! for all block numbers. In which case we don't know if a block was never scanned, or if it was
//! scanned and is empty.
//!
//! It is ok for this to contain false negatives, that is, to not contain a range that was in fact
//! scanned. As the `existing_blocks` check is what ensures no data is written twice. This
//! is important as it lets us get away with this optimization even without an atomic transaction
//! system. For example in:
//! ```ignore
//! write_real_data();
//! write_scanned_ranges();
//! ```
//! If the process is interrupted between the two writes, that will create a false negative.

use std::sync::Arc;

use crate::{
    arrow::array::{ArrayRef, StringBuilder},
    timestamp_type, Timestamp, TimestampArrayBuilder,
};
use datafusion::arrow::{
    array::{ArrayBuilder as _, RecordBatch, UInt64Builder},
    datatypes::{DataType, Field, Schema, SchemaRef},
    error::ArrowError,
};

use crate::Table;

pub const TABLE_NAME: &'static str = "__scanned_ranges";

lazy_static::lazy_static! {
    static ref SCHEMA: SchemaRef = Arc::new(schema());
}

pub fn table() -> Table {
    Table {
        name: TABLE_NAME.to_string(),
        schema: SCHEMA.clone(),
    }
}

fn schema() -> Schema {
    let table = Field::new("table", DataType::Utf8, false);
    let start = Field::new("range_start", DataType::UInt64, false);
    let end = Field::new("range_end", DataType::UInt64, false);
    let filename = Field::new("filename", DataType::Utf8, false);
    let created_at = Field::new("created_at", timestamp_type(), false);

    let fields = vec![table, start, end, filename, created_at];
    Schema::new(fields)
}

pub struct ScannedRange {
    pub table: String,
    pub range_start: u64,
    pub range_end: u64,
    pub filename: String,
    pub created_at: Timestamp,
}

#[derive(Debug)]
pub struct ScannedRangeRowsBuilder {
    table: StringBuilder,
    range_start: UInt64Builder,
    range_end: UInt64Builder,
    filename: StringBuilder,
    timestamp: TimestampArrayBuilder,
}

impl ScannedRangeRowsBuilder {
    pub fn new() -> Self {
        Self {
            table: StringBuilder::new(),
            range_start: UInt64Builder::new(),
            range_end: UInt64Builder::new(),
            filename: StringBuilder::new(),
            timestamp: TimestampArrayBuilder::with_capacity(0),
        }
    }

    pub fn append(&mut self, range: &ScannedRange) {
        self.table.append_value(&range.table);
        self.range_start.append_value(range.range_start);
        self.range_end.append_value(range.range_end);
        self.filename.append_value(&range.filename);
        self.timestamp.append_value(range.created_at);
    }

    pub fn flush(&mut self) -> Result<RecordBatch, ArrowError> {
        let columns = vec![
            Arc::new(self.table.finish()) as ArrayRef,
            Arc::new(self.range_start.finish()),
            Arc::new(self.range_end.finish()),
            Arc::new(self.filename.finish()),
            Arc::new(self.timestamp.finish()),
        ];

        RecordBatch::try_new(SCHEMA.clone(), columns)
    }

    pub fn len(&self) -> usize {
        self.table.len()
    }
}
