//! Keeps track of block ranges that have already been scanned. One use of this is an optimization to
//! skip scanned ranges when resuming a write process.
//!
//! These ranges could not be perfectly inferred from tables themselves, because tables can be sparse
//! and not have data for all block numbers. In which case we don't know if a block was never
//! scanned, or if it was scanned and is empty.
//!
//! # Consistency
//! Because there isn't an atomic way of writing multiple files to object storage, we need to be
//! mindful of consistency. The non-atomic write process is:
//! ```ignore
//! write_real_data();
//! write_scanned_ranges();
//! ```
//! Creating the possibility of orphaned data files if the process is interrupted between the two
//! writes or if the first operation succeeds and the second one errors. An orphaned file means a
//! data file for which `__scanned_ranges` does not contain a corresponding range. One way we ensure
//! consistency is by detecting and deleting orphaned files when starting up a write process.
//!
//! See also: scanned-ranges-consistency

use std::{
    collections::BTreeMap,
    sync::{Arc, LazyLock},
};

use crate::{
    arrow::array::{ArrayRef, StringBuilder},
    multirange::MultiRange,
    query_context::Error as CoreError,
    timestamp_type, BoxError, QueryContext, Timestamp, TimestampArrayBuilder,
};
use datafusion::{
    arrow::{
        array::{ArrayBuilder as _, AsArray as _, RecordBatch, UInt64Builder},
        datatypes::{DataType, Field, Schema, SchemaRef, UInt64Type},
    },
    sql::TableReference,
};

use crate::Table;

pub const TABLE_NAME: &'static str = "__scanned_ranges";

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(schema()));

pub fn table() -> Table {
    Table {
        name: TABLE_NAME.to_string(),
        schema: SCHEMA.clone(),
        network: None,
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

pub async fn ranges_for_table(
    ctx: &QueryContext,
    catalog_schema: &str,
    table_name: &str,
) -> Result<Vec<(u64, u64)>, CoreError> {
    let scanned_ranges_ref = TableReference::partial(catalog_schema, TABLE_NAME);
    let rb = ctx
            .meta_execute_sql(&format!(
                "select range_start, range_end from {scanned_ranges_ref} where table = '{table_name}' order by range_start, range_end",
            ))
            .await?;
    let start_blocks: &[u64] = rb.column(0).as_primitive::<UInt64Type>().values().as_ref();
    let end_blocks: &[u64] = rb.column(1).as_primitive::<UInt64Type>().values().as_ref();

    let ranges = start_blocks.iter().zip(end_blocks).map(|(s, e)| (*s, *e));
    Ok(ranges.collect())
}

pub async fn scanned_ranges_by_table(
    ctx: &QueryContext,
) -> Result<BTreeMap<String, MultiRange>, BoxError> {
    let mut multirange_by_table = BTreeMap::default();

    for table in ctx.catalog().all_tables() {
        let table_name = table.table_name().to_string();
        let ranges = ranges_for_table(ctx, table.catalog_schema(), &table_name).await?;
        let multi_range = MultiRange::from_ranges(ranges)?;
        multirange_by_table.insert(table_name, multi_range);
    }

    Ok(multirange_by_table)
}

pub async fn filenames_for_table(
    ctx: &QueryContext,
    catalog_schema: &str,
    table_name: &str,
) -> Result<Vec<String>, CoreError> {
    let scanned_ranges_ref = TableReference::partial(catalog_schema, TABLE_NAME);
    let rb = ctx
            .meta_execute_sql(&format!(
                "select filename from {scanned_ranges_ref} where table = '{table_name}' order by range_start, range_end",
            ))
            .await?;
    let filenames = rb.column(0).as_string::<i32>().iter().map(|s| s.unwrap());
    Ok(filenames.map(|s| s.to_string()).collect())
}

pub struct ScannedRange {
    pub table: String,
    pub range_start: u64,
    pub range_end: u64,
    pub filename: String,
    pub created_at: Timestamp,
}

impl ScannedRange {
    pub fn as_record_batch(&self) -> RecordBatch {
        let mut builder = ScannedRangeRowsBuilder::new();
        builder.append(self);
        builder.build()
    }
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

    pub fn build(&mut self) -> RecordBatch {
        let columns = vec![
            Arc::new(self.table.finish()) as ArrayRef,
            Arc::new(self.range_start.finish()),
            Arc::new(self.range_end.finish()),
            Arc::new(self.filename.finish()),
            Arc::new(self.timestamp.finish()),
        ];

        // Unwrap: The columns follow the schema and have an equal length.
        RecordBatch::try_new(SCHEMA.clone(), columns).unwrap()
    }

    pub fn len(&self) -> usize {
        self.table.len()
    }
}
