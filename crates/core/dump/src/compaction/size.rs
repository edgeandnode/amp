use std::{
    array::TryFromSliceError,
    fmt::{Display, Formatter},
    num::NonZeroI64,
    ops::{Add, AddAssign},
};

use common::{BLOCK_NUM, SPECIAL_BLOCK_NUM, config::CompactionConfig};
use datafusion::parquet::{
    arrow::arrow_reader::ArrowReaderMetadata, file::metadata::RowGroupMetaData,
};

/// Represents the size of a Segment (one or more parquet files) in four dimensions: blocks, bytes, rows, and count.
///
/// This struct is used to track file sizes for compaction operations, allowing the
/// system to make decisions based on different metrics. The four dimensions are:
///
/// - **blocks**: Number of distinct blockchain blocks contained in the file
/// - **bytes**: Raw data size in bytes (excluding metadata, indexes, etc.)
/// - **rows**: Total number of rows across all row groups
/// - **count**: Total number of files in the segment
///
/// # Examples
///
/// ```
/// # use dump::compaction::size::SegmentSize;
/// // Create a SegmentSize representing a file with 1000 blocks, 1MB of data, and 5000 rows
/// let size = SegmentSize {
///     length: 1,
///     blocks: 1000,
///     bytes: 1_048_576,
///     rows: 5000,
/// };
///
/// // SegmentSize can be copied and compared
/// let size2 = size;
/// assert_eq!(size, size2);
///
/// // Addition using the + operator
/// let size1 = SegmentSize {
///     length: 1,
///     blocks: 100,
///     bytes: 1000,
///     rows: 500,
/// };
/// let size2 = SegmentSize {
///     length: 1,
///     blocks: 50,
///     bytes: 500,
///     rows: 250,
/// };
///
/// let total = size1 + size2;
/// assert_eq!(total.length, 2);
/// assert_eq!(total.blocks, 150);
/// assert_eq!(total.bytes, 1500);
/// assert_eq!(total.rows, 750);
///
/// // Addition assignment using += operator
/// let mut accumulator = SegmentSize {
///     length: 1,
///     blocks: 100,
///     bytes: 1000,
///     rows: 500,
/// };
///
/// accumulator += SegmentSize {
///     length: 1,
///     blocks: 50,
///     bytes: 500,
///     rows: 250,
/// };
///
/// assert_eq!(accumulator.blocks, 150);
/// assert_eq!(accumulator.bytes, 1500);
/// assert_eq!(accumulator.rows, 750);
///
/// // Collecting from an iterator
/// let sizes = vec![
///     SegmentSize {
///         length: 1,
///         blocks: 10,
///         bytes: 100,
///         rows: 50,
///     },
///     SegmentSize {
///         length: 1,
///         blocks: 20,
///         bytes: 200,
///         rows: 100,
///     },
///     SegmentSize {
///         length: 2,
///         blocks: 30,
///         bytes: 300,
///         rows: 150,
///     },
/// ];
///
/// let total: SegmentSize = sizes.into_iter().collect();
/// assert_eq!(total.length, 4);
/// assert_eq!(total.blocks, 60);
/// assert_eq!(total.bytes, 600);
/// assert_eq!(total.rows, 300);
///
/// // Collecting with filter
/// let file_sizes = vec![
///     SegmentSize {
///         length: 1,
///         blocks: 100,
///         bytes: 1000,
///         rows: 500,
///     },
///     SegmentSize {
///         length: 1,
///         blocks: 50,
///         bytes: 500,
///         rows: 250,
///     },
///     SegmentSize {
///         length: 1,
///         blocks: 200,
///         bytes: 2000,
///         rows: 1000,
///     },
///     SegmentSize {
///         length: 1,
///         blocks: 25,
///         bytes: 250,
///         rows: 125,
///     },
/// ];
///
/// // Sum only files with more than 400 rows
/// let large_files_total: SegmentSize = file_sizes
///     .into_iter()
///     .filter(|size| size.rows > 400)
///     .collect();
///
/// assert_eq!(large_files_total.length, 2); // 1 + 1
/// assert_eq!(large_files_total.blocks, 300); // 100 + 200
/// assert_eq!(large_files_total.bytes, 3000); // 1000 + 2000
/// assert_eq!(large_files_total.rows, 1500); // 500 + 1000
/// ```
#[derive(Debug, Default, Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
pub struct SegmentSize {
    /// Total number of files in the segment
    pub length: usize,
    /// Number of distinct blocks in the file
    pub blocks: i64,
    /// Size of the data in the file in bytes (does not include metadata, indexes, etc.)
    pub bytes: i64,
    /// Number of rows in the file
    pub rows: i64,
}

impl Add for SegmentSize {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            blocks: self.blocks + other.blocks,
            bytes: self.bytes + other.bytes,
            rows: self.rows + other.rows,
            length: self.length + other.length,
        }
    }
}

impl std::ops::Mul<i32> for SegmentSize {
    type Output = Self;

    fn mul(self, rhs: i32) -> Self::Output {
        Self {
            blocks: self.blocks * rhs as i64,
            bytes: self.bytes * rhs as i64,
            rows: self.rows * rhs as i64,
            length: self.length,
        }
    }
}

impl FromIterator<SegmentSize> for SegmentSize {
    fn from_iter<T: IntoIterator<Item = Self>>(iter: T) -> Self {
        iter.into_iter()
            .fold(Self::default(), |acc, size| acc + size)
    }
}

impl AddAssign for SegmentSize {
    fn add_assign(&mut self, other: Self) {
        self.blocks += other.blocks;
        self.bytes += other.bytes;
        self.rows += other.rows;
        self.length += other.length;
    }
}

impl<'a> From<&'a ArrowReaderMetadata> for SegmentSize {
    fn from(value: &'a ArrowReaderMetadata) -> Self {
        let rows = value.metadata().file_metadata().num_rows();
        let mut pmax = 0;

        let (bytes, blocks) = value
            .metadata()
            .row_groups()
            .iter()
            .map(|rg| {
                let bytes = rg.total_byte_size();
                let blocks = get_block_count(rg, &mut pmax);
                (bytes, blocks)
            })
            .reduce(|(acc_bytes, acc_blocks), (bytes, blocks)| {
                (acc_bytes + bytes, acc_blocks + blocks)
            })
            .unwrap_or_default();

        SegmentSize {
            length: 1,
            blocks,
            bytes,
            rows,
        }
    }
}

impl Display for SegmentSize {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let mut size_string = (0..4)
            .filter_map(|i| match i {
                0 if self.length != 0 => Some(format!("length: {}, ", self.length)),
                1 if self.blocks.is_positive() => Some(format!("blocks: {}, ", self.blocks)),
                2 if self.bytes.is_positive() => Some(format!("bytes: {}, ", self.bytes)),
                3 if self.rows.is_positive() => Some(format!("rows: {}", self.rows)),
                _ => None,
            })
            .collect::<String>();

        if size_string.ends_with(", ") {
            size_string.truncate(size_string.len() - 2);
        } else if size_string.is_empty() {
            size_string.push_str("null");
        }

        write!(f, "{{ {} }}", size_string)
    }
}

impl SegmentSize {
    /// For Tracing
    pub fn value(&self) -> String {
        format!("{self}")
    }
}

/// Counts the number of distinct blocks in a Parquet row group while avoiding double-counting.
///
/// This function extracts block statistics from a row group's metadata and adjusts the count
/// to prevent blocks from being counted multiple times across row group boundaries.
///
/// # Arguments
///
/// * `rg` - The row group metadata containing column statistics
/// * `pmax` - A mutable reference to the previous maximum block number seen. This is used
///           to detect when consecutive row groups share a boundary block that shouldn't
///           be counted twice.
///
/// # Returns
///
/// The number of distinct blocks in this row group, adjusted for boundary overlaps.
/// Returns 0 if:
/// - No block number column is found
/// - Statistics are missing or invalid
/// - Block numbers are zero (invalid block numbers)
///
/// # Algorithm
///
/// 1. Finds the block number column (either BLOCK_NUM or SPECIAL_BLOCK_NUM)
/// 2. Extracts min/max block numbers and distinct count from column statistics
/// 3. If the minimum block equals the previous maximum, decrements the count by 1
///    (this block was already counted in the previous row group)
/// 4. Updates `pmax` to the current maximum for the next iteration
///
/// # Examples
///
/// ```ignore
/// // Scenario: Two consecutive row groups with overlapping block ranges
/// // Row Group 1: blocks [1000, 1001, 1002] (min=1000, max=1002, distinct=3)
/// // Row Group 2: blocks [1002, 1003, 1004] (min=1002, max=1004, distinct=3)
///
/// let mut pmax = 0;
///
/// // First row group: returns 3, sets pmax to 1002
/// let count1 = get_block_count(&rg1, &mut pmax);
/// assert_eq!(count1, 3);
/// assert_eq!(pmax, 1002);
///
/// // Second row group: returns 2 (3 - 1), sets pmax to 1004
/// // Block 1002 is not double-counted
/// let count2 = get_block_count(&rg2, &mut pmax);
/// assert_eq!(count2, 2);
/// assert_eq!(pmax, 1004);
///
/// // Total distinct blocks: 3 + 2 = 5 (not 6)
/// ```
fn get_block_count(rg: &RowGroupMetaData, pmax: &mut i64) -> i64 {
    if let Some(column) = rg
        .columns()
        .iter()
        .filter(|c| {
            let name = c.column_descr().name();
            name == BLOCK_NUM || name == SPECIAL_BLOCK_NUM
        })
        .next()
        && let Some(statistics) = column.statistics()
        && let Some(Ok(Some(max))) = statistics.max_bytes_opt().map(le_bytes_to_nonzero_i64_opt)
        && let Some(Ok(Some(min))) = statistics.min_bytes_opt().map(le_bytes_to_nonzero_i64_opt)
        && let Some(mut blocks) = statistics.distinct_count_opt()
    {
        let min = min.get();
        let max = max.get();

        // Check if this row group's minimum block was already counted
        // in the previous row group (boundary overlap). Subtract 1 if so.
        if min == *pmax {
            blocks = blocks.saturating_sub(1);
        }

        *pmax = max;

        blocks as i64
    } else {
        0
    }
}

/// Converts a little-endian byte slice to an optional NonZeroI64.
///
/// This function is used to parse block number statistics from Parquet metadata,
/// where block numbers are stored as little-endian i64 values. The function:
///
/// 1. Attempts to convert the byte slice to exactly 8 bytes
/// 2. Interprets those bytes as a little-endian i64
/// 3. Wraps the result in NonZeroI64, returning None if the value is zero
///
/// # Arguments
///
/// * `bytes` - A byte slice that should contain exactly 8 bytes representing an i64 in little-endian format
///
/// # Returns
///
/// * `Ok(Some(NonZeroI64))` - Successfully parsed a non-zero value
/// * `Ok(None)` - Successfully parsed but the value was zero
/// * `Err(TryFromSliceError)` - The byte slice was not exactly 8 bytes long
///
/// # Examples
///
/// ```
/// # use dump::compaction::size::le_bytes_to_nonzero_i64_opt;
///
/// // Valid non-zero value (42 in little-endian)
/// let bytes = [42, 0, 0, 0, 0, 0, 0, 0];
/// let result = le_bytes_to_nonzero_i64_opt(&bytes).unwrap();
/// assert_eq!(result.unwrap().get(), 42);
///
/// // Zero value returns None
/// let zero_bytes = [0u8; 8];
/// let result = le_bytes_to_nonzero_i64_opt(&zero_bytes).unwrap();
/// assert!(result.is_none());
///
/// // Negative value (-1 in little-endian)
/// let negative_bytes = [255u8; 8];
/// let result = le_bytes_to_nonzero_i64_opt(&negative_bytes).unwrap();
/// assert_eq!(result.unwrap().get(), -1);
///
/// // Wrong size returns error
/// let short_bytes = [1, 2, 3, 4];
/// assert!(le_bytes_to_nonzero_i64_opt(&short_bytes).is_err());
///
/// let long_bytes = [1, 2, 3, 4, 5, 6, 7, 8, 9];
/// assert!(le_bytes_to_nonzero_i64_opt(&long_bytes).is_err());
/// ```
pub fn le_bytes_to_nonzero_i64_opt(bytes: &[u8]) -> Result<Option<NonZeroI64>, TryFromSliceError> {
    bytes
        .try_into()
        .map(i64::from_le_bytes)
        .map(NonZeroI64::new)
}

/// Represents configurable size limits for file compaction operations.
///
/// `SegmentSizeLimit` wraps a `SegmentSize` to define thresholds that determine when files
/// should be compacted. It supports limiting by multiple dimensions simultaneously:
/// blocks, bytes, rows, and file count.
///
/// ### Limit Behavior
///
/// - A limit value of `-1` or `0` means that dimension is ignored (no limit)
/// - The `length` field specifies the minimum number of files required for compaction
/// - Multiple dimensions can be active simultaneously
/// ```
#[derive(Debug, Clone, Copy)]
pub struct SegmentSizeLimit(
    /// The size limit for the file
    /// If a limit is set to -1, it means that dimension is not considered for the limit.
    pub SegmentSize,
);

// Interface methods
impl SegmentSizeLimit {
    pub fn new(blocks: i64, bytes: i64, rows: i64, length: usize) -> Self {
        Self(SegmentSize {
            blocks,
            bytes,
            rows,
            length,
        })
    }

    /// Checks if a segment exceeds the configured size limits.
    ///
    /// Returns a tuple of two booleans:
    /// - First: Whether all non-ignored size dimensions (blocks, bytes, rows) are exceeded
    /// - Second: Whether the file count exceeds the minimum required
    ///
    /// Note: Dimensions with limit value `-1` are considered as always exceeded.
    ///
    /// # Arguments
    ///
    /// * `segment` - The segment to check against the limits
    ///
    /// # Returns
    ///
    /// `(size_exceeded, count_exceeded)` where:
    /// - `size_exceeded`: true if all active size limits are met or exceeded
    /// - `count_exceeded`: true if segment has more files than the minimum required
    ///
    /// # Example
    ///
    /// ```
    /// # use dump::compaction::size::{SegmentSizeLimit, SegmentSize};
    /// let limit = SegmentSizeLimit::new(100, 1000, -1, 2);
    ///
    /// let small_segment = SegmentSize {
    ///     blocks: 50,
    ///     bytes: 500,
    ///     rows: 9999, // Ignored due to -1 limit
    ///     length: 1,
    /// };
    ///
    /// let (size_exceeded, count_exceeded) = limit.is_exceeded(&small_segment);
    /// assert!(!size_exceeded); // Not all size limits exceeded
    /// assert!(!count_exceeded); // Below minimum file count
    ///
    /// let large_segment = SegmentSize {
    ///     blocks: 200,
    ///     bytes: 2000,
    ///     rows: 1, // Still ignored
    ///     length: 3,
    /// };
    ///
    /// let (size_exceeded, count_exceeded) = limit.is_exceeded(&large_segment);
    /// assert!(size_exceeded); // All active limits exceeded
    /// assert!(count_exceeded); // Above minimum file count
    /// ```
    pub fn is_exceeded(&self, segment: &SegmentSize) -> (bool, bool) {
        (
            segment.blocks >= self.0.blocks
                && segment.bytes >= self.0.bytes
                && segment.rows >= self.0.rows,
            segment.length >= self.0.length,
        )
    }
}

impl From<&CompactionConfig> for SegmentSizeLimit {
    fn from(config: &CompactionConfig) -> Self {
        Self(SegmentSize {
            blocks: config.block_threshold,
            bytes: config.byte_threshold,
            rows: config.row_threshold,
            length: config.min_file_count,
        })
    }
}

impl Display for SegmentSizeLimit {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let size_string = format!("{}", self.0);
        write!(f, "{}", size_string.replace("null", "unbounded"))
    }
}

pub mod test {
    use std::sync::Arc;

    use common::{
        BLOCK_NUM,
        parquet::{
            basic::Type as PhysicalType,
            file::{
                metadata::{
                    ColumnChunkMetaData, FileMetaData, ParquetMetaData, ParquetMetaDataBuilder,
                    RowGroupMetaData,
                },
                statistics::Statistics,
            },
            schema::types::{ColumnDescriptor, ColumnPath, SchemaDescriptor, Type},
        },
    };

    /// Create a Parquet metadata object for testing.
    ///
    /// # Sanity Check
    /// ```
    /// use dump::compaction::size::test;
    /// let meta = test::parquet_meta(1000, 3, 400, 100);
    /// assert_eq!(meta.file_metadata().num_rows(), 3000);
    /// assert_eq!(meta.row_groups().len(), 3);
    /// assert_eq!(meta.row_groups()[0].num_rows(), 1000);
    /// assert_eq!(meta.row_groups()[1].num_rows(), 1000);
    /// assert_eq!(meta.row_groups()[2].num_rows(), 1000);
    /// assert_eq!(
    ///     meta.row_groups()[0].columns()[0]
    ///         .statistics()
    ///         .unwrap()
    ///         .max_bytes(),
    ///     199i64.to_le_bytes()
    /// );
    /// assert_eq!(
    ///     meta.row_groups()[0].columns()[0]
    ///         .statistics()
    ///         .unwrap()
    ///         .min_bytes(),
    ///     100i64.to_le_bytes()
    /// );
    /// assert_eq!(
    ///     meta.row_groups()[0].columns()[0]
    ///         .statistics()
    ///         .unwrap()
    ///         .null_count(),
    ///     0
    /// );
    /// assert_eq!(
    ///     meta.row_groups()[0].columns()[0]
    ///         .statistics()
    ///         .unwrap()
    ///         .distinct_count(),
    ///     Some(100u64)
    /// );
    /// ```
    pub fn parquet_meta(
        num_rows: i64,
        num_row_groups: i16,
        block_max: i64,
        block_min: i64,
    ) -> ParquetMetaData {
        assert!(num_row_groups > 0, "Must have at least one row group");
        assert!(
            block_max - block_min + 1 >= num_row_groups as i64,
            "Block range must cover at least one distinct block per row group"
        );

        fn schema_descr() -> SchemaDescriptor {
            let block_num = Type::primitive_type_builder(BLOCK_NUM, PhysicalType::INT64)
                .with_repetition(common::parquet::basic::Repetition::REPEATED)
                .build()
                .unwrap()
                .into();

            let schema = Type::group_type_builder("schema")
                .with_fields(vec![block_num])
                .build()
                .unwrap()
                .into();

            SchemaDescriptor::new(schema)
        }

        fn column_descr(schema_descr: Arc<SchemaDescriptor>) -> ColumnDescriptor {
            let primitive_type = schema_descr.column(0).self_type_ptr();
            let max_def_level = schema_descr.column(0).max_def_level();
            let max_rep_level = schema_descr.column(0).max_rep_level();
            let path = ColumnPath::new(vec![BLOCK_NUM.to_string()]);
            ColumnDescriptor::new(primitive_type, max_def_level, max_rep_level, path)
        }

        fn file_meta(num_rows: i64) -> FileMetaData {
            let schema_descr = schema_descr().into();
            FileMetaData::new(2, num_rows, None, None, schema_descr, None)
        }

        fn statistics(block_max: i64, block_min: i64) -> Statistics {
            let max = Some(block_max);
            let min = Some(block_min);
            let null_count = Some(0);
            let distinct_count = Some((block_max - block_min + 1) as u64);

            Statistics::new(min, max, distinct_count, null_count, false)
        }

        fn column_chunk_metadata(
            block_max: i64,
            block_min: i64,
            schema_descr: Arc<SchemaDescriptor>,
        ) -> ColumnChunkMetaData {
            ColumnChunkMetaData::builder(column_descr(schema_descr).into())
                .set_statistics(statistics(block_max, block_min).into())
                .build()
                .unwrap()
        }

        fn row_group(
            num_rows: i64,
            ordinal: i16,
            block_max: i64,
            block_min: i64,
        ) -> RowGroupMetaData {
            let schema_descr: Arc<SchemaDescriptor> = schema_descr().into();
            RowGroupMetaData::builder(schema_descr.clone())
                .set_num_rows(num_rows)
                .set_ordinal(ordinal)
                .add_column_metadata(column_chunk_metadata(block_max, block_min, schema_descr))
                .build()
                .unwrap()
        }
        let step = (block_max - block_min + 1) / num_row_groups as i64;
        let mut pmin = block_min;
        let mut pmax = block_min + step - 1;

        (0..num_row_groups)
            .fold(
                ParquetMetaDataBuilder::new(file_meta(num_rows * num_row_groups as i64)),
                |mut builder, ordinal| {
                    builder = builder.add_row_group(row_group(num_rows, ordinal, pmax, pmin));
                    pmin = pmax + 1;
                    pmax = pmin + step - 1;
                    builder
                },
            )
            .build()
    }

    #[test]
    fn segment_size_calc() {
        let options =
            common::parquet::arrow::arrow_reader::ArrowReaderOptions::new().with_page_index(true);
        let metadata = common::parquet::arrow::arrow_reader::ArrowReaderMetadata::try_new(
            parquet_meta(1000, 3, 400, 100).into(),
            options,
        )
        .unwrap();
        let file_size = crate::compaction::SegmentSize::from(&metadata);

        assert_eq!(file_size.length, 1);
        assert_eq!(file_size.blocks, 300);
        assert_eq!(file_size.rows, 3000);
        // We did not set data sizes in the test metadata, this is hard to do without writing data to disk
        // which we cannot do in a unit test
        assert_eq!(file_size.bytes, 0);
    }

    #[test]
    fn segment_size_display() {
        let size = super::SegmentSize {
            length: 12,
            blocks: 100,
            bytes: 1000,
            rows: 500,
        };
        assert_eq!(
            format!("{size}"),
            "{ length: 12, blocks: 100, bytes: 1000, rows: 500 }"
        );
        let size = super::SegmentSize {
            length: 0,
            blocks: -1,
            bytes: -1,
            rows: -1,
        };

        assert_eq!(format!("{size}"), "{ null }");
    }

    #[test]
    fn segment_size_limit_display() {
        let limit = super::SegmentSizeLimit::new(100, 1000, -1, 2);
        assert_eq!(
            format!("{limit}"),
            "{ length: 2, blocks: 100, bytes: 1000 }"
        );
        let limit = super::SegmentSizeLimit::new(-1, -1, -1, 0);
        assert_eq!(format!("{limit}"), "{ unbounded }");
    }
}
