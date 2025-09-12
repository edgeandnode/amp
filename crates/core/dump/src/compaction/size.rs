use std::{
    array::TryFromSliceError,
    fmt::{Debug, Display, Formatter},
    num::NonZeroI64,
    ops::{Add, AddAssign, Deref},
    time::Duration,
};

use chrono::{DateTime, Utc};
use common::{
    BLOCK_NUM, SPECIAL_BLOCK_NUM, Timestamp,
    metadata::parquet::{GENERATION_METADATA_KEY, PARQUET_METADATA_KEY, ParquetMeta},
};
use datafusion::parquet::{
    arrow::arrow_reader::ArrowReaderMetadata, file::metadata::RowGroupMetaData,
};

use crate::compaction::{
    Overflow,
    algorithm::{Cooldown, TestResult},
};

/// Represents the generation of a file, used to track how many times it has been compacted.
/// Each compaction operation increments the generation by 1.
/// A generation of 0 indicates the file is in its original, raw state.
/// This is useful for compaction algorithms that may want to prioritize
/// or treat files differently based on their generation.
///
/// # Examples
/// ```
/// # use dump::compaction::size::Generation;
/// # use std::ops::{Add, AddAssign};
/// let mut generation = Generation::default(); // Raw file
/// assert_eq!(*generation, 0u64);
/// assert!(generation.is_raw());
/// generation += 1_u64; // First compaction
/// assert_eq!(*generation, 1u64);
/// assert!(!generation.is_raw());
/// let second_generation = generation + 1_u64; // Second compaction
/// assert_eq!(*second_generation, 2u64);
/// assert!(!second_generation.is_raw());
/// assert!(second_generation > generation);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Generation(u64);

impl Generation {
    pub fn is_raw(&self) -> bool {
        self.0 == 0
    }
    pub fn is_compacted(&self) -> bool {
        self.0 > 0
    }
}

impl Default for Generation {
    fn default() -> Self {
        Self(0)
    }
}

impl Display for Generation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for Generation {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl Into<u64> for Generation {
    fn into(self) -> u64 {
        self.0
    }
}

impl Deref for Generation {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Add<u64> for Generation {
    type Output = Self;

    fn add(self, rhs: u64) -> Self::Output {
        Self(self.0.saturating_add(rhs))
    }
}

impl AddAssign<u64> for Generation {
    fn add_assign(&mut self, rhs: u64) {
        self.0 = self.0.saturating_add(rhs);
    }
}

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
/// # use dump::compaction::size::{SegmentSize, Generation};
/// // Create a SegmentSize representing a file with 1000 blocks, 1MB of data, and 5000 rows
/// let size = SegmentSize {
///     length: 1,
///     blocks: 1000,
///     bytes: 1_048_576,
///     rows: 5000,
///     ..Default::default()
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
///     ..Default::default()
/// };
/// let size2 = SegmentSize {
///     length: 1,
///     blocks: 50,
///     bytes: 500,
///     rows: 250,
///     ..Default::default()
/// };
///
/// let total = size1 + size2;
/// assert_eq!(total.length, 2);
/// assert_eq!(total.blocks, 150);
/// assert_eq!(total.bytes, 1500);
/// assert_eq!(total.rows, 750);
/// assert_eq!(total.generation, Generation::default());
///
/// // Addition assignment using += operator
/// let mut accumulator = SegmentSize {
///     length: 1,
///     blocks: 100,
///     bytes: 1000,
///     rows: 500,
///     ..Default::default()
/// };
///
/// accumulator += SegmentSize {
///     length: 1,
///     blocks: 50,
///     bytes: 500,
///     rows: 250,
///     ..Default::default()
/// };
///
/// assert_eq!(accumulator.blocks, 150);
/// assert_eq!(accumulator.bytes, 1500);
/// assert_eq!(accumulator.rows, 750);
/// assert_eq!(accumulator.length, 2);
/// assert_eq!(accumulator.generation, Generation::default());
///
/// // Collecting from an iterator
/// let sizes = vec![
///     SegmentSize {
///         length: 1,
///         blocks: 10,
///         bytes: 100,
///         rows: 50,
///         ..Default::default()
///     },
///     SegmentSize {
///         length: 1,
///         blocks: 20,
///         bytes: 200,
///         rows: 100,
///         ..Default::default()
///     },
///     SegmentSize {
///         length: 2,
///         blocks: 30,
///         bytes: 300,
///         rows: 150,
///         generation: 1u64.into(),
///         ..Default::default()
///     },
/// ];
///
/// let total: SegmentSize = sizes.into_iter().collect();
/// assert_eq!(total.length, 4);
/// assert_eq!(total.blocks, 60);
/// assert_eq!(total.bytes, 600);
/// assert_eq!(total.rows, 300);
/// assert_eq!(total.generation, 1u64.into());
///
/// // Collecting with filter
/// let file_sizes = vec![
///     SegmentSize {
///         length: 1,
///         blocks: 100,
///         bytes: 1000,
///         rows: 500,
///         ..Default::default()
///     },
///     SegmentSize {
///         length: 1,
///         blocks: 50,
///         bytes: 500,
///         rows: 250,
///         generation: 10u64.into(),
///         ..Default::default()
///     },
///     SegmentSize {
///         length: 1,
///         blocks: 200,
///         bytes: 2000,
///         rows: 1000,
///         ..Default::default()
///     },
///     SegmentSize {
///         length: 1,
///         blocks: 25,
///         bytes: 250,
///         rows: 125,
///         generation: 1u64.into(),
///         ..Default::default()
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
/// assert_eq!(large_files_total.generation, Generation::default()); // max(0, 10, 0, 1)
/// ```
#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
pub struct SegmentSize {
    /// Total number of files in the segment
    pub length: usize,
    /// Number of distinct blocks in the file
    pub blocks: i64,
    /// Size of the data in the file in bytes (does not include metadata, indexes, etc.)
    pub bytes: i64,
    /// Number of rows in the file
    pub rows: i64,
    /// The generation of the file (for tracking compaction iterations)
    pub generation: Generation,
    /// The timestamp when the segment was created (for cooldown calculations)
    /// This is a Unix timestamp in microseconds.
    pub created_at: u128,
}

impl Default for SegmentSize {
    fn default() -> Self {
        let created_at = Timestamp::now().0.as_micros();

        Self {
            length: 0,
            blocks: 0,
            bytes: 0,
            rows: 0,
            generation: Generation::default(),
            created_at,
        }
    }
}

impl Add for SegmentSize {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            blocks: self.blocks + other.blocks,
            bytes: self.bytes + other.bytes,
            rows: self.rows + other.rows,
            length: self.length + other.length,
            generation: self.generation.max(other.generation),
            created_at: self.created_at.max(other.created_at),
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
            generation: self.generation + rhs as u64,
            created_at: self.created_at,
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
        self.generation = self.generation.max(other.generation);
        self.created_at = self.created_at.min(other.created_at);
    }
}

impl<'a> From<&'a ArrowReaderMetadata> for SegmentSize {
    fn from(value: &'a ArrowReaderMetadata) -> Self {
        let file_metadata = value.metadata().file_metadata();
        let rows = file_metadata.num_rows();

        let generation = file_metadata
            .key_value_metadata()
            .and_then(|kv_metadata| {
                kv_metadata
                    .into_iter()
                    .find(|kv| kv.key == GENERATION_METADATA_KEY)
            })
            .and_then(|kv| kv.value.as_deref())
            .and_then(|v| v.parse::<u64>().ok())
            .map(Generation)
            .unwrap_or_default();

        let created_at = file_metadata
            .key_value_metadata()
            .and_then(|kv_metadata| {
                kv_metadata
                    .into_iter()
                    .find(|kv| kv.key == PARQUET_METADATA_KEY)
            })
            .and_then(|kv| kv.value.as_deref())
            .and_then(|v| serde_json::from_str(v).ok())
            .and_then(|meta: ParquetMeta| Some(meta.created_at.0.as_micros()))
            .unwrap_or_default();

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
            generation,
            created_at,
        }
    }
}

impl Display for SegmentSize {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let mut size_string = (0..6)
            .filter_map(|i| match i {
                0 if self.length != 0 => Some(format!("length: {}, ", self.length)),
                1 if self.blocks.is_positive() => Some(format!("blocks: {}, ", self.blocks)),
                2 if self.bytes.is_positive() => Some(format!("bytes: {}, ", self.bytes)),
                3 if self.rows.is_positive() => Some(format!("rows: {}, ", self.rows)),
                4 if self.generation.is_compacted() => {
                    Some(format!("generation: {}, ", self.generation))
                }
                5 if self.created_at != 0 => self
                    .created_at
                    .try_into()
                    .ok()
                    .and_then(|micros: i64| DateTime::<Utc>::from_timestamp_micros(micros))
                    .map(|dt| format!("created_at: {}", dt)),
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

    /// Creates a `Cooldown` instance based on the segment's generation.
    pub fn cooldown(&self, base: Duration) -> Cooldown {
        Cooldown::new(base, self.generation)
    }

    pub fn is_live(&self, lower_bound: &SegmentSizeLimit) -> TestResult {
        let (size_exceeded, length_exceeded, generation_exceeded) = lower_bound.is_exceeded(&self);

        size_exceeded
            .or(length_exceeded)
            .or(generation_exceeded)
            .not()
    }

    pub fn is_hot(&self, base_duration: Duration) -> TestResult {
        self.cooldown(base_duration).is_hot(self.created_at)
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
#[derive(Debug, Default, Clone, Copy)]
pub struct SegmentSizeLimit(
    /// The size limit for the file
    /// If a limit is set to -1, it means that dimension is not considered for the limit.
    pub SegmentSize,
    pub Overflow,
);

// Interface methods
impl SegmentSizeLimit {
    pub fn new(
        blocks: i64,
        bytes: i64,
        rows: i64,
        length: usize,
        generation: impl Into<Generation>,
        overflow: impl Into<Overflow>,
    ) -> Self {
        Self(
            SegmentSize {
                blocks,
                bytes,
                rows,
                length,
                generation: generation.into(),
                ..Default::default()
            },
            overflow.into(),
        )
    }

    pub fn default_bounded() -> Self {
        let mut this = Self::default();
        this.0.bytes = 512 * 1024 * 1024;
        this.1 = 1.5.into();
        this
    }

    pub fn is_unbounded(&self) -> bool {
        self.0.blocks <= 0
            && self.0.bytes <= 0
            && self.0.rows <= 0
            && self.0.length == 0
            && self.0.generation.is_raw()
    }

    /// Checks if a segment exceeds the size limits defined by this [`SegmentSizeLimit`].
    ///
    /// ## Note:
    /// - Tested dimensions are considered exceeded if they are greater than
    /// or equal to the corresponding limit.
    /// - Dimensions with limit value `-1` or `0` are considered
    /// as unbounded and are [`TestResult::Skipped`] in the evaluation.
    /// - The blocks, bytes, and rows dimensions are combined using [`TestResult::and`],
    /// meaning skipped dimensions do not propagate a failure if other dimensions exceed their limits.
    ///
    /// ## Arguments
    /// - `segment`: [`SegmentSize`] - The segment to check against the limits
    ///
    /// ## Returns
    /// [`SizeCheckResult`] - A tuple of three [`TestResult`] values:
    /// 1. Combined result for blocks, bytes, and rows dimensions
    /// 2. Result for the length (file count) dimension
    /// 3. Result for the generation dimension
    pub(super) fn is_exceeded(&self, segment: &SegmentSize) -> SizeCheckResult {
        let blocks_ge: TestResult = self
            .0
            .blocks
            .is_positive()
            .then_some(segment.blocks.ge(&self.1.soft_limit(self.0.blocks)))
            .into();

        let bytes_ge: TestResult = self
            .0
            .bytes
            .is_positive()
            .then_some(segment.bytes.ge(&self.1.soft_limit(self.0.bytes)))
            .into();

        let rows_ge: TestResult = self
            .0
            .rows
            .is_positive()
            .then_some(segment.rows.ge(&self.1.soft_limit(self.0.rows)))
            .into();

        let generation_ge: TestResult = self
            .0
            .generation
            .is_compacted()
            .then_some(segment.generation.ge(&self.0.generation))
            .into();

        let length_ge: TestResult = self
            .0
            .length
            .eq(&0)
            .then_some(segment.length.ge(&self.0.length))
            .into();

        (
            blocks_ge.and(bytes_ge).and(rows_ge),
            length_ge,
            generation_ge,
        )
    }
}

/// The result of checking if a segment exceeds size limits.
///
/// This is a tuple of three [`TestResult`] values:
/// 1. Combined result for blocks, bytes, and rows dimensions
/// 2. Result for the length (file count) dimension
/// 3. Result for the generation dimension
pub(super) type SizeCheckResult = (TestResult, TestResult, TestResult);

impl Display for SegmentSizeLimit {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let size_string = format!("{}", self.0);
        if size_string.contains("null") {
            return write!(f, "{{ unbounded }}");
        } else if !(self.1.0 == 1 && self.1.1 == 1) {
            // Only display overflow if it's not the default of 1
            return write!(
                f,
                "{}",
                size_string.replace(" }", &format!(", overflow: {} }}", self.1))
            );
        } else {
            return write!(f, "{}", size_string);
        }
    }
}

pub mod test {
    use std::sync::Arc;

    #[allow(unused_imports)]
    use chrono::{DateTime, Utc};
    #[allow(unused_imports)]
    use common::{
        BLOCK_NUM,
        parquet::{
            arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions},
            basic::{Repetition, Type as PhysicalType},
            file::{
                metadata::{
                    ColumnChunkMetaData, FileMetaData, KeyValue, ParquetMetaData,
                    ParquetMetaDataBuilder, RowGroupMetaData,
                },
                statistics::Statistics,
            },
            schema::types::{ColumnDescriptor, ColumnPath, SchemaDescriptor, Type},
        },
    };
    use common::{
        Timestamp,
        metadata::parquet::{GENERATION_METADATA_KEY, PARQUET_METADATA_KEY, ParquetMeta},
    };

    /// Create a Parquet metadata object for testing.
    ///
    /// # Sanity Check
    /// ```
    /// use common::Timestamp;
    /// use dump::compaction::size::test;
    /// let now = Timestamp::now();
    /// let meta = test::parquet_meta(1000, 3, 400, 100, 10000, 2u64.into(), now);
    /// assert_eq!(meta.file_metadata().num_rows(), 3000);
    /// assert_eq!(meta.row_groups().len(), 3);
    /// assert_eq!(meta.row_groups()[0].num_rows(), 1000);
    /// assert_eq!(meta.row_groups()[1].num_rows(), 1000);
    /// assert_eq!(meta.row_groups()[2].num_rows(), 1000);
    /// assert_eq!(
    ///     meta.row_groups()[0].columns()[0]
    ///         .statistics()
    ///         .unwrap()
    ///         .max_bytes_opt(),
    ///     199i64.to_le_bytes()[..8].try_into().ok()
    /// );
    /// assert_eq!(
    ///     meta.row_groups()[0].columns()[0]
    ///         .statistics()
    ///         .unwrap()
    ///         .min_bytes_opt(),
    ///     100i64.to_le_bytes()[..8].try_into().ok()
    /// );
    /// assert_eq!(
    ///     meta.row_groups()[0].columns()[0]
    ///         .statistics()
    ///         .unwrap()
    ///         .null_count_opt(),
    ///     Some(0u64)
    /// );
    /// assert_eq!(
    ///     meta.row_groups()[0].columns()[0]
    ///         .statistics()
    ///         .unwrap()
    ///         .distinct_count_opt(),
    ///     Some(100u64)
    /// );
    /// assert!(meta.file_metadata().key_value_metadata().is_some());
    /// assert!(
    ///     meta.file_metadata()
    ///         .key_value_metadata()
    ///         .as_ref()
    ///         .unwrap()
    ///         .iter()
    ///         .any(|kv| kv.key == "generation")
    /// );
    /// assert_eq!(
    ///     meta.file_metadata()
    ///         .key_value_metadata()
    ///         .as_ref()
    ///         .unwrap()
    ///         .iter()
    ///         .find(|kv| kv.key == "generation")
    ///         .unwrap()
    ///         .value
    ///         .as_ref()
    ///         .unwrap(),
    ///     "2"
    /// );
    /// assert!(
    ///     meta.file_metadata()
    ///         .key_value_metadata()
    ///         .as_ref()
    ///         .unwrap()
    ///         .iter()
    ///         .any(|kv| kv.key == "nozzle_metadata")
    /// );
    /// ```
    pub fn parquet_meta(
        num_rows: i64,
        num_row_groups: i16,
        block_max: i64,
        block_min: i64,
        byte_size: i64,
        generation: super::Generation,
        created_at: Timestamp,
    ) -> ParquetMetaData {
        assert!(num_row_groups > 0, "Must have at least one row group");
        assert!(
            block_max - block_min + 1 >= num_row_groups as i64,
            "Block range must cover at least one distinct block per row group"
        );
        fn schema_descr() -> SchemaDescriptor {
            let block_num = Type::primitive_type_builder(BLOCK_NUM, PhysicalType::INT64)
                .with_repetition(Repetition::REQUIRED)
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

        fn file_meta(
            total_rows: i64,
            generation: super::Generation,
            created_at: Timestamp,
        ) -> FileMetaData {
            let schema_descr = schema_descr().into();
            let mut key_value_metadata = vec![];

            key_value_metadata.push(KeyValue {
                key: GENERATION_METADATA_KEY.to_string(),
                value: Some(generation.to_string()),
            });

            let parquet_meta = ParquetMeta {
                table: String::from("test"),
                created_at,
                filename: String::from("test.parquet"),
                ranges: vec![],
            };

            key_value_metadata.push(KeyValue {
                key: PARQUET_METADATA_KEY.to_string(),
                value: Some(serde_json::to_string(&parquet_meta).unwrap()),
            });

            FileMetaData::new(
                2,
                total_rows,
                None,
                Some(key_value_metadata),
                schema_descr,
                None,
            )
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
            byte_size: i64,
        ) -> RowGroupMetaData {
            let schema_descr: Arc<SchemaDescriptor> = schema_descr().into();
            RowGroupMetaData::builder(schema_descr.clone())
                .set_num_rows(num_rows)
                .set_ordinal(ordinal)
                .set_total_byte_size(byte_size)
                .add_column_metadata(column_chunk_metadata(block_max, block_min, schema_descr))
                .build()
                .unwrap()
        }
        let step = (block_max - block_min + 1) / num_row_groups as i64;
        let mut pmin = block_min;
        let mut pmax = block_min + step - 1;

        let total_rows = num_rows * num_row_groups as i64;

        (0..num_row_groups)
            .fold(
                (
                    ParquetMetaDataBuilder::new(file_meta(total_rows, generation, created_at)),
                    byte_size,
                ),
                |(mut builder, mut bytes_remaining), ordinal| {
                    let byte_size = if ordinal == num_row_groups - 1 {
                        bytes_remaining
                    } else {
                        byte_size / num_row_groups as i64
                    };
                    bytes_remaining -= byte_size;
                    builder =
                        builder.add_row_group(row_group(num_rows, ordinal, pmax, pmin, byte_size));
                    pmin = pmax + 1;
                    pmax = pmin + step - 1;
                    (builder, bytes_remaining)
                },
            )
            .0
            .build()
    }

    #[test]
    fn segment_size_calc() {
        let options = ArrowReaderOptions::new().with_page_index(true);
        let now = Timestamp::now();
        let metadata = ArrowReaderMetadata::try_new(
            parquet_meta(1000, 3, 400, 100, 12345, 2u64.into(), now).into(),
            options,
        )
        .unwrap();
        let file_size = super::SegmentSize::from(&metadata);

        assert_eq!(file_size.length, 1);
        assert_eq!(file_size.blocks, 300);
        assert_eq!(file_size.rows, 3000);
        assert_eq!(file_size.bytes, 12345);
        assert_eq!(file_size.generation, 2u64.into());
        assert_eq!(file_size.created_at, now.0.as_micros());
    }

    #[test]
    fn segment_size_display() {
        let now = Timestamp::now();
        let created_at = now.0.as_micros();
        let created_at_dt = DateTime::<Utc>::from_timestamp_micros(created_at as i64).unwrap();
        let now_str = format!("{}", created_at_dt);

        let size = super::SegmentSize {
            length: 12,
            blocks: 100,
            bytes: 1000,
            rows: 500,
            generation: super::Generation::from(2u64),
            created_at,
        };
        assert_eq!(
            format!("{size}"),
            format!(
                "{{ length: 12, blocks: 100, bytes: 1000, rows: 500, generation: 2, created_at: {now_str} }}"
            )
        );

        let size = super::SegmentSize::default();

        assert_eq!(format!("{size}"), "{ null }", "Testing default null size");
    }

    #[test]
    fn segment_size_limit_display() {
        let limit = super::SegmentSizeLimit::new(100, 1000, -1, 2, 2, 1.5);
        assert_eq!(
            format!("{limit}"),
            "{ length: 2, blocks: 100, bytes: 1000, generation: 2, overflow: 3/2 }",
            "We are testing if the overflow of 1.5 is correctly represented as 3/2 and if the rows limit is omitted because it is -1"
        );

        let limit = super::SegmentSizeLimit::new(100, 1000, -1, 2, 0, 1u64);
        assert_eq!(
            format!("{limit}"),
            "{ length: 2, blocks: 100, bytes: 1000 }",
            "We are testing if the overflow, rows, and generation limits are omitted because they are 1, -1, and 0 respectively"
        );

        let limit = super::SegmentSizeLimit::new(-1, -1, -1, 0, 0, 3u64);
        assert_eq!(
            format!("{limit}"),
            "{ unbounded }",
            "We are testing if the limit is correctly represented as unbounded which means the overflow is omitted even though it is 3"
        );

        let limit = super::SegmentSizeLimit::new(100, 1000, -1, 2, 0, 4u64);
        assert_eq!(
            format!("{limit}"),
            "{ length: 2, blocks: 100, bytes: 1000, overflow: 4 }",
            "We are testing if the generation and rows limits are omitted because they are 0 and -1 respectively, overflow is shown as 4"
        );

        let limit = super::SegmentSizeLimit::default();

        assert_eq!(
            format!("{limit}"),
            "{ unbounded }",
            "Testing default unbounded size limit"
        );
    }
}
