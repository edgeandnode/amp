use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    num::NonZero,
    ops::{AddAssign, Index, IndexMut, Not},
};

use common::{BLOCK_NUM, SPECIAL_BLOCK_NUM, parquet::arrow::arrow_reader::ArrowReaderMetadata};

/// ### FileSize
/// Represents the size of a file in different units.
///
/// #### Layout
/// - The layout of the struct is as follows:
///   - Field 0: number of blocks
///   - Field 1: number of bytes
///   - Field 2: number of rows
#[derive(Debug, Clone, Copy)]
pub struct FileSize([i64; 3]);

impl FileSize {
    const BLOCK_INDEX: usize = 0;
    const BYTE_INDEX: usize = 1;
    const ROW_INDEX: usize = 2;

    const BLOCK_NAME: &'static str = "blocks";
    const BYTE_NAME: &'static str = "bytes";
    const ROW_NAME: &'static str = "rows";

    pub fn blocks(&self) -> i64 {
        self[Self::BLOCK_INDEX]
    }

    pub fn bytes(&self) -> i64 {
        self[Self::BYTE_INDEX]
    }

    pub fn rows(&self) -> i64 {
        self[Self::ROW_INDEX]
    }
}

impl Default for FileSize {
    fn default() -> Self {
        FileSize([0, 0, 0])
    }
}

impl Index<usize> for FileSize {
    type Output = i64;

    fn index(&self, index: usize) -> &Self::Output {
        assert!(index < 3, "Index out of bounds for FileSize");
        &self.0[index]
    }
}

impl IndexMut<usize> for FileSize {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        assert!(index < 3, "Index out of bounds for FileSize");
        &mut self.0[index]
    }
}

impl AddAssign for FileSize {
    fn add_assign(&mut self, other: Self) {
        self[Self::BLOCK_INDEX] += other[Self::BLOCK_INDEX];
        self[Self::BYTE_INDEX] += other[Self::BYTE_INDEX];
        self[Self::ROW_INDEX] += other[Self::ROW_INDEX];
    }
}

impl FromIterator<(i64, usize)> for FileSize {
    fn from_iter<T: IntoIterator<Item = (i64, usize)>>(iter: T) -> Self {
        let mut size = FileSize([0, 0, 0]);
        for (value, i) in iter.into_iter().take(3) {
            size[i % 3] = value;
        }
        size
    }
}

impl FileSize {
    /// For tracing
    pub fn value(&self) -> String {
        format!("{self}")
    }
}

impl Display for FileSize {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_set()
            .entries(self.0.iter().enumerate().filter_map(|(i, &value)| {
                value.eq(&0).not().then_some(match i {
                    Self::BLOCK_INDEX => format!("{}: {value}", Self::BLOCK_NAME),
                    Self::BYTE_INDEX => format!("{}: {value}", Self::BYTE_NAME),
                    Self::ROW_INDEX => format!("{}: {value}", Self::ROW_NAME),
                    _ => unreachable!(),
                })
            }))
            .finish()
    }
}

/// ### FileSizeLimitInterface
/// This is a generic struct that can be used to represent different
///   configurations of file size limits for defining file groups for
///   compaction.
/// #### Generic Parameter(s)
/// - It has a generic parameter:
///   - `A`: constant boolean that indicates whether the api is available
///     for the particular combination of limits. When `A` is `true`, the API
///     is available; when `A` is `false`, the API is not available.
/// #### Layout
/// - The layout of the struct is as follows:
///   - Field 0 `[bool; 3]`: enforcement flags for each limit kind
///   - Field 1 `[i64; 3]`: the actual limits (maximum) for each limit kind
/// #### Order
/// - The order of the elements in the arrays is as follows:
///   - Index 0: blocks
///   - Index 1: bytes
///   - Index 2: rows
#[derive(Debug, Clone, Copy)]
pub struct FileSizeLimitInterface<const A: bool = true>(
    /// Enforcement flags for each limit kind
    [bool; 3],
    /// The actual limits (maximum) for each limit kind
    [i64; 3],
);

pub type DefaultFileSizeLimit = FileSizeLimitInterface<false>;
pub type FileSizeLimit = FileSizeLimitInterface<true>;

impl Default for DefaultFileSizeLimit {
    fn default() -> Self {
        FileSizeLimitInterface([false; 3], [0; 3])
    }
}

impl<const A: bool> FileSizeLimitInterface<A> {
    const BLOCK_INDEX: usize = FileSize::BLOCK_INDEX;
    const BYTE_INDEX: usize = FileSize::BYTE_INDEX;
    const ROW_INDEX: usize = FileSize::ROW_INDEX;

    const BLOCK_LIMIT_NAME: &'static str = "block limit";
    const BYTE_LIMIT_NAME: &'static str = "byte limit";
    const ROW_LIMIT_NAME: &'static str = "row limit";

    const BLOCK_THRESHOLD: i64 = 100_000; // 100k blocks
    const BYTE_THRESHOLD: i64 = 2 * 1024 * 1024 * 1024; // 2 GB
    const ROW_THRESHOLD: i64 = 10_000_000; // 10 million rows

    const BLOCK_LIMIT_KIND: fn(i64) -> FileSizeLimitKind = FileSizeLimitKind::Block;
    const BYTE_LIMIT_KIND: fn(i64) -> FileSizeLimitKind = FileSizeLimitKind::Byte;
    const ROW_LIMIT_KIND: fn(i64) -> FileSizeLimitKind = FileSizeLimitKind::Row;
}

impl DefaultFileSizeLimit {
    pub fn with_block_limit<T: Into<NonZero<i64>>>(mut self, limit: T) -> FileSizeLimit {
        self.0[Self::BLOCK_INDEX] = true;
        self.1[Self::BLOCK_INDEX] = i64::from(limit.into());
        FileSizeLimitInterface::<true>(self.0, self.1)
    }

    pub fn with_byte_limit<T: Into<NonZero<i64>>>(mut self, limit: T) -> FileSizeLimit {
        self.0[Self::BYTE_INDEX] = true;
        self.1[Self::BYTE_INDEX] = i64::from(limit.into());
        FileSizeLimitInterface::<true>(self.0, self.1)
    }

    pub fn with_row_limit<T: Into<NonZero<i64>>>(mut self, limit: T) -> FileSizeLimit {
        self.0[Self::ROW_INDEX] = true;
        self.1[Self::ROW_INDEX] = i64::from(limit.into());
        FileSizeLimitInterface::<true>(self.0, self.1)
    }
}

impl FileSizeLimit {
    pub fn default() -> DefaultFileSizeLimit {
        DefaultFileSizeLimit::default()
    }

    #[allow(non_snake_case)]
    pub const fn BLOCK() -> Self {
        FileSizeLimitInterface([true, false, false], [Self::BLOCK_THRESHOLD, 0, 0])
    }

    #[allow(non_snake_case)]
    pub const fn BYTE() -> Self {
        FileSizeLimitInterface([false, true, false], [0, Self::BYTE_THRESHOLD, 0])
    }

    #[allow(non_snake_case)]
    pub const fn ROW() -> Self {
        FileSizeLimitInterface([false, false, true], [0, 0, Self::ROW_THRESHOLD])
    }

    pub fn get_size(&self, reader_metadata: &ArrowReaderMetadata) -> FileSize {
        self.enabled()
            .map(|(kind, ..)| kind.get_size(reader_metadata))
            .collect()
    }

    pub fn is_exceeded(&self, size: FileSize) -> bool {
        self.enabled()
            .any(|(kind, index)| kind.is_exceeded(size[index]))
    }

    fn enabled(&self) -> impl Iterator<Item = (FileSizeLimitKind, usize)> {
        self.0.iter().enumerate().filter_map(|(index, &enabled)| {
            enabled.then_some(match index {
                Self::BLOCK_INDEX => (
                    Self::BLOCK_LIMIT_KIND(self.1[Self::BLOCK_INDEX]),
                    Self::BLOCK_INDEX,
                ),
                Self::BYTE_INDEX => (
                    Self::BYTE_LIMIT_KIND(self.1[Self::BYTE_INDEX]),
                    Self::BYTE_INDEX,
                ),
                Self::ROW_INDEX => (
                    Self::ROW_LIMIT_KIND(self.1[Self::ROW_INDEX]),
                    Self::ROW_INDEX,
                ),
                _ => unreachable!(),
            })
        })
    }

    /// Set the block limit for the file size limit
    /// - If the limit is zero, the block limit will be removed unless it is the
    /// only limit set.
    /// - If the limit is not zero, the block limit will be set to the new limit.
    pub fn with_block_limit<T: Into<i64>>(mut self, limit: T) -> FileSizeLimit {
        if let Some(limit) = NonZero::new(limit.into()) {
            self.0[Self::BLOCK_INDEX] = true;
            self.1[Self::BLOCK_INDEX] = limit.into();
        } else if self.0.iter().filter(|&&x| x).count() > 1 {
            self.0[Self::BLOCK_INDEX] = false;
            self.1[Self::BLOCK_INDEX] = 0;
        }

        self
    }

    /// Set the byte limit for the file size limit
    /// - If the limit is zero, the byte limit will be removed unless it is the
    /// only limit set.
    /// - If the limit is not zero, the byte limit will be set to the new limit.
    pub fn with_byte_limit<T: Into<i64>>(mut self, limit: T) -> FileSizeLimit {
        if let Some(limit) = NonZero::new(limit.into()) {
            self.0[Self::BYTE_INDEX] = true;
            self.1[Self::BYTE_INDEX] = limit.into();
        } else if self.0.iter().filter(|&&x| x).count() > 1 {
            self.0[Self::BYTE_INDEX] = false;
            self.1[Self::BYTE_INDEX] = 0;
        }

        self
    }

    /// Set the row limit for the file size limit
    /// - If the limit is zero, the row limit will be removed unless it is the
    /// only limit set.
    /// - If the limit is not zero, the row limit will be set to the new limit.
    pub fn with_row_limit<T: Into<i64>>(mut self, limit: T) -> FileSizeLimit {
        if let Some(limit) = NonZero::new(limit.into()) {
            self.0[Self::ROW_INDEX] = true;
            self.1[Self::ROW_INDEX] = limit.into();
        } else if self.0.iter().filter(|&&x| x).count() > 1 {
            self.0[Self::ROW_INDEX] = false;
            self.1[Self::ROW_INDEX] = 0;
        }

        self
    }

    /// Merge another FileSizeLimit into this one.
    /// It combines the enabled states of the limits and applies a function
    /// `F: Fn(i64, i64) -> i64` to merge the limit values.
    ///
    /// See also [FileSizeLimit::merge_max] for a specific implementation
    pub fn merge<F: Fn(i64, i64) -> i64>(mut self, other: Self, f: F) -> Self {
        self.0[Self::BLOCK_INDEX] |= other.0[Self::BLOCK_INDEX];
        self.0[Self::BYTE_INDEX] |= other.0[Self::BYTE_INDEX];
        self.0[Self::ROW_INDEX] |= other.0[Self::ROW_INDEX];
        self.1[Self::BLOCK_INDEX] = f(self.1[Self::BLOCK_INDEX], other.1[Self::BLOCK_INDEX]);
        self.1[Self::BYTE_INDEX] = f(self.1[Self::BYTE_INDEX], other.1[Self::BYTE_INDEX]);
        self.1[Self::ROW_INDEX] = f(self.1[Self::ROW_INDEX], other.1[Self::ROW_INDEX]);
        self
    }

    /// Merge another FileSizeLimit into this one.
    /// It combines the enabled states of the limits and uses `i64::max`
    /// to merge the limit values.
    pub fn merge_max(self, other: Self) -> Self {
        self.merge(other, i64::max)
    }

    /// Intersect this FileSizeLimit with another one by combining the enabled
    /// states of the limits using bitwise AND to determine which limits are
    /// still enabled; applying a function `F: Fn(i64, i64) -> i64` to merge
    /// enabled limit values.
    ///
    /// See also [FileSizeLimit::intersect_max] for a specific implementation
    pub fn intersect<F: Fn(i64, i64) -> i64>(mut self, other: Self, f: F) -> Self {
        self.0[Self::BLOCK_INDEX] &= other.0[Self::BLOCK_INDEX];
        self.0[Self::BYTE_INDEX] &= other.0[Self::BYTE_INDEX];
        self.0[Self::ROW_INDEX] &= other.0[Self::ROW_INDEX];

        if self.0[Self::BLOCK_INDEX] {
            self.1[Self::BLOCK_INDEX] = f(self.1[Self::BLOCK_INDEX], other.1[Self::BLOCK_INDEX]);
        } else {
            self.1[Self::BLOCK_INDEX] = 0;
        }

        if self.0[Self::BYTE_INDEX] {
            self.1[Self::BYTE_INDEX] = f(self.1[Self::BYTE_INDEX], other.1[Self::BYTE_INDEX]);
        } else {
            self.1[Self::BYTE_INDEX] = 0;
        }

        if self.0[Self::ROW_INDEX] {
            self.1[Self::ROW_INDEX] = f(self.1[Self::ROW_INDEX], other.1[Self::ROW_INDEX]);
        } else {
            self.1[Self::ROW_INDEX] = 0;
        }

        self
    }

    /// Merge another FileSizeLimit into this one.
    /// It combines the enabled states of the limits and uses `i64::max`
    /// to merge the limit values.
    pub fn intersect_max(self, other: Self) -> Self {
        self.intersect(other, i64::max)
    }
}

impl FileSizeLimit {
    /// For tracing
    pub fn value(&self) -> String {
        format!("{self}")
    }
}

impl Display for FileSizeLimit {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_set()
            .entries(self.0.iter().enumerate().filter_map(|(i, &enforce)| {
                enforce.then_some(match i {
                    Self::BLOCK_INDEX => {
                        format!("{}: {}", Self::BLOCK_LIMIT_NAME, self.1[Self::BLOCK_INDEX])
                    }
                    Self::BYTE_INDEX => {
                        format!("{}: {}", Self::BYTE_LIMIT_NAME, self.1[Self::BYTE_INDEX])
                    }
                    Self::ROW_INDEX => {
                        format!("{}: {}", Self::ROW_LIMIT_NAME, self.1[Self::ROW_INDEX])
                    }
                    _ => unreachable!(),
                })
            }))
            .finish()
    }
}

#[derive(Debug, Clone, Copy)]
pub enum FileSizeLimitKind {
    Block(i64),
    Byte(i64),
    Row(i64),
}

impl FileSizeLimitKind {
    fn is_exceeded<T: Into<i64>>(&self, size: T) -> bool {
        match self {
            Self::Block(limit) => size.into() >= *limit,
            Self::Byte(limit) => size.into() >= *limit,
            Self::Row(limit) => size.into() >= *limit,
        }
    }

    fn get_size(&self, reader_metadata: &ArrowReaderMetadata) -> (i64, usize) {
        match self {
            Self::Block(..) => {
                let metadata = reader_metadata.metadata();
                let (distinct_block_count, ..) =
                    metadata
                        .row_groups()
                        .iter()
                        .fold((0, 0), |(mut distinct, mut pmax), rg| {
                            let column_descriptor = rg
                                .schema_descr()
                                .columns()
                                .iter()
                                .find(|c| c.name() == BLOCK_NUM || c.name() == SPECIAL_BLOCK_NUM)
                                .expect("Block number column not found in schema");

                            let column_chunk = rg
                                .columns()
                                .iter()
                                .find(|c| c.column_path() == column_descriptor.path())
                                .expect("Column stats not found for block number column");

                            let column_statistics = column_chunk.statistics().unwrap();

                            let min_block_num = column_statistics
                                .min_bytes_opt()
                                .map(|bytes| i64::from_le_bytes(bytes[0..8].try_into().unwrap()))
                                .unwrap_or_default();

                            let max_block_num = column_statistics
                                .max_bytes_opt()
                                .map(|bytes| i64::from_le_bytes(bytes[0..8].try_into().unwrap()))
                                .unwrap_or_default();

                            let distinct_block_count =
                                column_statistics.distinct_count_opt().unwrap_or_default() as i64;

                            distinct += min_block_num - pmax + distinct_block_count;

                            pmax = max_block_num;

                            (distinct, pmax)
                        });

                (distinct_block_count, FileSize::BLOCK_INDEX)
            }
            FileSizeLimitKind::Byte(..) => (
                reader_metadata
                    .metadata()
                    .row_groups()
                    .iter()
                    .map(|rg| rg.total_byte_size())
                    .sum(),
                FileSize::BYTE_INDEX,
            ),
            FileSizeLimitKind::Row(..) => (
                reader_metadata.metadata().file_metadata().num_rows(),
                FileSize::ROW_INDEX,
            ),
        }
    }
}
