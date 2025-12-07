use std::{
    fmt::{Debug, Display, Formatter},
    ops::Not,
    time::Duration,
};

use common::{
    Timestamp,
    arrow::array::ArrowNativeTypeOp,
    config::{ParquetConfig, SizeLimitConfig},
    metadata::{Generation, Overflow, SegmentSize},
};

use crate::compaction::{compactor::CompactionGroup, plan::CompactionFile};

/// Compaction algorithm parameters.
/// Defines the criteria for grouping files for compaction
/// based on their size and age.
///
/// ## Fields
/// - `cooldown_duration`: The base duration used to calculate
///   the cooldown period for files based on their generation.
/// - `target_partition_size`: The upper bound for segment size limits.
///   Files exceeding this limit will not be compacted together. This
///   value must be non-unbounded.
/// - `max_eager_generation`: Segments up to this generation will not be subject to cooldowns.
#[derive(Clone, Copy, Debug)]
pub struct CompactionAlgorithm {
    /// The amount of time a file must wait before it can be
    /// compacted with files of different generations.
    pub cooldown_duration: Duration,
    /// The upper bound for segment size limits. Files exceeding this limit
    /// will not be compacted together. This value must be non-unbounded.
    pub target_partition_size: SegmentSizeLimit,

    /// Segments up to this generation will not be subject to cooldowns
    pub max_eager_generation: Option<Generation>,
}

impl CompactionAlgorithm {
    fn is_live(&self, segment: &SegmentSize) -> bool {
        Some(segment.generation) <= self.max_eager_generation
    }

    fn is_hot(&self, segment: &SegmentSize) -> TestResult {
        Cooldown::new(self.cooldown_duration).is_hot(segment.created_at)
    }

    /// Determines the state of a file:
    /// - `Live`: Cooldown does not apply to this file, it can be compacted immediately.
    /// - `Hot`: The file is still within its cooldown period.
    /// - `Cold`: The file is outside its cooldown period.
    fn file_state(&self, segment: &SegmentSize) -> FileState {
        let is_live = self.is_live(segment);

        if is_live {
            return FileState::Live;
        }

        let is_hot = self.is_hot(segment);

        match is_hot {
            // If there is no cooldown, file is always cold.
            TestResult::Skipped => FileState::Cold,
            TestResult::Activated(true) => FileState::Hot,
            TestResult::Activated(false) => FileState::Cold,
        }
    }

    /// Predicate function to determine if:
    /// - When a group is empty, if the candidate can start a new group.
    /// - When a group is started, if the candidate can be added to it.
    ///
    /// The current algorithm is:
    /// - If the file is `Hot`, it cannot start a new group.
    /// - If a group has been started, it will accept files up to the target size, regardless of file state.
    pub fn predicate(&self, group: &CompactionGroup, candidate: &CompactionFile) -> bool {
        if group.is_empty() && self.file_state(&candidate.size) == FileState::Hot {
            return false;
        }

        // Check if combining sizes exceeds upper bound.
        let size_exceeded = self
            .target_partition_size
            .is_exceeded(&(candidate.size + group.size));

        match size_exceeded {
            TestResult::Activated(exceeded) => !exceeded,

            // If all limits are zero, assume compaction is disabled.
            TestResult::Skipped => false,
        }
    }
}

impl<'a> From<&'a ParquetConfig> for CompactionAlgorithm {
    fn from(config: &'a ParquetConfig) -> Self {
        CompactionAlgorithm {
            cooldown_duration: config.compactor.algorithm.cooldown_duration.clone().into(),
            target_partition_size: SegmentSizeLimit::from(&config.target_size),
            max_eager_generation: {
                let generation = config.compactor.algorithm.max_eager_generation;
                if generation < 0 {
                    None
                } else {
                    Some(Generation::from(generation as u64))
                }
            },
        }
    }
}

/// Cooldown period for file compaction. Before the period elapses,
/// the file will only be compacted if the candidate group shares the
/// same generation.
#[derive(Clone, Copy)]
pub struct Cooldown(Duration);

impl Cooldown {
    pub fn new(duration: Duration) -> Self {
        Self(duration)
    }

    /// Returns the cooldown period as a `Duration`
    pub fn as_micros(&self) -> u128 {
        self.0.as_micros()
    }

    pub(super) fn is_hot(&self, created_at: u128) -> TestResult {
        if self.0.is_zero() {
            TestResult::Skipped
        } else {
            let now = Timestamp::now();
            TestResult::Activated(created_at.saturating_add(self.as_micros()) > now.0.as_micros())
        }
    }
}

impl Debug for Cooldown {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Cooldown({:?})", self.0)
    }
}

impl Display for Cooldown {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

/// Represents the state of a file for compaction purposes.
/// The states are ordered like so:
/// `Live < Hot < Cold`
#[derive(Debug, PartialEq, Eq)]
pub enum FileState {
    Live,
    Hot,
    Cold,
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
        blocks: u64,
        bytes: u64,
        rows: u64,
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
        self.0.blocks == 0
            && self.0.bytes == 0
            && self.0.rows == 0
            && self.0.length == 0
            && self.0.generation.is_raw()
    }

    /// Checks if a segment exceeds the size limits defined by this [`SegmentSizeLimit`].
    ///
    /// ## Note:
    /// - Tested dimensions are considered exceeded if they are greater than
    ///   or equal to the corresponding limit.
    /// - Dimensions with limit value `-1` or `0` are considered
    ///   as unbounded and are [`TestResult::Skipped`] in the evaluation.
    /// - The blocks, bytes, and rows dimensions are combined using [`TestResult::and`],
    ///   meaning skipped dimensions do not propagate a failure if other dimensions exceed their limits.
    ///
    /// ## Arguments
    /// - `segment`: [`SegmentSize`] - The segment to check against the limits
    ///
    /// ## Returns
    /// [`TestResult`] for combined `or` result of blocks, bytes, and rows limits
    pub fn is_exceeded(&self, segment: &SegmentSize) -> TestResult {
        let blocks_ge: TestResult = self
            .0
            .blocks
            .is_zero()
            .not()
            .then_some(segment.blocks.ge(&self.1.soft_limit(self.0.blocks)))
            .into();

        let bytes_ge: TestResult = self
            .0
            .bytes
            .is_zero()
            .not()
            .then_some(segment.bytes.ge(&self.1.soft_limit(self.0.bytes)))
            .into();

        let rows_ge: TestResult = self
            .0
            .rows
            .is_zero()
            .not()
            .then_some(segment.rows.ge(&self.1.soft_limit(self.0.rows)))
            .into();

        blocks_ge.or(bytes_ge).or(rows_ge)
    }
}

impl Display for SegmentSizeLimit {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let size_string = format!("{}", self.0);
        if size_string.contains("null") {
            write!(f, "{{ unbounded }}")
        } else if !(self.1.0.get() == 1 && self.1.1.get() == 1) {
            // Only display overflow if it's not the default of 1
            write!(
                f,
                "{}",
                size_string.replace(" }", &format!(", overflow: {} }}", self.1))
            )
        } else {
            write!(f, "{}", size_string)
        }
    }
}

impl<'a> From<&'a SizeLimitConfig> for SegmentSizeLimit {
    fn from(value: &'a SizeLimitConfig) -> Self {
        Self::new(
            value.blocks,
            value.bytes,
            value.rows,
            value.file_count as usize,
            value.generation,
            value.overflow,
        )
    }
}

/// Three-valued logic to represent boolean tests that may be skipped.
///
/// It differs from kleene algebra in that the skipped value is
/// neutral in `AND` and `OR` operations, rather than propagating
/// uncertainty. This is useful for cases where a test may be
/// conditionally skipped, and we want to ignore its result in
/// favor of other tests. However, if all tests are skipped,
/// the overall result is considered false when dereferenced.
///
/// [`TestResult`] values can be constructed from [`Option<bool>`],
/// where `Some(bool)` maps to `Activated(bool)` and `None`
/// maps to `Skipped`. Internal values are not exposed, and
/// can only be accessed via dereferencing.
#[derive(Debug)]
pub enum TestResult {
    Activated(bool),
    Skipped,
}

impl TestResult {
    /// Logical AND operation.
    ///
    /// Does not propagate uncertainty; if either operand is `Skipped`,
    /// the result is the other operand. If both are `Activated`,
    /// the result is the logical AND of their values. If both are `Skipped`,
    /// the result is `Skipped`.
    ///
    /// # Truth Table
    /// | A                | B                | A AND B          | Deref(A AND B)   |
    /// |------------------|------------------|------------------|------------------|
    /// | Activated(true)  | Activated(true)  | Activated(true)  | true             |
    /// | Activated(true)  | Activated(false) | Activated(false) | false            |
    /// | Activated(false) | Activated(true)  | Activated(false) | false            |
    /// | Activated(false) | Activated(false) | Activated(false) | false            |
    /// | Skipped          | Activated(true)  | Activated(true)  | true             |
    /// | Skipped          | Activated(false) | Activated(false) | false            |
    /// | Activated(true)  | Skipped          | Activated(true)  | true             |
    /// | Activated(false) | Skipped          | Activated(false) | false            |
    /// | Skipped          | Skipped          | Skipped          | false            |
    pub fn and(self, other: TestResult) -> TestResult {
        match (self, other) {
            (TestResult::Activated(a), TestResult::Activated(b)) => TestResult::Activated(a && b),
            (TestResult::Skipped, b) => b,
            (a, TestResult::Skipped) => a,
        }
    }

    /// Logical OR operation.
    ///
    /// Does not propagate uncertainty; if either operand is `Skipped`,
    /// the result is the other operand. If both are `Activated`,
    /// the result is the logical OR of their values. If both are `Skipped`,
    /// the result is `Skipped`.
    ///
    /// # Truth Table
    /// | A                | B                | A OR B           | Deref(A OR B)    |
    /// |------------------|------------------|------------------|------------------|
    /// | Activated(true)  | Activated(true)  | Activated(true)  | true             |
    /// | Activated(true)  | Activated(false) | Activated(true)  | true             |
    /// | Activated(false) | Activated(true)  | Activated(true)  | true             |
    /// | Activated(false) | Activated(false) | Activated(false) | false            |
    /// | Skipped          | Activated(true)  | Activated(true)  | true             |
    /// | Skipped          | Activated(false) | Activated(false) | false            |
    /// | Activated(true)  | Skipped          | Activated(true)  | true             |
    /// | Activated(false) | Skipped          | Activated(false) | false            |
    /// | Skipped          | Skipped          | Skipped          | false            |
    pub fn or(self, other: TestResult) -> TestResult {
        match (self, other) {
            (TestResult::Activated(a), TestResult::Activated(b)) => TestResult::Activated(a || b),
            (TestResult::Skipped, b) => b,
            (a, TestResult::Skipped) => a,
        }
    }
}

impl From<Option<bool>> for TestResult {
    fn from(value: Option<bool>) -> Self {
        match value {
            Some(v) => TestResult::Activated(v),
            None => TestResult::Skipped,
        }
    }
}

impl PartialEq for TestResult {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (TestResult::Activated(a), TestResult::Activated(b)) => a == b,
            (TestResult::Skipped, TestResult::Skipped) => true,
            _ => false,
        }
    }
}

impl Not for TestResult {
    type Output = TestResult;

    /// Logical NOT operation.
    ///
    /// Implements kleene-style negation. If the operand is `Skipped`,
    /// the result is also `Skipped`. If the operand is `Activated`,
    /// the result is the logical NOT of its value.
    ///
    /// # Truth Table
    /// | A                | NOT A            | Deref(NOT A)     |
    /// |------------------|------------------|------------------|
    /// | Activated(true)  | Activated(false) | false            |
    /// | Activated(false) | Activated(true)  | true             |
    /// | Skipped          | Skipped          | false            |
    fn not(self) -> TestResult {
        match self {
            TestResult::Activated(a) => TestResult::Activated(!a),
            TestResult::Skipped => TestResult::Skipped,
        }
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn segment_size_limit_display() {
        let limit = super::SegmentSizeLimit::new(100, 1000, 0, 2, 2, 1.5);
        assert_eq!(
            format!("{limit}"),
            "{ length: 2, blocks: 100, bytes: 1000, generation: 2, overflow: 3/2 }",
            "We are testing if the overflow of 1.5 is correctly represented as 3/2 and if the rows limit is omitted because it is 0"
        );

        let limit = super::SegmentSizeLimit::new(100, 1000, 0, 2, 0, 1u64);
        assert_eq!(
            format!("{limit}"),
            "{ length: 2, blocks: 100, bytes: 1000 }",
            "We are testing if the overflow, rows, and generation limits are omitted because they are 1, 0, and 0 respectively"
        );

        let limit = super::SegmentSizeLimit::new(0, 0, 0, 0, 0, 3u64);
        assert_eq!(
            format!("{limit}"),
            "{ unbounded }",
            "We are testing if the limit is correctly represented as unbounded which means the overflow is omitted even though it is 3"
        );

        let limit = super::SegmentSizeLimit::new(100, 1000, 0, 2, 0, 4u64);
        assert_eq!(
            format!("{limit}"),
            "{ length: 2, blocks: 100, bytes: 1000, overflow: 4 }",
            "We are testing if the generation and rows limits are omitted because they are both 0, overflow is shown as 4"
        );

        let limit = super::SegmentSizeLimit::default();

        assert_eq!(
            format!("{limit}"),
            "{ unbounded }",
            "Testing default unbounded size limit"
        );
    }

    #[test]
    fn test_result_and() {
        use super::TestResult::{Activated as A, Skipped as S};
        assert_eq!(A(true).and(A(true)), A(true));
        assert_eq!(A(true).and(A(false)), A(false));
        assert_eq!(A(false).and(A(true)), A(false));
        assert_eq!(A(false).and(A(false)), A(false));
        assert_eq!(S.and(A(true)), A(true));
        assert_eq!(S.and(A(false)), A(false));
        assert_eq!(A(true).and(S), A(true));
        assert_eq!(A(false).and(S), A(false));
        assert_eq!(S.and(S), S);
    }

    #[test]
    fn test_result_or() {
        use super::TestResult::{Activated as A, Skipped as S};
        assert_eq!(A(true).or(A(true)), A(true));
        assert_eq!(A(true).or(A(false)), A(true));
        assert_eq!(A(false).or(A(true)), A(true));
        assert_eq!(A(false).or(A(false)), A(false));
        assert_eq!(S.or(A(true)), A(true));
        assert_eq!(S.or(A(false)), A(false));
        assert_eq!(A(true).or(S), A(true));
        assert_eq!(A(false).or(S), A(false));
        assert_eq!(S.or(S), S);
    }

    #[test]
    fn test_result_not() {
        use std::ops::Not;

        use super::TestResult::{Activated as A, Skipped as S};
        assert_eq!(A(true).not(), A(false));
        assert_eq!(A(false).not(), A(true));
        assert_eq!(S.not(), S);
    }
}
