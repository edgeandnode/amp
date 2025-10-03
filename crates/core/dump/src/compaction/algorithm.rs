use std::{
    fmt::{Debug, Display, Formatter},
    ops::{Deref, Not},
    time::Duration,
};

use common::{
    Timestamp,
    config::{ParquetConfig, SizeLimitConfig},
    metadata::{Generation, Overflow, SegmentSize},
};

use crate::compaction::{compactor::CompactionGroup, plan::CompactionFile};

/// Compaction algorithm parameters.
/// Defines the criteria for grouping files for compaction
/// based on their size and age.
///
/// ## Fields
/// - `base_duration`: The base duration used to calculate
///   the cooldown period for files based on their generation.
/// - `upper_bound`: The upper bound for segment size limits.
///   Files exceeding this limit will not be compacted together. This
///   value must be non-unbounded.
/// - `lower_bound`: The lower bound for segment size limits. This
///   value can be unbounded, indicating no lower limit for compaction.
///
/// ## Logic
///
/// ### Eager Compaction
/// > If the lower bound is set to >= the upper bound, all files are considered `Live`,
/// > and will be compacted together as long as they do not exceed the upper bound
/// > size limits, regardless of their generation or age.
///
/// ### Exponential Compaction
/// > If the lower bound is unbounded, and the base duration for cooldown
/// > is set to zero, all files are considered `Hot`, and will be compacted
/// > together as long as they do not exceed the upper bound size limits and
/// > share the same generation.
///
/// ### Relaxed Exponential Compaction
/// > If the lower bound is unbounded, and the base duration for cooldown
/// > is set to a non-zero value, files are considered `Hot` or `Cold`
/// > based on their age and the cooldown period. As long as the total size
/// > of the candidate group does not exceed the upper bound, `Hot` files
/// > will only be compacted with other files of the same generation, while
/// > `Cold` files can be compacted regardless of generation.
///
/// ### Hybrid Compaction
/// > If the lower bound is set to a non-unbounded value that is less than
/// > the upper bound, and the base duration for cooldown is set to a non-zero
/// > value, files are considered `Live`, `Hot`, or `Cold` based on their
/// > size and age. As long as the total size of the candidate group does not
/// > exceed the upper bound, `Live` files will be compacted together if
/// > they do not exceed the lower bound limits, `Hot` files will only be
/// > compacted with other files of the same generation, and `Cold` files
/// > can be compacted regardless of generation.
#[derive(Clone, Copy)]
pub struct CompactionAlgorithm {
    pub base_cooldown: Duration,
    pub upper_bound: SegmentSizeLimit,
    pub lower_bound: SegmentSizeLimit,
}

impl CompactionAlgorithm {
    pub fn kind(&self) -> &'static str {
        if self.lower_bound.0 >= self.upper_bound.0 {
            return "Eager Compaction";
        }

        let unbounded_lower = self.lower_bound.is_unbounded();

        if unbounded_lower && self.base_cooldown.is_zero() {
            "Exponential Compaction"
        } else if unbounded_lower {
            "Relaxed Exponential Compaction"
        } else {
            "Hybrid Compaction"
        }
    }

    fn is_live(&self, segment: &SegmentSize) -> TestResult {
        self.lower_bound.is_live(segment)
    }

    fn is_hot(&self, segment: &SegmentSize) -> TestResult {
        Cooldown::new(self.base_cooldown, segment.generation).is_hot(segment.created_at)
    }

    /// Determines the state of a file based on its size and age.
    /// - `Live`: The file is within the lower bound limits, if any.
    /// - `Hot`: The file has exceeded lower bound limits (if any)
    ///   but is still within its cooldown period.
    /// - `Cold`: The file has exceeded lower bound limits and is outside
    ///   its cooldown period.
    fn file_state(&self, segment: &SegmentSize) -> FileState {
        let is_live = self.is_live(segment);

        if *is_live {
            return FileState::Live;
        }

        let is_hot = self.is_hot(segment);

        match is_hot {
            TestResult::Skipped => FileState::Hot,
            TestResult::Activated(true) => FileState::Hot,
            TestResult::Activated(false) => FileState::Cold,
        }
    }

    /// Predicate function to determine if two files can be compacted together.
    ///
    /// Returns `true` if the candidate file can be compacted with the group,
    /// `false` otherwise. The decision is based on the combined size of the
    /// files, their states (Live, Hot, Cold), and their generations.
    pub fn predicate(&self, group: &CompactionGroup, candidate: &CompactionFile) -> bool {
        let state = self
            .file_state(&group.size)
            .max(self.file_state(&candidate.size));

        // Check if combining sizes exceeds upper bound.
        let (size_exceeded, length_exceeded, _) =
            self.upper_bound.is_exceeded(&(candidate.size + group.size));

        if state == FileState::Live {
            // For live files, only compact if size limit is not exceeded.
            // If it's the tail file, also require length limit to be exceeded
            // (for cases where a minimum number of segments is desired before compaction).
            *size_exceeded && (!candidate.is_tail || *length_exceeded)
        } else if state == FileState::Hot {
            // For hot files, only compact if size limit is not exceeded,
            // and both files share the same generation.
            group.size.generation == candidate.size.generation && !*size_exceeded
        } else {
            // For cold files, compact regardless of generation,
            // as long as size limit is not exceeded.
            !*size_exceeded
        }
    }
}

impl Debug for CompactionAlgorithm {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(self.kind())
            .field("base_duration", &self.base_cooldown)
            .field("upper_bound", &self.upper_bound)
            .field("lower_bound", &self.lower_bound)
            .finish()
    }
}

impl Display for CompactionAlgorithm {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let kind = self.kind().trim_end_matches(" Compaction");
        match kind {
            "Eager" => write!(f, "{{ kind: {}, upper_bound: {} }}", kind, self.upper_bound),
            "Exponential" => write!(f, "{{ kind: {}, upper_bound: {} }}", kind, self.upper_bound),
            "Relaxed Exponential" => write!(
                f,
                "{{ kind: {}, base_duration: {:?}, upper_bound: {} }}",
                kind, self.base_cooldown, self.upper_bound
            ),
            "Hybrid" => write!(
                f,
                "{{ kind: {}, base_duration: {:?}, upper_bound: {}, lower_bound: {} }}",
                kind, self.base_cooldown, self.upper_bound, self.lower_bound
            ),
            _ => unreachable!("Unexpected compaction algorithm kind"),
        }
    }
}

impl<'a> From<&'a ParquetConfig> for CompactionAlgorithm {
    fn from(config: &'a ParquetConfig) -> Self {
        CompactionAlgorithm {
            base_cooldown: config
                .compactor
                .algorithm
                .base_cooldown_duration
                .unwrap_or(Duration::from_secs(2)),
            upper_bound: SegmentSizeLimit::from(&config.target_size),
            lower_bound: SegmentSizeLimit::from(&config.compactor.algorithm.eager_compaction_limit),
        }
    }
}

/// Cooldown period for file compaction, as a function of the
/// generation of the file. Before the period elapses, the file
/// will only be compacted if the candidate group shares the
/// same generation.
///
/// The cooldown period is calculated as:
/// `base_duration * generation`
/// # Examples
/// ```rust
/// use std::time::Duration;
/// use dump::compaction::algorithm::Cooldown;
/// use dump::compaction::Generation;
/// let cooldown = Cooldown::new(Duration::from_secs(5), 3);
/// assert_eq!(cooldown.as_duration(), Duration::from_secs(15));
/// let cooldown_raw = Cooldown::new(Duration::from_secs(500), Generation::default());
/// assert_eq!(cooldown_raw.as_duration(), Duration::from_secs(0));
#[derive(Clone, Copy)]
pub struct Cooldown(Duration, Generation);

impl Cooldown {
    pub fn new(base: Duration, generation: impl Into<Generation>) -> Self {
        Self(base, generation.into())
    }

    /// Returns the cooldown period as a `Duration` by
    /// multiplying the base duration by the dereferenced generation.
    ///
    /// See the [`Cooldown`] documentation for details and examples.
    pub fn as_duration(&self) -> Duration {
        Duration::from_micros(
            self.0
                .as_micros()
                .try_into()
                .unwrap_or(u64::MAX)
                .saturating_mul(*self.1),
        )
    }

    pub(super) fn is_hot(&self, created_at: u128) -> TestResult {
        if self.0.is_zero() {
            TestResult::Skipped
        } else {
            let now = Timestamp::now();
            TestResult::Activated(
                created_at.saturating_add(self.as_duration().as_micros()) > now.0.as_micros(),
            )
        }
    }
}

impl Debug for Cooldown {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Cooldown({:?} @ {})", self.as_duration(), self.1)
    }
}

impl Display for Cooldown {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.as_duration())
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

impl Ord for FileState {
    /// Defines a partial ordering for `FileState`:
    /// - `Cold` is greater than both `Live` and `Hot`.
    /// - `Hot` is greater than `Live` but less than `Cold`.
    /// - `Live` is less than both `Hot` and `Cold`.
    /// - States of the same type are considered equal.
    ///
    /// This ordering is used to determine the strictest state
    /// when comparing two files, where `Cold` is the strictest
    /// and `Live` is the least strict.
    ///
    /// # Examples
    /// ```
    /// use dump::compaction::algorithm::FileState;
    /// assert!(FileState::Cold > FileState::Hot);
    /// assert!(FileState::Cold > FileState::Live);
    /// assert!(FileState::Hot > FileState::Live);
    /// assert!(FileState::Hot < FileState::Cold);
    /// assert!(FileState::Live < FileState::Hot);
    /// assert!(FileState::Live < FileState::Cold);
    /// assert_eq!(FileState::Cold, FileState::Cold);
    /// assert_eq!(FileState::Hot, FileState::Hot);
    /// assert_eq!(FileState::Live, FileState::Live);
    /// ```
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (FileState::Cold, FileState::Cold)
            | (FileState::Live, FileState::Live)
            | (FileState::Hot, FileState::Hot) => std::cmp::Ordering::Equal,
            (FileState::Cold, _) => std::cmp::Ordering::Greater,
            (_, FileState::Cold) => std::cmp::Ordering::Less,
            (FileState::Hot, _) => std::cmp::Ordering::Greater,
            (_, FileState::Hot) => std::cmp::Ordering::Less,
        }
    }
}

impl PartialOrd for FileState {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
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
    /// [`SizeCheckResult`] - A tuple of three [`TestResult`] values:
    /// 1. Combined result for blocks, bytes, and rows dimensions
    /// 2. Result for the length (file count) dimension
    /// 3. Result for the generation dimension
    pub fn is_exceeded(&self, segment: &SegmentSize) -> SizeCheckResult {
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
            .then_some(segment.generation.ge(&self.1.soft_limit(self.0.generation)))
            .into();

        let length_ge: TestResult = self
            .0
            .length
            .ne(&0)
            .then_some(segment.length.ge(&self.1.soft_limit(self.0.length)))
            .into();

        (
            blocks_ge.or(bytes_ge).or(rows_ge),
            length_ge,
            generation_ge,
        )
    }

    pub fn is_live(&self, segment: &SegmentSize) -> TestResult {
        let (size_exceeded, length_exceeded, generation_exceeded) =
            Self::is_exceeded(self, segment);

        // A segment is considered live if it does not exceed size limits,
        // length limits, and generation limits (if any are set).
        size_exceeded
            .or(length_exceeded)
            .or(generation_exceeded)
            .not()
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
            value.blocks as i64,
            value.bytes as i64,
            value.rows as i64,
            value.file_count as usize,
            value.generation,
            value.overflow,
        )
    }
}

/// The result of checking if a segment exceeds size limits.
///
/// This is a tuple of three [`TestResult`] values:
/// 1. Combined result for blocks, bytes, and rows dimensions
/// 2. Result for the length (file count) dimension
/// 3. Result for the generation dimension
pub type SizeCheckResult = (TestResult, TestResult, TestResult);

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

impl Deref for TestResult {
    type Target = bool;

    fn deref(&self) -> &Self::Target {
        match self {
            TestResult::Activated(a) => a,
            TestResult::Skipped => &false,
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

    #[test]
    fn test_result_deref() {
        use super::TestResult::{Activated as A, Skipped as S};
        assert!(*A(true));
        assert!(!*A(false));
        assert!(!*S);
    }
}
