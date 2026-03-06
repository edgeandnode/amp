use std::{
    fmt::{Display, Formatter},
    ops::{Add, AddAssign, Deref},
};

/// Represents the generation of a file, used to track how many times it has been compacted.
/// Each compaction operation increments the generation by 1.
/// A generation of 0 indicates the file is in its original, raw state.
/// This is useful for compaction algorithms that may want to prioritize
/// or treat files differently based on their generation.
///
/// # Examples
/// ```
/// # use amp_parquet::generation::Generation;
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Generation(u64);

impl Generation {
    /// Returns `true` if this file has never been compacted (generation 0).
    pub fn is_raw(&self) -> bool {
        self.0 == 0
    }

    /// Returns `true` if this file has been through at least one compaction pass.
    pub fn is_compacted(&self) -> bool {
        self.0 > 0
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

impl Deref for Generation {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
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

impl From<Generation> for u64 {
    fn from(val: Generation) -> Self {
        val.0
    }
}
