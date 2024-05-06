#[cfg(test)]
mod tests;

use std::fmt;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("values are not sorted or contain duplicates: {0:?}")]
    UnsortedOrDuplicates(Vec<u64>),

    #[error("tried to append ranges that are not consecutive: left ends with {0} and right starts with {1}")]
    NonConsecutive(u64, u64),
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MultiRange {
    // The ranges are inclusive on both ends, non-overlapping, non-adjacent and sorted in ascending order.
    pub ranges: Vec<(u64, u64)>,
}

impl MultiRange {
    pub fn empty() -> Self {
        MultiRange { ranges: Vec::new() }
    }

    pub fn from_values(values: &[u64]) -> Result<Self, Error> {
        let is_sorted = values.windows(2).all(|w| w[0] < w[1]);
        if !is_sorted {
            return Err(Error::UnsortedOrDuplicates(values.to_vec()));
        }

        let mut ranges = Vec::new();

        // Use an iterator to walk through the sorted values and merge them into ranges.
        let mut iter = values.iter().peekable();
        while let Some(&start) = iter.next() {
            let mut end = start;

            // Continue extending the range end as long as consecutive elements are encountered.
            while iter.peek().map_or(false, |&next| *next == end + 1) {
                end = *iter.next().unwrap();
            }

            // Append the current range to the vector of ranges.
            ranges.push((start, end));
        }

        Ok(MultiRange { ranges })
    }

    /// Overlapping ranges will be merged.
    pub fn from_ranges(mut ranges: Vec<(u64, u64)>) -> Self {
        ranges.sort();
        ranges
            .into_iter()
            .fold(MultiRange::empty(), |mut multi_range, range| {
                multi_range.push_merge(range);
                multi_range
            })
    }

    /// If the range is overlapping or adjacent to the last range, it will be merged with it.
    /// Otherwise, it will be added as a new range.
    ///
    /// Panics if the range starts before the last range.
    fn push_merge(&mut self, range: (u64, u64)) {
        let Some(last) = self.ranges.last_mut() else {
            self.ranges.push(range);
            return;
        };

        assert!(last.0 <= range.0, "ranges must be sorted");

        if last.1 >= range.0 + 1 {
            // Merge the ranges.
            last.1 = std::cmp::max(last.1, range.1);
        } else {
            self.ranges.push(range);
        }
    }

    pub fn append(&mut self, mut other: MultiRange) -> Result<(), Error> {
        let Some(our_last) = self.ranges.last().map(|(_, end)| *end) else {
            *self = other;
            return Ok(());
        };

        let Some(their_first) = other.ranges.first().map(|(start, _)| *start) else {
            return Ok(());
        };

        // Check that the first range of `other` is consecutive to the last range of `self`.
        if our_last >= their_first {
            return Err(Error::NonConsecutive(our_last, their_first));
        }

        if our_last + 1 == their_first {
            // Merge the last range of `self` with the first range of `other`.
            let (start, _) = self.ranges.pop().unwrap();
            let (_, end) = other.ranges.remove(0);
            self.ranges.push((start, end));
        }

        self.ranges.append(&mut other.ranges);

        Ok(())
    }

    pub fn contains(&self, value: u64) -> bool {
        // Use binary search to find the range that contains the value.
        self.ranges
            .binary_search_by(|(start, end)| {
                if value < *start {
                    std::cmp::Ordering::Greater
                } else if value > *end {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .is_ok()
    }

    pub fn total_len(&self) -> u64 {
        self.ranges
            .iter()
            .map(|(start, end)| end - start + 1)
            .sum::<u64>()
    }

    pub fn max(&self) -> Option<u64> {
        self.ranges.last().map(|(_, end)| *end)
    }

    pub fn intersection(&self, other: &MultiRange) -> MultiRange {
        let mut intersection = MultiRange::empty();
        let mut i = 0;
        let mut j = 0;

        while i < self.ranges.len() && j < other.ranges.len() {
            let (self_start, self_end) = self.ranges[i];
            let (other_start, other_end) = other.ranges[j];

            // Check for overlap
            if self_end >= other_start && other_end >= self_start {
                // Calculate the intersection
                let start = std::cmp::max(self_start, other_start);
                let end = std::cmp::min(self_end, other_end);
                intersection.ranges.push((start, end));
            }

            // Move to the next range in the list with the smaller endpoint
            if self_end < other_end {
                i += 1;
            } else {
                j += 1;
            }
        }

        intersection
    }

    /// Calculate the complement of the ranges within a specified bounded space.
    pub fn complement(&self, start: u64, end: u64) -> MultiRange {
        let mut complement_ranges = Vec::new();

        let mut next_complement_start = start;
        for &(cur_start, cur_end) in &self.ranges {
            if cur_end < start {
                // Skip ranges that are entirely before the start
                continue;
            }
            if cur_start > end {
                break;
            }

            if cur_start > next_complement_start {
                complement_ranges.push((next_complement_start, cur_start - 1));
            }

            next_complement_start = cur_end + 1;
        }

        // Handle any remaining range after the last specified range
        if next_complement_start <= end {
            complement_ranges.push((next_complement_start, end));
        }

        MultiRange {
            ranges: complement_ranges,
        }
    }

    /// Splits the multirange into at most `n` partitions where each partition is as equal in total
    /// length as possible. If a range exceeds the target partition size, it is split across
    /// partitions. `min_part_len` should be used to prevent partitions from being too small, though
    /// the last partition may still be smaller than that.
    pub fn split_and_partition(&self, n: u64, min_part_len: u64) -> Vec<MultiRange> {
        let total_length = self.total_len();

        if total_length == 0 || n == 0 {
            return vec![];
        }

        let target_part_size = total_length.div_ceil(n).max(min_part_len);
        let mut partitions = Vec::new();
        let mut current_part = MultiRange::empty();

        for &(start, end) in &self.ranges {
            let mut current_start = start;

            while current_start <= end {
                let space_left = target_part_size - current_part.total_len();

                if space_left == 0 {
                    // Finalize the current partition and continue with the next one.
                    partitions.push(current_part);
                    current_part = MultiRange::empty();
                    continue;
                }

                let remaining_len = end - current_start + 1;

                if remaining_len <= space_left {
                    // Add the entire remaining range to the current partition and we're done with
                    // this range.
                    current_part.ranges.push((current_start, end));
                    break;
                } else {
                    // Split the range and effectively finalize the current partition by consuming
                    // all space left.
                    let split_end = current_start + space_left - 1;
                    current_part.ranges.push((current_start, split_end));

                    // Continue with the rest of the range.
                    current_start = split_end + 1;
                }
            }
        }

        // All partitions except the last should respect the minimum partition length.
        assert!(partitions.iter().all(|p| p.total_len() >= min_part_len));

        // Add the last partition if it has any ranges.
        if !current_part.ranges.is_empty() {
            partitions.push(current_part);
        }

        // Correctness checks.
        assert!(partitions.len() as u64 <= n);
        assert!(self.total_len() == partitions.iter().map(|p| p.total_len()).sum::<u64>());

        partitions
    }
}

impl fmt::Display for MultiRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ranges = self
            .ranges
            .iter()
            .map(|&(start, end)| format!("[{}-{}]", start, end))
            .collect::<Vec<_>>()
            .join(", ");

        write!(f, "{}", ranges)
    }
}
