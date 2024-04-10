use std::fmt;

#[derive(Clone, Debug, Default)]
pub struct MultiRange {
    // The ranges are inclusive on both ends.
    pub ranges: Vec<(u64, u64)>,
}

impl MultiRange {
    pub fn new(values: &[u64]) -> Result<Self, anyhow::Error> {
        let is_sorted = values.windows(2).all(|w| w[0] < w[1]);
        if !is_sorted {
            return Err(anyhow::anyhow!(
                "values are not sorted or contain duplicates: {:?}",
                values
            ));
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

    pub fn append(&mut self, mut other: MultiRange) -> Result<(), anyhow::Error> {
        let Some(our_last) = self.ranges.last().map(|(_, end)| *end) else {
            *self = other;
            return Ok(());
        };

        let Some(their_first) = other.ranges.first().map(|(start, _)| *start) else {
            return Ok(());
        };

        // Check that the first range of `other` is consecutive to the last range of `self`.
        if our_last >= their_first {
            return Err(anyhow::anyhow!(
                "tried to append ranges that are not consecutive: {} and {}",
                our_last,
                their_first
            ));
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

    pub fn total_len(&self) -> usize {
        self.ranges
            .iter()
            .map(|(start, end)| end - start + 1)
            .sum::<u64>() as usize
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

#[test]
fn test_multi_range() {
    // Test input that is not sorted
    let unsorted_values = vec![3, 1, 2];
    MultiRange::new(&unsorted_values).unwrap_err();

    // Test input with duplicates
    let duplicate_values = vec![1, 2, 2, 3];
    MultiRange::new(&duplicate_values).unwrap_err();

    // Test empty input
    let empty_values = vec![];
    let empty_result = MultiRange::new(&empty_values).unwrap();
    assert!(
        empty_result.ranges.is_empty(),
        "Ranges should be empty for empty input"
    );

    // Test input with consecutive and non-consecutive values, but without any duplicates
    let mixed_values = vec![1, 2, 3, 5, 6, 8];
    let mixed_result = MultiRange::new(&mixed_values).unwrap();
    assert_eq!(
        mixed_result.ranges,
        vec![(1, 3), (5, 6), (8, 8)],
        "Ranges should correctly merge consecutive values and separate non-consecutive values"
    );
    assert!(mixed_result.contains(1));
    assert!(mixed_result.contains(2));
    assert!(mixed_result.contains(3));
    assert!(!mixed_result.contains(4));
    assert!(mixed_result.contains(5));
    assert!(mixed_result.contains(6));
    assert!(!mixed_result.contains(7));
    assert!(mixed_result.contains(8));
}

#[test]
fn test_multi_range_append() {
    let mut range1 = MultiRange::new(&[1, 2, 3, 5, 6, 8]).unwrap();
    let range2 = MultiRange::new(&[10, 11, 12]).unwrap();
    range1.append(range2).unwrap();
    assert_eq!(
        range1.ranges,
        vec![(1, 3), (5, 6), (8, 8), (10, 12)],
        "Ranges should be appended correctly"
    );

    // test merge logic of `append`
    let mut range1 = MultiRange::new(&[1, 2, 3, 5, 6, 8]).unwrap();
    let range2 = MultiRange::new(&[9, 10, 11]).unwrap();
    range1.append(range2).unwrap();
    assert_eq!(
        range1.ranges,
        vec![(1, 3), (5, 6), (8, 11)],
        "Ranges should be merged correctly"
    );
}
