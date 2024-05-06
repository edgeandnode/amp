use super::*;

#[test]
fn test_multi_range() {
    // Test input that is not sorted
    let unsorted_values = vec![3, 1, 2];
    MultiRange::from_values(&unsorted_values).unwrap_err();

    // Test input with duplicates
    let duplicate_values = vec![1, 2, 2, 3];
    MultiRange::from_values(&duplicate_values).unwrap_err();

    // Test empty input
    let empty_values = vec![];
    let empty_result = MultiRange::from_values(&empty_values).unwrap();
    assert!(
        empty_result.ranges.is_empty(),
        "Ranges should be empty for empty input"
    );

    // Test input with consecutive and non-consecutive values, but without any duplicates
    let mixed_values = vec![1, 2, 3, 5, 6, 8];
    let mixed_result = MultiRange::from_values(&mixed_values).unwrap();
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
    let mut range1 = MultiRange::from_values(&[1, 2, 3, 5, 6, 8]).unwrap();
    let range2 = MultiRange::from_values(&[10, 11, 12]).unwrap();
    range1.append(range2).unwrap();
    assert_eq!(
        range1.ranges,
        vec![(1, 3), (5, 6), (8, 8), (10, 12)],
        "Ranges should be appended correctly"
    );

    // test merge logic of `append`
    let mut range1 = MultiRange::from_values(&[1, 2, 3, 5, 6, 8]).unwrap();
    let range2 = MultiRange::from_values(&[9, 10, 11]).unwrap();
    range1.append(range2).unwrap();
    assert_eq!(
        range1.ranges,
        vec![(1, 3), (5, 6), (8, 11)],
        "Ranges should be merged correctly"
    );
}

#[test]
fn test_multi_range_intersect() {
    // intersects and asserts commutativity
    fn intersect(
        range1: impl IntoIterator<Item = (u64, u64)>,
        range2: impl IntoIterator<Item = (u64, u64)>,
    ) -> MultiRange {
        let range1 = MultiRange::from_ranges(range1.into_iter().collect());
        let range2 = MultiRange::from_ranges(range2.into_iter().collect());
        let intersect = range1.intersection(&range2);
        let intersect2 = range2.intersection(&range1);
        assert_eq!(intersect.ranges, intersect2.ranges);
        intersect
    }

    let range1 = [(1, 3), (10, 12)];
    let range2 = [(2, 5), (10, 11)];
    let intersect_result = intersect(range1, range2);

    // Check that the intersect method correctly identifies overlaps
    assert_eq!(
        intersect_result.ranges,
        [(2, 3), (10, 11)],
        "Ranges should intersect correctly for partial and complete overlaps"
    );

    let range3 = [(4, 5), (13, 14)];
    let non_overlap_result = intersect(range1, range3);

    // Check for no intersection
    assert!(
        non_overlap_result.ranges.is_empty(),
        "Ranges should be empty if there is no overlap"
    );

    // Edge cases where ranges just touch each other
    let range4 = [(12, 12)];
    let edge_touch_result = intersect(range1, range4);
    assert_eq!(
        edge_touch_result.ranges,
        vec![(12, 12)],
        "Ranges should include the single point of overlap"
    );
}

#[test]
fn test_multi_range_complement() {
    // Complement helper function
    fn complement(
        ranges: impl IntoIterator<Item = (u64, u64)>,
        start: u64,
        end: u64,
    ) -> MultiRange {
        let multi_range = MultiRange::from_ranges(ranges.into_iter().collect());
        multi_range.complement(start, end)
    }

    // Test full complement within a given range
    let ranges = [(5, 10), (15, 20)];
    let complement_result = complement(ranges, 1, 25);
    assert_eq!(
        complement_result.ranges,
        [(1, 4), (11, 14), (21, 25)],
        "Complement should correctly identify all non-covered ranges within the specified bounds."
    );

    // Test with no ranges provided
    let empty_ranges = [];
    let empty_complement = complement(empty_ranges, 1, 25);
    assert_eq!(
        empty_complement.ranges,
        [(1, 25)],
        "Complement of an empty range set should cover the entire bounded space."
    );

    // Test with range extending beyond the bounds
    let overextended_ranges = [(0, 5), (10, 30)];
    let overextended_complement = complement(overextended_ranges, 1, 25);
    assert_eq!(
        overextended_complement.ranges,
        [(6, 9)],
        "Complement should correctly handle ranges extending beyond the specified bounds."
    );

    // Edge case with range just at the boundaries
    let edge_ranges = [(1, 1), (25, 25)];
    let edge_complement = complement(edge_ranges, 1, 25);
    assert_eq!(
        edge_complement.ranges,
        [(2, 24)],
        "Should handle edge bounds correctly, excluding the start and end points if covered."
    );
}

/// Asserts that two sets of partitions (one a Vec of Vec of tuples, one a Vec of MultiRange) are equal.
fn assert_parts(actual: Vec<MultiRange>, expected: Vec<Vec<(u64, u64)>>) {
    assert_eq!(
        expected.len(),
        actual.len(),
        "Number of partitions does not match"
    );

    for (exp_partition, act_partition) in expected.iter().zip(actual.iter()) {
        assert_eq!(
            exp_partition, &act_partition.ranges,
            "Partition contents do not match"
        );
    }
}

#[test]
fn test_multi_range_split_single_range() {
    let empty_mr = MultiRange { ranges: vec![] };
    let partitions_empty = empty_mr.split_and_partition(5, 10);
    assert_eq!(partitions_empty.len(), 0);

    let small_mr = MultiRange {
        ranges: vec![(1, 1)],
    };
    let partitions_small = small_mr.split_and_partition(1, 1);
    assert_parts(partitions_small, vec![vec![(1, 1)]]);

    let one_to_a_hundred = MultiRange {
        ranges: vec![(1, 100)],
    };

    let partitions_large_inexact = one_to_a_hundred.split_and_partition(3, 30);
    assert_parts(
        partitions_large_inexact,
        vec![vec![(1, 34)], vec![(35, 68)], vec![(69, 100)]],
    );

    // Test basic splitting into 2 and 10 partitions
    let partitions = one_to_a_hundred.split_and_partition(2, 5);
    assert_parts(partitions, vec![vec![(1, 50)], vec![(51, 100)]]);

    let partitions = one_to_a_hundred.split_and_partition(10, 5);
    assert_parts(
        partitions,
        vec![
            vec![(1, 10)],
            vec![(11, 20)],
            vec![(21, 30)],
            vec![(31, 40)],
            vec![(41, 50)],
            vec![(51, 60)],
            vec![(61, 70)],
            vec![(71, 80)],
            vec![(81, 90)],
            vec![(91, 100)],
        ],
    );

    // Test enforcement of min partition size
    let partitions_edge = one_to_a_hundred.split_and_partition(10, 15);
    assert_parts(
        partitions_edge,
        vec![
            vec![(1, 15)],
            vec![(16, 30)],
            vec![(31, 45)],
            vec![(46, 60)],
            vec![(61, 75)],
            vec![(76, 90)],
            vec![(91, 100)],
        ],
    );
}

#[test]
fn test_multi_range_split_and_partition_multiple_ranges() {
    let mr = MultiRange::from_ranges(vec![(1, 1), (3, 10), (15, 45), (50, 100)]);

    // Requesting zero partitions
    assert!(mr.split_and_partition(0, 5).is_empty());

    // 2 partitions
    let large_partition = mr.split_and_partition(2, 20);
    let expected_partitions_two = vec![vec![(1, 1), (3, 10), (15, 45), (50, 55)], vec![(56, 100)]];
    assert_parts(large_partition, expected_partitions_two);

    // 5 partitions
    let partitions = mr.split_and_partition(5, 5);
    let expected_partitions_five = vec![
        vec![(1, 1), (3, 10), (15, 24)],
        vec![(25, 43)],
        vec![(44, 45), (50, 66)],
        vec![(67, 85)],
        vec![(86, 100)],
    ];
    assert_parts(partitions, expected_partitions_five);

    // 3 partitions
    let exact_partitions = mr.split_and_partition(3, 10);
    let expected_partitions_three = vec![
        vec![(1, 1), (3, 10), (15, 36)],
        vec![(37, 45), (50, 71)],
        vec![(72, 100)],
    ];
    assert_parts(exact_partitions, expected_partitions_three);

    // 10 partitions
    let split_check = mr.split_and_partition(10, 1);
    let expected_partitions_ten = vec![
        vec![(1, 1), (3, 10), (15, 15)],
        vec![(16, 25)],
        vec![(26, 35)],
        vec![(36, 45)],
        vec![(50, 59)],
        vec![(60, 69)],
        vec![(70, 79)],
        vec![(80, 89)],
        vec![(90, 99)],
        vec![(100, 100)],
    ];
    assert_parts(split_check, expected_partitions_ten);

    // Request 10 partitions, but enforcing minimum partition size
    let split_check = mr.split_and_partition(10, 20);
    let expected_partitions_ten = vec![
        vec![(1, 1), (3, 10), (15, 25)],
        vec![(26, 45)],
        vec![(50, 69)],
        vec![(70, 89)],
        vec![(90, 100)],
    ];
    assert_parts(split_check, expected_partitions_ten);
}
