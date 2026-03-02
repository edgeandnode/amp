use std::sync::Arc;

use crate::{
    BlockRange,
    physical_table::{resolved::ResolvedFile, segments::Segment, table::PhysicalTable},
};

/// A segment-resolved view of a table.
///
/// Exposes both segment-level data (for streaming query and compaction) and a
/// resolved file list (for the execution layer).
#[derive(Debug, Clone)]
pub struct TableSnapshot {
    physical_table: Arc<PhysicalTable>,
    canonical_segments: Vec<Segment>,
}

impl TableSnapshot {
    pub fn new(physical_table: Arc<PhysicalTable>, canonical_segments: Vec<Segment>) -> Self {
        Self {
            physical_table,
            canonical_segments,
        }
    }

    // Segment-level access (for streaming query, compaction)

    pub fn canonical_segments(&self) -> &[Segment] {
        &self.canonical_segments
    }

    /// Return the block range to use for query execution over this table.
    ///
    /// Returns `Ok(None)` if no block range has been synced (empty table).
    ///
    /// # Errors
    /// Returns [`MultiNetworkSegmentsError`] if any segment does not have exactly
    /// one block range, or if segments belong to different networks. Both cases
    /// indicate an inconsistent catalog state. Multi-network materialized segments
    /// are not yet supported; all segments must be single-range and share the same
    /// network identity.
    pub fn synced_range(&self) -> Result<Option<BlockRange>, MultiNetworkSegmentsError> {
        compute_synced_range(&self.canonical_segments)
    }

    pub fn physical_table(&self) -> &Arc<PhysicalTable> {
        &self.physical_table
    }

    // Resolved file access (for execution layer)

    pub fn resolved_files(&self) -> Vec<ResolvedFile> {
        self.canonical_segments
            .iter()
            .map(|s| ResolvedFile {
                id: s.id(),
                object: s.object().clone(),
            })
            .collect()
    }

    pub fn file_count(&self) -> usize {
        self.canonical_segments.len()
    }

    pub fn has_data(&self) -> bool {
        self.synced_range().is_ok_and(|r| r.is_some())
    }
}

/// Error returned when [`TableSnapshot::synced_range`] is called on a snapshot
/// containing multi-network or inconsistently-networked segments.
///
/// `synced_range()` constructs a single-network [`BlockRange`] and therefore
/// requires all segments to have exactly one block range and all ranges to
/// share the same network identity. This error acts as an explicit guard:
/// multi-network materialized segments are not yet supported, and returning
/// this error is safer than panicking via `Segment::single_range()` or
/// silently constructing a `BlockRange` with mixed network identity.
///
/// When multi-network materialization is eventually enabled, `synced_range()`
/// (or a replacement API) must be extended to return one range per network.
#[derive(Debug, thiserror::Error)]
#[error(
    "synced_range() is not supported for multi-network segments; \
     each segment must have exactly one block range for the same network"
)]
pub struct MultiNetworkSegmentsError;

/// Compute the synced range from a slice of canonical segments.
///
/// Extracted from [`TableSnapshot::synced_range`] so that the guard logic can be unit-tested
/// without constructing a full [`TableSnapshot`] (which requires a [`PhysicalTable`]).
fn compute_synced_range(
    segments: &[Segment],
) -> Result<Option<BlockRange>, MultiNetworkSegmentsError> {
    // Guard 1: every segment must have exactly one range. Multi-network
    // materialized segments are not yet enabled; return an error rather than
    // panicking via `Segment::single_range()` if they appear.
    if segments.iter().any(|s| s.ranges().len() != 1) {
        return Err(MultiNetworkSegmentsError);
    }

    // Guard 2: all single ranges must belong to the same network.
    // If mixed-network segments appear (e.g. via snapshot(true)/fork-context
    // flows or corrupted metadata), constructing a BlockRange would silently
    // mix network identity from different segments.
    if segments
        .windows(2)
        .any(|w| w[0].ranges()[0].network != w[1].ranges()[0].network)
    {
        return Err(MultiNetworkSegmentsError);
    }

    // SAFETY: every segment has exactly 1 range (verified above), so
    // `ranges()[0]` is always in bounds.
    let Some(start_seg) = segments.iter().min_by_key(|s| s.ranges()[0].start()) else {
        return Ok(None);
    };
    let Some(end_seg) = segments.iter().max_by_key(|s| s.ranges()[0].end()) else {
        return Ok(None);
    };

    let start = &start_seg.ranges()[0];
    let end = &end_seg.ranges()[0];

    Ok(Some(BlockRange {
        network: start.network.clone(),
        numbers: start.start()..=end.end(),
        hash: end.hash,
        prev_hash: start.prev_hash,
        timestamp: end.timestamp,
    }))
}

#[cfg(test)]
mod tests {
    use alloy::primitives::BlockHash;
    use metadata_db::files::FileId;
    use object_store::ObjectMeta;

    use super::{BlockRange, MultiNetworkSegmentsError, Segment, compute_synced_range};

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    fn test_hash(byte: u8) -> BlockHash {
        let mut h = BlockHash::default();
        h.0[0] = byte;
        h
    }

    fn test_object() -> ObjectMeta {
        use chrono::DateTime;
        ObjectMeta {
            location: Default::default(),
            last_modified: DateTime::from_timestamp_millis(0)
                .expect("zero is a valid millisecond timestamp"),
            size: 0,
            e_tag: None,
            version: None,
        }
    }

    fn single_range_segment(network: &str, start: u64, end: u64) -> Segment {
        let range = BlockRange {
            numbers: start..=end,
            network: network.parse().expect("valid network id"),
            hash: test_hash(end as u8),
            prev_hash: test_hash(start as u8),
            timestamp: None,
        };
        Segment::new(
            FileId::try_from(1i64).expect("FileId 1 is valid"),
            test_object(),
            vec![range],
            None,
        )
    }

    fn multi_range_segment(network_a: &str, network_b: &str, start: u64, end: u64) -> Segment {
        let range_a = BlockRange {
            numbers: start..=end,
            network: network_a.parse().expect("valid network id"),
            hash: test_hash(end as u8),
            prev_hash: test_hash(start as u8),
            timestamp: None,
        };
        let range_b = BlockRange {
            numbers: start..=end,
            network: network_b.parse().expect("valid network id"),
            hash: test_hash(end as u8),
            prev_hash: test_hash(start as u8),
            timestamp: None,
        };
        Segment::new(
            FileId::try_from(1i64).expect("FileId 1 is valid"),
            test_object(),
            vec![range_a, range_b],
            None,
        )
    }

    // ---------------------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------------------

    /// Empty segment list returns Ok(None) — table has no synced data.
    #[test]
    fn empty_segments_returns_none() {
        // Given: no segments
        let segments: Vec<Segment> = vec![];

        // When: computing the synced range
        let result = compute_synced_range(&segments);

        // Then: Ok(None) — table is empty, not an error
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    /// A single valid single-range single-network segment produces Ok(Some(range)).
    #[test]
    fn single_valid_segment_returns_range() {
        // Given: one segment for network "mainnet", blocks 0..=99
        let segments = vec![single_range_segment("mainnet", 0, 99)];

        // When: computing the synced range
        let result = compute_synced_range(&segments);

        // Then: Ok(Some(_)) with correct start and end
        let range = result
            .expect("single valid segment should not error")
            .expect("should have data");
        assert_eq!(*range.numbers.start(), 0);
        assert_eq!(*range.numbers.end(), 99);
    }

    /// Multiple valid segments on the same network returns the spanning range.
    #[test]
    fn multiple_same_network_segments_returns_spanning_range() {
        // Given: two contiguous segments for the same network
        let segments = vec![
            single_range_segment("mainnet", 0, 49),
            single_range_segment("mainnet", 50, 99),
        ];

        // When: computing the synced range
        let result = compute_synced_range(&segments);

        // Then: Ok(Some(_)) spanning blocks 0..=99
        let range = result
            .expect("same-network segments should not error")
            .expect("should have data");
        assert_eq!(*range.numbers.start(), 0);
        assert_eq!(*range.numbers.end(), 99);
    }

    /// A segment with more than one block range triggers MultiNetworkSegmentsError (guard 1).
    #[test]
    fn multi_range_segment_returns_error() {
        // Given: one segment with ranges for two networks
        let segments = vec![multi_range_segment("mainnet", "testnet", 0, 99)];

        // When: computing the synced range
        let result = compute_synced_range(&segments);

        // Then: Err — multi-range segments are not supported
        assert!(
            matches!(result, Err(MultiNetworkSegmentsError)),
            "expected MultiNetworkSegmentsError, got: {:?}",
            result
        );
    }

    /// Segments from different networks trigger MultiNetworkSegmentsError (guard 2).
    #[test]
    fn mixed_network_segments_returns_error() {
        // Given: two single-range segments for different networks
        let segments = vec![
            single_range_segment("mainnet", 0, 49),
            single_range_segment("testnet", 50, 99),
        ];

        // When: computing the synced range
        let result = compute_synced_range(&segments);

        // Then: Err — mixed-network segments are not supported
        assert!(
            matches!(result, Err(MultiNetworkSegmentsError)),
            "expected MultiNetworkSegmentsError, got: {:?}",
            result
        );
    }
}
