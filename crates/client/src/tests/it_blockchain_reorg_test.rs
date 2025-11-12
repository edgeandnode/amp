//! Reorg scenarios testing invalidation logic.

use crate::{
    scenario,
    tests::utils::{
        Step, assert_data_event, assert_data_event_with_label, assert_undo_event_with_cause,
        assert_undo_event_with_invalidation, assert_watermark_event,
    },
};

/// Tests basic reorg detection when a watermark arrives with different hashes
/// (different epoch) than expected. The reorg should invalidate the affected
/// data batch and emit an Undo event with the correct invalidation range.
#[tokio::test]
async fn reorg_invalidates_affected_batches() {
    let events = scenario! {
        Step::data(0..=10).label("safe"),
        Step::watermark(0..=10),
        Step::data(11..=15).label("reorgd"),
        Step::watermark(11..=12).reorg(vec!["eth"]), // Reorg at block 12
    }
    .await;

    assert_eq!(events.len(), 5);
    assert_data_event_with_label(&events[0], 0, "safe");
    assert_watermark_event(&events[1], 1);
    assert_data_event_with_label(&events[2], 2, "reorgd");
    assert_undo_event_with_cause(&events[3], 3, true);
    assert_watermark_event(&events[4], 4);
}

/// Validates that a deep reorg (going back many blocks) correctly identifies
/// and invalidates all affected batches in the buffer. Tests the buffer
/// traversal logic that walks backwards to find all impacted transaction IDs.
#[tokio::test]
async fn reorg_invalidates_multiple_consecutive_batches() {
    let events = scenario! {
        Step::data(0..=10).label("safe"),
        Step::watermark(0..=10),
        Step::data(11..=20).label("invalid1"),
        Step::data(21..=30).label("invalid2"),
        Step::data(31..=40).label("invalid3"),
        Step::watermark(11..=20).reorg(vec!["eth"]), // Deep reorg back to block 20
    }
    .await;

    assert_eq!(events.len(), 7);
    assert_undo_event_with_invalidation(&events[5], 5, 2..=4);
    assert_watermark_event(&events[6], 6);
}

/// Verifies that reorgs only invalidate batches whose block ranges overlap
/// with the reorg point. Batches finalized before the reorg (protected by
/// earlier watermarks) should remain valid and not be invalidated.
#[tokio::test]
async fn reorg_does_not_invalidate_unaffected_batches() {
    let events = scenario! {
        Step::data(0..=10).label("safe1"),
        Step::watermark(0..=10),
        Step::data(11..=20).label("safe2"),
        Step::watermark(11..=20),
        Step::data(21..=30).label("reorgd"),
        Step::watermark(21..=25).reorg(vec!["eth"]), // Only affects last batch
    }
    .await;

    assert_eq!(events.len(), 7);
    assert_data_event_with_label(&events[0], 0, "safe1");
    assert_watermark_event(&events[1], 1);
    assert_data_event_with_label(&events[2], 2, "safe2");
    assert_watermark_event(&events[3], 3);
    assert_data_event_with_label(&events[4], 4, "reorgd");
    assert_undo_event_with_invalidation(&events[5], 5, 4..=4);
    assert_watermark_event(&events[6], 6);
}

/// Validates partial reorg detection in multi-network scenarios where only
/// some networks experience a reorg while others continue normally. Tests
/// the network-specific reorg detection logic.
#[tokio::test]
async fn multi_network_reorg_partial_invalidation() {
    let events = scenario! {
        Step::data(vec![("eth", 0..=10), ("polygon", 0..=10)]).label("safe"),
        Step::watermark(vec![("eth", 0..=10), ("polygon", 0..=10)]),
        Step::data(vec![("eth", 11..=20), ("polygon", 11..=20)]).label("partial"),
        Step::data(vec![("eth", 11..=15), ("polygon", 11..=20)]).label("reorg").reorg(vec!["eth"]), // Only eth reorgs
        Step::watermark(vec![("eth", 11..=15), ("polygon", 11..=20)]),
    }
    .await;

    assert_eq!(events.len(), 6);
    assert_data_event_with_label(&events[0], 0, "safe");
    assert_watermark_event(&events[1], 1);
    assert_data_event_with_label(&events[2], 2, "partial");
    assert_undo_event_with_invalidation(&events[3], 3, 2..=2);
    assert_data_event_with_label(&events[4], 4, "reorg");
    assert_watermark_event(&events[5], 5);
}

/// Tests handling of multiple consecutive reorgs.
#[tokio::test]
async fn consecutive_reorgs_cumulative_invalidation() {
    let events = scenario! {
        Step::data(0..=10).label("batch1"),          // Emits Data(0)
        Step::watermark(0..=10),                     // Emits Watermark(1)
        Step::data(11..=20).label("batch2"),         // Emits Data(2)
        Step::watermark(11..=20),                    // Emits Watermark(3)
        Step::data(21..=30).label("batch3"),         // Emits Data(4)
        Step::watermark(21..=25).reorg(vec!["eth"]), // Emits Undo(5) and Watermark(6) and invalidates id=4..=4 (jumps back to watermark id=3)
        Step::data(26..=30).label("batch4"),         // Emits Data(7)
        Step::watermark(21..=30).reorg(vec!["eth"]), // Emits Undo(8) and Watermark(9) and invalidates id=4..=7 (jumps back to watermark id=3)
    }
    .await;

    assert_eq!(events.len(), 10);
    assert_data_event(&events[0], 0);
    assert_watermark_event(&events[1], 1);
    assert_data_event(&events[2], 2);
    assert_watermark_event(&events[3], 3);
    assert_data_event(&events[4], 4);
    assert_undo_event_with_invalidation(&events[5], 5, 4..=4);
    assert_watermark_event(&events[6], 6);
    assert_data_event(&events[7], 7);
    assert_undo_event_with_invalidation(&events[8], 8, 4..=7);
    assert_watermark_event(&events[9], 9);
}

/// Tests that reorgs with backwards jumps are properly validated.
///
/// This verifies that the protocol validation correctly handles reorgs
/// that go backwards in block ranges with proper hash chain mismatches.
#[tokio::test]
async fn reorg_with_backwards_jump_succeeds() {
    let events = scenario! {
        Step::data(0..=10).label("safe"),
        Step::watermark(0..=10),
        Step::data(11..=20).label("will_reorg"),
        Step::watermark(15..=25).reorg(vec!["eth"]), // Reorg back to block 15
    }
    .await;

    assert_eq!(events.len(), 5);
    assert_data_event_with_label(&events[0], 0, "safe");
    assert_watermark_event(&events[1], 1);
    assert_data_event_with_label(&events[2], 2, "will_reorg");
    assert_undo_event_with_cause(&events[3], 3, true); // Reorg detected
    assert_watermark_event(&events[4], 4);
}
