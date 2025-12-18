//! Happy path streaming scenarios testing state transitions.

use crate::tests::utils::{
    Step, assert_data_event_with_label, assert_watermark_event, assert_watermark_event_with_prune,
};

/// Verifies that a basic stream with one data batch followed by a watermark
/// correctly advances transaction IDs and produces the expected sequence of events.
/// Tests the fundamental happy path of data → watermark progression.
#[tokio::test]
async fn data_watermark_advances_state() {
    let events = scenario! {
        Step::data(0..=10).label("d1"),
        Step::watermark(0..=10),
    }
    .await;

    assert_eq!(events.len(), 2);
    assert_data_event_with_label(&events[0], 0, "d1");
    assert_watermark_event(&events[1], 1);
}

/// Validates that multiple data batches received before a watermark are all
/// stored in the buffer and assigned sequential transaction IDs. This tests
/// the buffering mechanism that enables reorg detection across multiple batches.
#[tokio::test]
async fn multiple_data_before_watermark_builds_buffer() {
    let events = scenario! {
        Step::data(0..=10).label("d1"),
        Step::data(11..=20).label("d2"),
        Step::data(21..=30).label("d3"),
        Step::watermark(21..=30),
    }
    .await;

    assert_eq!(events.len(), 4);
    assert_data_event_with_label(&events[0], 0, "d1");
    assert_data_event_with_label(&events[1], 1, "d2");
    assert_data_event_with_label(&events[2], 2, "d3");
    assert_watermark_event(&events[3], 3);
}

/// Tests that the retention window correctly prunes old batches from the buffer
/// when a watermark is received that is beyond the retention threshold. Verifies
/// that the cutoff ID is calculated correctly based on block ranges and retention.
///
/// Retention=10 means batches are kept if their end block is within 10 blocks of
/// the watermark's end block. When watermark reaches block 25, anything ending
/// before block 15 (25-10) should be pruned.
#[tokio::test]
async fn buffer_pruning_triggered_by_retention() {
    let events = scenario! {
        retention: 10;

        Step::data(0..=5).label("batch1"),    // id=0, end=5, will be pruned when watermark hits 25
        Step::watermark(0..=5),               // id=1, end=5, will be pruned when watermark hits 25
        Step::data(6..=10).label("batch2"),   // id=2, end=10, will be pruned when watermark hits 25
        Step::watermark(6..=10),              // id=3, end=10, will be pruned when watermark hits 25
        Step::data(11..=20).label("batch3"),  // id=4, end=20, will be kept (20 >= 15)
        Step::watermark(11..=20),             // id=5, end=20, will be kept (20 >= 15)
        Step::data(21..=25).label("batch4"),  // id=6, end=25, will be kept (25 >= 15)
        Step::watermark(21..=25),             // id=7, end=25, triggers pruning (cutoff=15)
    }
    .await;

    assert_eq!(events.len(), 8);
    assert_data_event_with_label(&events[0], 0, "batch1");
    assert_watermark_event(&events[1], 1);
    assert_data_event_with_label(&events[2], 2, "batch2");
    assert_watermark_event(&events[3], 3);
    assert_data_event_with_label(&events[4], 4, "batch3");
    assert_watermark_event(&events[5], 5);
    assert_data_event_with_label(&events[6], 6, "batch4");
    // Watermarks 1 and 3 have end blocks < 15, should be pruned (last to prune is 3)
    assert_watermark_event_with_prune(&events[7], 7, Some(3));
}

/// Tests that no pruning occurs when all batches are within the retention window.
/// Verifies that cutoff remains None when nothing needs to be pruned.
#[tokio::test]
async fn no_pruning_when_within_retention() {
    let events = scenario! {
        retention: 20;

        Step::data(0..=10).label("batch1"),
        Step::watermark(0..=10),
        Step::data(11..=15).label("batch2"),
        Step::watermark(11..=15),
    }
    .await;

    assert_eq!(events.len(), 4);
    assert_data_event_with_label(&events[0], 0, "batch1");
    assert_watermark_event(&events[1], 1);
    assert_data_event_with_label(&events[2], 2, "batch2");
    // Cutoff=15-20=-5 (saturates to 0), so all watermarks kept, prune should be None
    assert_watermark_event_with_prune(&events[3], 3, None);
}

/// Tests gradual pruning as the stream progresses and more batches fall outside
/// the retention window. Verifies that cutoff advances correctly with each watermark.
#[tokio::test]
async fn gradual_pruning_with_advancing_watermark() {
    let events = scenario! {
        retention: 5;

        Step::data(0..=5).label("batch1"),
        Step::watermark(0..=5),     // cutoff = 5-5=0, keep all (cutoff=None)
        Step::data(6..=10).label("batch2"),
        Step::watermark(6..=10),    // cutoff = 10-5=5, prune watermarks ending < 5 (none yet, cutoff=None)
        Step::data(11..=15).label("batch3"),
        Step::watermark(11..=15),   // cutoff = 15-5=10, prune watermarks ending < 10 (IDs 0,1 end at 5, cutoff=Some(2))
    }
    .await;

    assert_eq!(events.len(), 6);
    assert_data_event_with_label(&events[0], 0, "batch1");
    assert_watermark_event_with_prune(&events[1], 1, None); // Nothing to prune yet
    assert_data_event_with_label(&events[2], 2, "batch2");
    assert_watermark_event_with_prune(&events[3], 3, None); // Still nothing to prune (end=10 >= cutoff=5)
    assert_data_event_with_label(&events[4], 4, "batch3");
    assert_watermark_event_with_prune(&events[5], 5, Some(1)); // Prune watermark 1 (end=5 < cutoff=10)
}

/// Verifies that the stream correctly handles multiple blockchain networks
/// (e.g., Ethereum and Polygon) with independent block ranges, ensuring
/// each network's ranges are tracked separately in the watermark.
#[tokio::test]
async fn multi_network_tracks_independent_ranges() {
    let events = scenario! {
        Step::data(vec![("eth", 0..=10), ("polygon", 0..=5)]).label("d1"),
        Step::data(vec![("eth", 11..=20), ("polygon", 6..=15)]).label("d2"),
        Step::watermark(vec![("eth", 11..=20), ("polygon", 6..=15)]),
    }
    .await;

    assert_eq!(events.len(), 3);
    assert_data_event_with_label(&events[0], 0, "d1");
    assert_data_event_with_label(&events[1], 1, "d2");
    assert_watermark_event(&events[2], 2);
}

/// Confirms that consecutive watermarks (with no data between them) each
/// receive unique, incrementing transaction IDs. This validates that
/// watermarks advance the transaction counter like any other event.
#[tokio::test]
async fn consecutive_watermarks_advance_transaction_ids() {
    let events = scenario! {
        Step::watermark(0..=10),
        Step::watermark(11..=20),
        Step::watermark(21..=30),
    }
    .await;

    assert_eq!(events.len(), 3);
    assert_watermark_event(&events[0], 0);
    assert_watermark_event(&events[1], 1);
    assert_watermark_event(&events[2], 2);
}
