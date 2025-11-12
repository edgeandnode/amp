//! Rewind and crash recovery scenarios.

use futures::StreamExt;

use crate::{
    scenario,
    tests::utils::{
        SharedStore, Step, assert_data_event, assert_data_event_with_label,
        assert_undo_event_with_cause, assert_undo_event_with_invalidation, assert_watermark_event,
    },
};

/// Tests the basic crash recovery mechanism where the stream detects uncommitted
/// transactions on restart. Simulates a crash after processing but before committing
/// a data batch, then verifies that rewind correctly invalidates the uncommitted work.
#[tokio::test]
async fn rewind_detects_uncommitted_after_watermark() {
    let store = SharedStore::new();

    // First stream: commit watermark, leave data uncommitted
    let mut stream = scenario! {
        @stream
        store: store.clone();

        Step::data(0..=10).label("committed"),
        Step::watermark(0..=10),
        Step::data(11..=20).label("uncommitted"),
    }
    .await;

    let (_, c1) = stream.next().await.unwrap().unwrap();
    c1.await.unwrap();
    let (_, c2) = stream.next().await.unwrap().unwrap();
    c2.await.unwrap();
    // Don't commit batch 3 (id=2) - simulates crash
    stream.next().await;

    // Restart triggers rewind
    let events = scenario! {
        store: store.clone();
        Step::data(11..=20).label("retry"),
    }
    .await;

    assert_eq!(events.len(), 2);
    assert_undo_event_with_cause(&events[0], 3, false);
    assert_undo_event_with_invalidation(&events[0], 3, 2..=2);
    assert_data_event_with_label(&events[1], 4, "retry");
}

/// Validates that rewind correctly identifies and invalidates multiple uncommitted
/// batches spanning a range of transaction IDs. Tests the gap detection logic
/// between the last watermark and the current transaction counter.
#[tokio::test]
async fn rewind_multiple_uncommitted_batches() {
    let store = SharedStore::new();

    let mut stream = scenario! {
        @stream
        store: store.clone();

        Step::data(0..=10).label("committed1"),
        Step::watermark(0..=10),
        Step::data(11..=20).label("uncommitted1"),
        Step::data(21..=30).label("uncommitted2"),
        Step::data(31..=40).label("uncommitted3"),
    }
    .await;

    // Commit first two only (ids 0,1)
    let (_, c1) = stream.next().await.unwrap().unwrap();
    c1.await.unwrap();
    let (_, c2) = stream.next().await.unwrap().unwrap();
    c2.await.unwrap();

    // Consume but don't commit last three (ids 2,3,4) - simulates crash
    stream.next().await;
    stream.next().await;
    stream.next().await;

    // Restart
    let events = scenario! {
        store: store.clone();
        Step::data(11..=20).label("retry"),
    }
    .await;

    assert_eq!(events.len(), 2);
    assert_undo_event_with_invalidation(&events[0], 5, 2..=4);
}

/// Verifies that when all events have been committed before restart, no rewind
/// event is emitted and the stream continues normally from the next transaction ID.
/// This confirms the rewind detection logic correctly identifies clean shutdowns.
#[tokio::test]
async fn no_rewind_when_fully_committed() {
    let store = SharedStore::new();

    let _events = scenario! {
        store: store.clone();

        Step::data(0..=10).label("committed1"),
        Step::watermark(0..=10),
    }
    .await; // Auto-commits all events

    let events = scenario! {
        store: store.clone();
        Step::data(11..=20).label("next"),
    }
    .await;

    // First event should be Data, not Undo
    assert_eq!(events.len(), 1);
    assert_data_event_with_label(&events[0], 2, "next");
}

/// Tests rewind detection from a completely fresh state (no previous watermark).
/// Validates that the system correctly handles the edge case of crashing on the
/// very first batch before any watermark has been established.
#[tokio::test]
async fn rewind_with_no_previous_watermark() {
    let store = SharedStore::new();

    let mut stream = scenario! {
        @stream
        store: store.clone();

        Step::data(0..=10).label("uncommitted"),
    }
    .await;

    // Don't commit (id=0) - simulates crash on first batch
    stream.next().await;

    // Restart from empty watermark
    let events = scenario! {
        store: store.clone();
        Step::data(0..=10).label("retry"),
    }
    .await;

    assert_eq!(events.len(), 2);
    assert_undo_event_with_cause(&events[0], 1, false);
    assert_undo_event_with_invalidation(&events[0], 1, 0..=0);
}

/// Validates that after a rewind event, the stream can continue processing
/// normally with new data and watermarks. Tests the full recovery cycle:
/// crash → rewind → resume normal operation.
#[tokio::test]
async fn rewind_then_normal_streaming() {
    let store = SharedStore::new();

    // First stream: partial commit
    let mut stream = scenario! {
        @stream
        store: store.clone();

        Step::data(0..=10).label("committed"),
        Step::watermark(0..=10),
        Step::data(11..=20).label("uncommitted"),
    }
    .await;

    let (_, c1) = stream.next().await.unwrap().unwrap();
    c1.await.unwrap();
    let (_, c2) = stream.next().await.unwrap().unwrap();
    c2.await.unwrap();
    // Don't commit third (id=2) - simulates crash
    stream.next().await;

    // Restart and continue streaming
    let events = scenario! {
        store: store.clone();

        Step::data(11..=20).label("retry"),
        Step::watermark(11..=20),
        Step::data(21..=30).label("new"),
    }
    .await;

    assert_eq!(events.len(), 4);
    assert_undo_event_with_invalidation(&events[0], 3, 2..=2);
    assert_data_event(&events[1], 4);
    assert_watermark_event(&events[2], 5);
    assert_data_event(&events[3], 6);
}
