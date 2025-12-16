//! CDC stream testing scenarios.

use futures::StreamExt;

use crate::{
    cdc::{CdcEvent, CdcStream},
    client::ResponseBatch,
    store::{InMemoryBatchStore, InMemoryStateStore},
    tests::utils::{MockResponseStream, Step, assert_insert_event, collect_delete_batches},
    transactional::TransactionalStream,
};

/// Helper to create a CDC stream from a sequence of response steps.
async fn create_cdc_stream(
    steps: Vec<Step>,
    retention: u64,
) -> Result<CdcStream, crate::error::Error> {
    let state_store = Box::new(InMemoryStateStore::new());
    let batch_store = Box::new(InMemoryBatchStore::new());

    let responses: Vec<ResponseBatch> = steps.into_iter().map(|step| step.into_batch()).collect();

    // Create the underlying transactional stream
    let responses_clone = responses.clone();
    let transactional = TransactionalStream::create(state_store, retention, move |_| {
        let responses = responses_clone.clone();
        async move { Ok(MockResponseStream::new(responses).into_raw_stream()) }
    })
    .await?;

    CdcStream::create(transactional, batch_store)
}

/// Verifies that a single data batch followed by a watermark produces
/// one Insert event with correct transaction ID and batch content.
#[tokio::test]
async fn cdc_stream_with_single_insert_emits_correct_event() {
    //* Given
    let steps = vec![Step::data(0..=10).label("batch1"), Step::watermark(0..=10)];

    //* When
    let mut stream = create_cdc_stream(steps, 128)
        .await
        .expect("CDC stream creation should succeed");

    //* Then
    let (event, commit) = stream
        .next()
        .await
        .expect("stream should have event")
        .expect("event should be Ok");

    assert_insert_event(&event, 0, "batch1");

    commit.await.expect("commit should succeed");

    // Should have no more CDC events (watermarks are internal)
    assert!(
        stream.next().await.is_none(),
        "stream should end after data"
    );
}

/// Validates that multiple data batches received before a watermark
/// produce multiple Insert events in correct order with sequential IDs.
#[tokio::test]
async fn cdc_stream_with_multiple_inserts_preserves_order() {
    //* Given
    let steps = vec![
        Step::data(0..=10).label("batch1"),
        Step::data(11..=20).label("batch2"),
        Step::data(21..=30).label("batch3"),
        Step::watermark(21..=30),
    ];

    //* When
    let mut stream = create_cdc_stream(steps, 128)
        .await
        .expect("CDC stream creation should succeed");

    //* Then
    // First insert
    let (event, commit) = stream
        .next()
        .await
        .expect("stream should have first event")
        .expect("first event should be Ok");
    assert_insert_event(&event, 0, "batch1");
    commit.await.expect("first commit should succeed");

    // Second insert
    let (event, commit) = stream
        .next()
        .await
        .expect("stream should have second event")
        .expect("second event should be Ok");
    assert_insert_event(&event, 1, "batch2");
    commit.await.expect("second commit should succeed");

    // Third insert
    let (event, commit) = stream
        .next()
        .await
        .expect("stream should have third event")
        .expect("third event should be Ok");
    assert_insert_event(&event, 2, "batch3");
    commit.await.expect("third commit should succeed");
}

/// Tests that a blockchain reorg triggers a Delete event with an iterator
/// containing the invalidated batches.
#[tokio::test]
async fn cdc_stream_with_reorg_emits_delete_event() {
    //* Given
    // Build up enough history so we can reorg without going before initial state
    let steps = vec![
        Step::data(0..=5).label("batch1"),
        Step::watermark(0..=5),
        Step::data(6..=10).label("batch2"),
        Step::watermark(6..=10),
        Step::data(11..=15).label("batch3"),
        Step::watermark(11..=15),
        Step::data(16..=20).label("batch4"),
        Step::watermark(16..=20),
        // Now reorg back to block 10 - this will invalidate batches 3 and 4
        Step::watermark(6..=10).reorg(vec!["eth"]),
    ];

    //* When
    let mut stream = create_cdc_stream(steps, 128)
        .await
        .expect("CDC stream creation should succeed");

    //* Then
    // Skip the insert events (4 of them)
    for _ in 0..4 {
        let (event, commit) = stream
            .next()
            .await
            .expect("should have event")
            .expect("should be Ok");
        assert!(matches!(event, CdcEvent::Insert { .. }));
        commit.await.expect("commit should succeed");
    }

    // Should get delete event
    let (event, commit) = stream
        .next()
        .await
        .expect("stream should have delete event")
        .expect("delete event should be Ok");

    match event {
        CdcEvent::Delete { id, batches } => {
            // The Undo event happens after the 4 data batches and 4 watermarks
            assert_eq!(id, 8, "Delete event should have correct ID");

            // Consume iterator
            let deleted = collect_delete_batches(batches)
                .await
                .expect("should collect delete batches");

            // The reorg invalidates batch2, batch3, and batch4 (IDs 2, 4, 6)
            assert_eq!(deleted.len(), 3, "should have three deleted batches");
            assert_eq!(deleted[0].0, 2, "first deleted batch should be batch2");
            assert_eq!(deleted[1].0, 4, "second deleted batch should be batch3");
            assert_eq!(deleted[2].0, 6, "third deleted batch should be batch4");
        }
        _ => panic!("Expected Delete event"),
    }

    commit.await.expect("commit should succeed");
}

/// Tests that the delete iterator correctly skips watermark IDs (which have no batch content)
/// and only returns actual data batches.
#[tokio::test]
async fn delete_iterator_skips_missing_batches() {
    //* Given
    // Build up committed history first
    let steps = vec![
        Step::data(0..=5).label("committed1"),
        Step::watermark(0..=5), // ID 1 - committed
        Step::data(6..=10).label("committed2"),
        Step::watermark(6..=10), // ID 3 - committed
        // Now add data that will be invalidated
        Step::data(11..=15).label("batch1"),
        Step::watermark(11..=15), // ID 5 - will be invalidated
        Step::data(16..=20).label("batch2"),
        Step::watermark(16..=20), // ID 7 - will be invalidated
        Step::data(21..=25).label("batch3"),
        Step::watermark(21..=25), // ID 9 - will be invalidated
        // Reorg back to committed state
        Step::watermark(6..=10).reorg(vec!["eth"]),
    ];

    //* When
    let mut stream = create_cdc_stream(steps, 128)
        .await
        .expect("CDC stream creation should succeed");

    //* Then
    // Skip insert events (5 total: 2 committed + 3 to be invalidated)
    for _ in 0..5 {
        let (event, commit) = stream
            .next()
            .await
            .expect("should have event")
            .expect("should be Ok");
        assert!(matches!(event, CdcEvent::Insert { .. }));
        commit.await.expect("commit should succeed");
    }

    // Get delete event
    let (event, _commit) = stream
        .next()
        .await
        .expect("stream should have delete event")
        .expect("delete event should be Ok");

    match event {
        CdcEvent::Delete { batches, .. } => {
            let deleted = collect_delete_batches(batches)
                .await
                .expect("should collect batches");

            // The reorg invalidates committed2, batch1, batch2, and batch3
            // IDs are: 2 (committed2), 4 (batch1), 6 (batch2), 8 (batch3)
            // Watermarks (IDs 3, 5, 7, 9) are skipped
            assert_eq!(
                deleted.len(),
                4,
                "should have four data batches (watermarks skipped)"
            );
            assert_eq!(deleted[0].0, 2, "batch should be committed2");
            assert_eq!(deleted[1].0, 4, "batch should be batch1");
            assert_eq!(deleted[2].0, 6, "batch should be batch2");
            assert_eq!(deleted[3].0, 8, "batch should be batch3");
        }
        _ => panic!("Expected Delete event"),
    }
}

/// Tests that an empty undo range (no data batches to delete) does not
/// produce a Delete event.
#[tokio::test]
async fn empty_undo_range_produces_no_delete_event() {
    //* Given
    let steps = vec![
        Step::data(0..=10).label("batch1"),
        Step::watermark(0..=10),
        Step::data(11..=20).label("batch2"),
        Step::watermark(11..=20),
        Step::data(21..=30).label("batch3"),
        Step::watermark(21..=30),
        // Reorg at same point (no actual invalidation)
        Step::watermark(21..=30).reorg(vec!["eth"]),
    ];

    //* When
    let mut stream = create_cdc_stream(steps, 128)
        .await
        .expect("CDC stream creation should succeed");

    //* Then
    // Should get three insert events
    for i in 0..3 {
        let (event, commit) = stream
            .next()
            .await
            .unwrap_or_else(|| panic!("should have event {}", i))
            .expect("event should be Ok");
        assert!(matches!(event, CdcEvent::Insert { .. }));
        commit.await.expect("commit should succeed");
    }

    // No more events (no delete event since nothing was invalidated)
    assert!(
        stream.next().await.is_none(),
        "should not emit delete event for empty undo range"
    );
}

/// Tests that consuming all batches from a delete iterator allows
/// the commit to succeed.
#[tokio::test]
async fn commit_succeeds_after_consuming_delete_iterator() {
    //* Given
    let steps = vec![
        Step::data(0..=5).label("batch1"),
        Step::watermark(0..=5),
        Step::data(6..=10).label("batch2"),
        Step::watermark(6..=10),
        Step::data(11..=15).label("batch3"),
        Step::watermark(11..=15),
        // Reorg to invalidate batch3
        Step::watermark(6..=10).reorg(vec!["eth"]),
    ];

    //* When
    let mut stream = create_cdc_stream(steps, 128)
        .await
        .expect("CDC stream creation should succeed");

    //* Then
    // Skip insert events
    for _ in 0..3 {
        let (event, commit) = stream
            .next()
            .await
            .expect("should have event")
            .expect("should be Ok");
        assert!(matches!(event, CdcEvent::Insert { .. }));
        commit.await.expect("commit should succeed");
    }

    // Get delete event
    let (event, commit) = stream
        .next()
        .await
        .expect("should have delete event")
        .expect("delete event should be Ok");

    match event {
        CdcEvent::Delete { mut batches, .. } => {
            // Consume all batches
            while let Some(result) = batches.next().await {
                result.expect("batch load should succeed");
            }

            // Commit should succeed after consuming iterator
            commit
                .await
                .expect("commit should succeed after consuming iterator");
        }
        _ => panic!("Expected Delete event"),
    }
}
