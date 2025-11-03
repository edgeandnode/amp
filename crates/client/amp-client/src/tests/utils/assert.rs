//! Assertion helpers for testing streaming scenarios.

use std::ops::RangeInclusive;

use common::arrow::array::AsArray;

use crate::transactional::{Cause, TransactionEvent};

/// Assert that an event is a Data event.
///
/// # Example
/// ```ignore
/// assert_data_event(&events[0], 1);
/// ```
#[track_caller]
pub fn assert_data_event(event: &TransactionEvent, expected_id: u64) {
    match event {
        TransactionEvent::Data { id, .. } => {
            assert_eq!(
                *id, expected_id,
                "Expected data event with id {}",
                expected_id
            );
        }
        other => panic!("Expected Data event, got {:?}", other),
    }
}

/// Assert that an event is a Data event with a specific label.
#[track_caller]
pub fn assert_data_event_with_label(
    event: &TransactionEvent,
    expected_id: u64,
    expected_label: &str,
) {
    match event {
        TransactionEvent::Data { id, batch, .. } => {
            assert_eq!(
                *id, expected_id,
                "Expected data event with id {}",
                expected_id
            );

            // Check label column if present
            if let Some(label_col) = batch.column_by_name("label") {
                let labels = label_col.as_string::<i32>();
                let first_label = labels.value(0);
                assert_eq!(
                    first_label, expected_label,
                    "Expected label '{}', got '{}'",
                    expected_label, first_label
                );
            } else {
                panic!("Data event {} has no label column", id);
            }
        }
        other => panic!("Expected Data event, got {:?}", other),
    }
}

/// Assert that an event is a Watermark event.
///
/// # Example
/// ```ignore
/// assert_watermark_event(&events[1], 2);
/// ```
#[track_caller]
pub fn assert_watermark_event(event: &TransactionEvent, expected_id: u64) {
    match event {
        TransactionEvent::Watermark { id, .. } => {
            assert_eq!(
                *id, expected_id,
                "Expected watermark event with id {}",
                expected_id
            );
        }
        other => panic!("Expected Watermark event, got {:?}", other),
    }
}

/// Assert that an event is a Watermark event with specific prune range.
#[track_caller]
pub fn assert_watermark_event_with_prune(
    event: &TransactionEvent,
    expected_id: u64,
    expected_prune: Option<RangeInclusive<u64>>,
) {
    match event {
        TransactionEvent::Watermark { id, prune, .. } => {
            assert_eq!(
                *id, expected_id,
                "Expected watermark event with id {}",
                expected_id
            );
            assert_eq!(
                *prune, expected_prune,
                "Expected prune range {:?}, got {:?}",
                expected_prune, prune
            );
        }
        other => panic!("Expected Watermark event, got {:?}", other),
    }
}

/// Assert that an event is an Undo event with specific invalidation range.
#[track_caller]
pub fn assert_undo_event_with_invalidation(
    event: &TransactionEvent,
    expected_id: u64,
    expected_invalidation: RangeInclusive<u64>,
) {
    match event {
        TransactionEvent::Undo {
            id,
            invalidate: invalidation,
            ..
        } => {
            assert_eq!(
                *id, expected_id,
                "Expected undo event with id {}",
                expected_id
            );
            assert_eq!(
                *invalidation, expected_invalidation,
                "Expected invalidation {:?}, got {:?}",
                expected_invalidation, invalidation
            );
        }
        other => panic!("Expected Undo event, got {:?}", other),
    }
}

/// Assert that an event is an Undo event with a specific cause.
#[track_caller]
pub fn assert_undo_event_with_cause(event: &TransactionEvent, expected_id: u64, is_reorg: bool) {
    match event {
        TransactionEvent::Undo { id, cause, .. } => {
            assert_eq!(
                *id, expected_id,
                "Expected undo event with id {}",
                expected_id
            );
            match (is_reorg, cause) {
                (true, Cause::Reorg(_)) => {}
                (false, Cause::Rewind) => {}
                _ => panic!(
                    "Expected {} cause, got {:?}",
                    if is_reorg { "Reorg" } else { "Rewind" },
                    cause
                ),
            }
        }
        other => panic!("Expected Undo event, got {:?}", other),
    }
}
