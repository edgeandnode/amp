//! CDC-specific test utilities.

use arrow::array::RecordBatch;

use crate::{cdc::CdcEvent, transactional::TransactionId};

/// Assertion helper for CDC Insert events.
pub fn assert_insert_event(event: &CdcEvent, expected_id: TransactionId, expected_label: &str) {
    match event {
        CdcEvent::Insert { id, batch } => {
            assert_eq!(*id, expected_id, "Insert event should have expected ID");
            let label_array = batch
                .column_by_name("label")
                .expect("batch should have 'label' column");
            let label_str = format!("{:?}", label_array);
            assert!(
                label_str.contains(expected_label),
                "Insert batch should contain label '{}', got: {}",
                expected_label,
                label_str
            );
        }
        _ => panic!("Expected Insert event, got Delete event"),
    }
}

/// Collect all batches from a delete iterator.
pub async fn collect_delete_batches(
    mut iterator: crate::cdc::DeleteBatchIterator,
) -> Result<Vec<(TransactionId, RecordBatch)>, crate::error::Error> {
    let mut batches = Vec::new();
    while let Some(result) = iterator.next().await {
        batches.push(result?);
    }
    Ok(batches)
}
