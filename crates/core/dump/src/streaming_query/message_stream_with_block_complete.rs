use std::{
    pin::Pin,
    task::{Context, Poll},
};

use common::{BlockNum, BoxError, SPECIAL_BLOCK_NUM, arrow::array::RecordBatch};
use futures::{Stream, ready};

use super::QueryMessage;

/// A stream adapter that enriches a `QueryMessage` stream with `BlockComplete` messages.
///
/// This stream wraps a QueryMessage stream and:
/// - Validates that Data batches are ordered by `_block_num`
/// - Tracks the last block number emitted
/// - Splits batches on block boundaries and emits BlockComplete messages
/// - Clears state on MicrobatchEnd and reorgs
pub struct MessageStreamWithBlockComplete<S> {
    inner: S,
    /// The last block number we've been emitting data for
    last_block_num: Option<BlockNum>,
    /// Buffer for partial batch data that needs to be emitted
    pending_batch: Option<RecordBatch>,
    /// Block number to complete when emitting the pending batch
    pending_block_complete: Option<BlockNum>,
}

impl<S> MessageStreamWithBlockComplete<S>
where
    S: Stream<Item = Result<QueryMessage, BoxError>>,
{
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            last_block_num: None,
            pending_batch: None,
            pending_block_complete: None,
        }
    }
}

impl<S> Stream for MessageStreamWithBlockComplete<S>
where
    S: Stream<Item = Result<QueryMessage, BoxError>> + Unpin,
{
    type Item = Result<QueryMessage, BoxError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // First, emit any pending block complete message, then pending batch
        if let Some(block_num) = this.pending_block_complete.take() {
            return Poll::Ready(Some(Ok(QueryMessage::BlockComplete(block_num))));
        }

        if let Some(batch) = this.pending_batch.take() {
            return Poll::Ready(Some(Ok(QueryMessage::Data(batch))));
        }

        // Poll the inner stream
        let message = match ready!(Pin::new(&mut this.inner).poll_next(cx)) {
            Some(Ok(message)) => message,
            Some(Err(e)) => return Poll::Ready(Some(Err(e))),
            None => return Poll::Ready(None),
        };

        match message {
            QueryMessage::MicrobatchStart { range, is_reorg } => {
                // Clear state on reorgs
                if is_reorg {
                    this.last_block_num = None;
                }
                Poll::Ready(Some(Ok(QueryMessage::MicrobatchStart { range, is_reorg })))
            }
            QueryMessage::Data(batch) => {
                match process_data_batch(batch, this.last_block_num) {
                    Ok(BatchProcessResult::PassThrough(batch)) => {
                        Poll::Ready(Some(Ok(QueryMessage::Data(batch))))
                    }
                    Ok(BatchProcessResult::Split {
                        current_block_batch,
                        completed_block_num,
                        next_block_batch,
                        next_block_num,
                    }) => {
                        // Update state for the next block
                        this.last_block_num = Some(next_block_num);

                        // Queue the next batch
                        this.pending_batch = Some(next_block_batch);

                        // If current block batch is None, emit BlockComplete directly
                        match current_block_batch {
                            None => Poll::Ready(Some(Ok(QueryMessage::BlockComplete(
                                completed_block_num,
                            )))),
                            Some(batch) => {
                                // Queue the block complete message and emit the current batch first
                                this.pending_block_complete = Some(completed_block_num);
                                Poll::Ready(Some(Ok(QueryMessage::Data(batch))))
                            }
                        }
                    }
                    Ok(BatchProcessResult::UpdateBlockNum(batch, block_num)) => {
                        this.last_block_num = Some(block_num);
                        Poll::Ready(Some(Ok(QueryMessage::Data(batch))))
                    }
                    Err(e) => Poll::Ready(Some(Err(e))),
                }
            }
            QueryMessage::MicrobatchEnd(range) => {
                // Clear state on microbatch end
                this.last_block_num = None;
                Poll::Ready(Some(Ok(QueryMessage::MicrobatchEnd(range))))
            }
            QueryMessage::BlockComplete(block_num) => Poll::Ready(Some(Err(format!(
                "Unexpected BlockComplete message from inner stream: {}",
                block_num
            )
            .into()))),
        }
    }
}

enum BatchProcessResult {
    /// Pass the batch through unchanged
    PassThrough(RecordBatch),
    /// Update the tracked block number and pass through
    UpdateBlockNum(RecordBatch, BlockNum),
    /// Split the batch on block boundary
    Split {
        current_block_batch: Option<RecordBatch>,
        completed_block_num: BlockNum,
        next_block_batch: RecordBatch,
        next_block_num: BlockNum,
    },
}

fn process_data_batch(
    batch: RecordBatch,
    last_block_num: Option<BlockNum>,
) -> Result<BatchProcessResult, BoxError> {
    // Find the _block_num column
    let block_num_column = batch.column_by_name(SPECIAL_BLOCK_NUM);

    let Some(block_num_array) = block_num_column else {
        // No _block_num column, pass through unchanged
        return Ok(BatchProcessResult::PassThrough(batch));
    };

    // Extract block numbers from the array
    let block_nums = extract_block_numbers(block_num_array)?;

    if block_nums.is_empty() {
        return Ok(BatchProcessResult::PassThrough(batch));
    }

    // Always validate ordering
    validate_block_ordering(&block_nums)?;

    let first_block = block_nums[0];
    let last_block = block_nums[block_nums.len() - 1];

    match last_block_num {
        None => {
            // First batch, just track the block number (no splitting on first batch)
            Ok(BatchProcessResult::UpdateBlockNum(batch, last_block))
        }
        Some(current_block) => {
            if first_block == current_block {
                if last_block == current_block {
                    // All rows are for the same block as before
                    Ok(BatchProcessResult::PassThrough(batch))
                } else {
                    // Batch spans multiple blocks, need to split
                    let split_index = block_nums.iter().position(|&n| n > current_block).unwrap(); // Safe: we know last_block > current_block from condition above
                    let (current_batch, next_batch) = split_record_batch(&batch, split_index)?;

                    Ok(BatchProcessResult::Split {
                        current_block_batch: Some(current_batch),
                        completed_block_num: current_block,
                        next_block_batch: next_batch,
                        next_block_num: last_block,
                    })
                }
            } else if first_block > current_block {
                // New block started - emit entire batch as-is (don't split within it)
                Ok(BatchProcessResult::Split {
                    current_block_batch: None,
                    completed_block_num: current_block,
                    next_block_batch: batch,
                    next_block_num: last_block,
                })
            } else {
                // Block number went backwards - this can happen during reorgs
                // Since we clear state on reorgs, this should be handled naturally
                Ok(BatchProcessResult::UpdateBlockNum(batch, last_block))
            }
        }
    }
}

fn extract_block_numbers(
    array: &dyn common::arrow::array::Array,
) -> Result<Vec<BlockNum>, BoxError> {
    use common::arrow::{array::*, datatypes::DataType};

    let block_nums = match array.data_type() {
        DataType::UInt64 => {
            let uint64_array = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or("Failed to downcast to UInt64Array")?;
            uint64_array
                .iter()
                .collect::<Option<Vec<_>>>()
                .ok_or("Found null values in _block_num column")?
        }
        _ => {
            return Err(format!(
                "Unsupported _block_num column type: {:?}",
                array.data_type()
            )
            .into());
        }
    };

    Ok(block_nums)
}

fn validate_block_ordering(block_nums: &[BlockNum]) -> Result<(), BoxError> {
    for window in block_nums.windows(2) {
        if window[0] > window[1] {
            return Err(format!("Block numbers not ordered: {} > {}", window[0], window[1]).into());
        }
    }
    Ok(())
}

fn split_record_batch(
    batch: &RecordBatch,
    split_index: usize,
) -> Result<(RecordBatch, RecordBatch), BoxError> {
    if split_index == 0 {
        return Err("Split index is 0, cannot split".into());
    }

    if split_index >= batch.num_rows() {
        return Err("Split index is out of bounds".into());
    }

    let first_batch_arrays: Vec<_> = batch
        .columns()
        .iter()
        .map(|array| array.slice(0, split_index))
        .collect();

    let second_batch_arrays: Vec<_> = batch
        .columns()
        .iter()
        .map(|array| array.slice(split_index, batch.num_rows() - split_index))
        .collect();

    let first_batch = RecordBatch::try_new(batch.schema(), first_batch_arrays)?;
    let second_batch = RecordBatch::try_new(batch.schema(), second_batch_arrays)?;

    Ok((first_batch, second_batch))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common::{
        BlockRange,
        arrow::{
            array::UInt64Array,
            datatypes::{DataType, Field, Schema},
        },
    };
    use futures::{StreamExt, stream};

    use super::*;

    fn create_test_batch(block_nums: Vec<u64>, data: Vec<u64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(SPECIAL_BLOCK_NUM, DataType::UInt64, false),
            Field::new("data", DataType::UInt64, false),
        ]));

        let block_array = Arc::new(UInt64Array::from(block_nums));
        let data_array = Arc::new(UInt64Array::from(data));

        RecordBatch::try_new(schema, vec![block_array, data_array]).unwrap()
    }

    fn create_test_range(start: u64, end: u64) -> BlockRange {
        BlockRange {
            numbers: start..=end,
            network: "test".parse().expect("valid network id"),
            hash: [0u8; 32].into(),
            prev_hash: [0u8; 32].into(),
        }
    }

    async fn collect_messages(
        messages: Vec<Result<QueryMessage, BoxError>>,
    ) -> Vec<Result<QueryMessage, BoxError>> {
        let input_stream = stream::iter(messages);
        let mut aligned_stream = MessageStreamWithBlockComplete::new(input_stream);

        let mut results = Vec::new();
        while let Some(msg) = aligned_stream.next().await {
            results.push(msg);
        }
        results
    }

    fn expect_data_blocks(msg: &Result<QueryMessage, BoxError>) -> Vec<u64> {
        if let Ok(QueryMessage::Data(batch)) = msg {
            extract_block_numbers(batch.column_by_name(SPECIAL_BLOCK_NUM).unwrap()).unwrap()
        } else {
            panic!("Expected Data message");
        }
    }

    fn microbatch(start: u64, end: u64) -> QueryMessage {
        QueryMessage::MicrobatchStart {
            range: create_test_range(start, end),
            is_reorg: false,
        }
    }

    fn microbatch_reorg(start: u64, end: u64) -> QueryMessage {
        QueryMessage::MicrobatchStart {
            range: create_test_range(start, end),
            is_reorg: true,
        }
    }

    #[tokio::test]
    async fn test_single_block_batch() {
        let messages = vec![
            Ok(microbatch(100, 100)),
            Ok(QueryMessage::Data(create_test_batch(
                vec![100, 100, 100],
                vec![1, 2, 3],
            ))),
            Ok(QueryMessage::MicrobatchEnd(create_test_range(100, 100))),
        ];

        let results = collect_messages(messages).await;
        assert_eq!(results.len(), 3); // No BlockComplete for single block
    }

    #[tokio::test]
    async fn test_split_at_block_boundary() {
        let messages = vec![
            Ok(microbatch(100, 101)),
            Ok(QueryMessage::Data(create_test_batch(
                vec![100, 100],
                vec![1, 2],
            ))),
            Ok(QueryMessage::Data(create_test_batch(
                vec![100, 101, 101],
                vec![3, 4, 5],
            ))), // spans blocks
            Ok(QueryMessage::MicrobatchEnd(create_test_range(100, 101))),
        ];

        let results = collect_messages(messages).await;
        assert_eq!(results.len(), 6);
        assert_eq!(expect_data_blocks(&results[2]), vec![100]); // Split portion
        matches!(results[3], Ok(QueryMessage::BlockComplete(100)));
        assert_eq!(expect_data_blocks(&results[4]), vec![101, 101]); // Remainder
    }

    #[tokio::test]
    async fn test_new_block_transition_no_current_data() {
        let messages = vec![
            Ok(microbatch(100, 101)),
            Ok(QueryMessage::Data(create_test_batch(
                vec![100, 100],
                vec![1, 2],
            ))),
            Ok(QueryMessage::Data(create_test_batch(
                vec![101, 101],
                vec![3, 4],
            ))), // new block
            Ok(QueryMessage::MicrobatchEnd(create_test_range(100, 101))),
        ];

        let results = collect_messages(messages).await;
        assert_eq!(results.len(), 5);
        matches!(results[2], Ok(QueryMessage::BlockComplete(100))); // No empty batch emitted
    }

    #[tokio::test]
    async fn test_reorg_clears_state() {
        let messages = vec![
            Ok(microbatch(100, 100)),
            Ok(QueryMessage::Data(create_test_batch(
                vec![100, 100],
                vec![1, 2],
            ))),
            Ok(microbatch_reorg(99, 99)),
            Ok(QueryMessage::Data(create_test_batch(
                vec![99, 99],
                vec![3, 4],
            ))), // goes backwards
            Ok(QueryMessage::MicrobatchEnd(create_test_range(99, 99))),
        ];

        let results = collect_messages(messages).await;
        assert_eq!(results.len(), 5); // No BlockComplete during reorg
    }

    #[tokio::test]
    async fn test_three_blocks_in_batch() {
        let messages = vec![
            Ok(microbatch(100, 102)),
            Ok(QueryMessage::Data(create_test_batch(vec![100], vec![1]))),
            Ok(QueryMessage::Data(create_test_batch(
                vec![100, 101, 102, 102],
                vec![2, 3, 4, 5],
            ))), // 3 blocks
            Ok(QueryMessage::MicrobatchEnd(create_test_range(100, 102))),
        ];

        let results = collect_messages(messages).await;
        assert_eq!(results.len(), 6);
        assert_eq!(expect_data_blocks(&results[2]), vec![100]); // Split portion
        matches!(results[3], Ok(QueryMessage::BlockComplete(100)));
        assert_eq!(expect_data_blocks(&results[4]), vec![101, 102, 102]); // Remainder (not split again)
    }

    #[tokio::test]
    async fn test_ordering_validation() {
        let messages = vec![
            Ok(microbatch(99, 101)),
            Ok(QueryMessage::Data(create_test_batch(
                vec![100, 99, 101],
                vec![1, 2, 3],
            ))), // unordered
        ];

        let results = collect_messages(messages).await;
        assert_eq!(results.len(), 2);
        assert!(results[1].is_err()); // Error due to bad ordering
    }

    #[tokio::test]
    async fn test_no_block_num_column() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "data",
            DataType::UInt64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(vec![1, 2, 3]))]).unwrap();

        let messages = vec![
            Ok(microbatch(100, 100)),
            Ok(QueryMessage::Data(batch)),
            Ok(QueryMessage::MicrobatchEnd(create_test_range(100, 100))),
        ];

        let results = collect_messages(messages).await;
        assert_eq!(results.len(), 3); // Should pass through unchanged
    }

    #[tokio::test]
    async fn test_unexpected_block_complete_from_inner() {
        let messages = vec![Ok(QueryMessage::BlockComplete(100))]; // Unexpected from inner

        let results = collect_messages(messages).await;
        assert_eq!(results.len(), 1);
        assert!(results[0].is_err()); // Should be an error
    }

    #[tokio::test]
    async fn test_same_block_passthrough() {
        let messages = vec![
            Ok(microbatch(100, 100)),
            Ok(QueryMessage::Data(create_test_batch(
                vec![100, 100],
                vec![1, 2],
            ))), // sets last_block_num = 100
            Ok(QueryMessage::Data(create_test_batch(
                vec![100, 100],
                vec![3, 4],
            ))), // same block, should pass through
            Ok(QueryMessage::MicrobatchEnd(create_test_range(100, 100))),
        ];

        let results = collect_messages(messages).await;
        assert_eq!(results.len(), 4); // No BlockComplete, just passthrough
        assert_eq!(expect_data_blocks(&results[1]), vec![100, 100]);
        assert_eq!(expect_data_blocks(&results[2]), vec![100, 100]); // Passed through unchanged
    }

    #[tokio::test]
    async fn test_block_number_goes_backwards() {
        let messages = vec![
            Ok(microbatch(100, 100)),
            Ok(QueryMessage::Data(create_test_batch(
                vec![100, 100],
                vec![1, 2],
            ))), // sets last_block_num = 100
            Ok(QueryMessage::Data(create_test_batch(
                vec![99, 99],
                vec![3, 4],
            ))), // goes backwards (not during reorg)
            Ok(QueryMessage::MicrobatchEnd(create_test_range(99, 100))),
        ];

        let results = collect_messages(messages).await;
        assert_eq!(results.len(), 4); // Should handle backwards gracefully (UpdateBlockNum)
        assert_eq!(expect_data_blocks(&results[2]), vec![99, 99]); // Backwards batch passed through
    }
}
