use std::{
    pin::Pin,
    task::{Context, Poll},
};

use common::arrow::array::RecordBatch;
use datafusion::execution::SendableRecordBatchStream;
use futures::stream::Stream;

/// Stream adapter that yields an empty record batch if the stream would otherwise be empty.
///
/// This ensures the stream always yields at least one item - either the actual data or an empty
/// record batch with the correct schema. Without this adapter, an empty stream would only yield
/// `None` without ever yielding any `Some(_)`.
pub struct NonEmptyRecordBatchStream {
    stream: SendableRecordBatchStream,
    has_yielded: bool,
}

impl NonEmptyRecordBatchStream {
    pub fn new(stream: SendableRecordBatchStream) -> Self {
        Self {
            stream,
            has_yielded: false,
        }
    }
}

impl Stream for NonEmptyRecordBatchStream {
    type Item = datafusion::common::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.stream).poll_next(cx) {
            Poll::Ready(Some(item)) => {
                self.has_yielded = true;
                Poll::Ready(Some(item))
            }
            Poll::Ready(None) => {
                if !self.has_yielded {
                    self.has_yielded = true;
                    Poll::Ready(Some(Ok(RecordBatch::new_empty(self.stream.schema()))))
                } else {
                    Poll::Ready(None)
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common::arrow::{
        array::{Int32Array, RecordBatch},
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::physical_plan::memory::MemoryStream;
    use futures::StreamExt;

    use super::*;

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]))
    }

    fn create_test_batch(schema: Arc<Schema>, values: Vec<i32>) -> RecordBatch {
        let array = Int32Array::from(values);
        RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }

    #[tokio::test]
    async fn test_empty_stream() {
        let schema = create_test_schema();
        let empty_stream = MemoryStream::try_new(vec![], schema.clone(), None).unwrap();

        let mut stream = NonEmptyRecordBatchStream::new(Box::pin(empty_stream));

        // Should yield one empty record batch
        let result = stream.next().await;
        assert!(result.is_some());
        let batch = result.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema(), schema);

        // Should yield None after
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_non_empty_stream() {
        let schema = create_test_schema();
        let batch1 = create_test_batch(schema.clone(), vec![1, 2, 3]);
        let batch2 = create_test_batch(schema.clone(), vec![4, 5, 6]);

        let memory_stream =
            MemoryStream::try_new(vec![batch1.clone(), batch2.clone()], schema.clone(), None)
                .unwrap();
        let mut stream = NonEmptyRecordBatchStream::new(Box::pin(memory_stream));

        // Should yield both batches
        let result1 = stream.next().await.unwrap().unwrap();
        assert_eq!(result1.num_rows(), 3);

        let result2 = stream.next().await.unwrap().unwrap();
        assert_eq!(result2.num_rows(), 3);

        // Should yield None after all items
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_single_item_stream() {
        let schema = create_test_schema();
        let batch = create_test_batch(schema.clone(), vec![42]);

        let memory_stream =
            MemoryStream::try_new(vec![batch.clone()], schema.clone(), None).unwrap();
        let mut stream = NonEmptyRecordBatchStream::new(Box::pin(memory_stream));

        // Should yield the single batch
        let result = stream.next().await.unwrap().unwrap();
        assert_eq!(result.num_rows(), 1);

        // Should yield None after
        assert!(stream.next().await.is_none());
    }
}
