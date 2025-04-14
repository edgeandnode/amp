use crate::validate_block_range;
use common::{BlockNum, BlockStreamer, BoxError, DatasetRows};
use std::future::Future;
use tokio::sync::mpsc::Sender;

#[tokio::test]
async fn test_validate_block_range() {
    #[derive(Clone)]
    struct MockStreamer;
    impl BlockStreamer for MockStreamer {
        fn block_stream(
            self,
            _: BlockNum,
            _: BlockNum,
            _: Sender<DatasetRows>,
        ) -> impl Future<Output = Result<(), BoxError>> + Send {
            async move { Ok(()) }
        }

        fn latest_block(
            &mut self,
            _: bool,
        ) -> impl Future<Output = Result<BlockNum, BoxError>> + Send {
            async move { Ok(100) }
        }
    }

    let mut test_streamer = MockStreamer;

    let test_cases: Vec<(i64, Option<i64>, Result<(BlockNum, BlockNum), BoxError>)> = vec![
        (50, None, Ok((50, 100))),
        (-80, None, Ok((20, 100))),
        (100, Some(150), Ok((100, 150))),
        (-80, Some(-10), Ok((20, 90))),
        (70, Some(-50), Err(BoxError::from(""))), // overlapped
    ];

    for (start_block, end_block, expected) in test_cases {
        match validate_block_range(&mut test_streamer, start_block, end_block).await {
            Ok(result) => assert_eq!(expected.unwrap(), result),
            Err(_) => assert!(expected.is_err()),
        }
    }
}
