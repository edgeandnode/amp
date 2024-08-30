use common::tracing;

use crate::test_support::{bless, check_blocks};

#[tokio::test]
async fn eth_firehose_single() {
    tracing::register_logger();

    if std::env::var("NOZZLE_TESTS_BLESS").is_ok() {
        bless("eth_firehose", 15_000_000, 15_000_000).await.unwrap();
    } else {
        check_blocks("eth_firehose", 15_000_000, 15_000_000)
            .await
            .unwrap();
    }
}
