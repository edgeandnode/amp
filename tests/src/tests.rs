use crate::test_support::{bless, check_blocks};
use common::tracing;
use log::warn;

#[tokio::test]
async fn eth_firehose_single() {
    tracing::register_logger();

    if std::env::var("NOZZLE_TESTS_BLESS").is_ok() {
        bless("eth_firehose", 15_000_000, 15_000_000).await.unwrap();

        warn!("bless completed successfully, running check blocks");
    }

    check_blocks("eth_firehose", 15_000_000, 15_000_000)
        .await
        .unwrap();
}
