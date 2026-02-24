use futures::TryStreamExt;
use solana_datasets::{
    Client, UseArchive, of1_client,
    rpc_client::{self, rpc_config::CommitmentConfig},
};
use url::Url;

/// Test the transition from historical blocks to JSON-RPC blocks in the Solana client.
#[tokio::test]
async fn historical_to_json_rpc_transition() {
    let provider_name = "test_provider"
        .parse()
        .expect("Failed to parse provider name");
    let url = std::env::var("SOLANA_MAINNET_HTTP_URL")
        .expect("Missing environment variable SOLANA_MAINNET_HTTP_URL")
        .parse::<Url>()
        .map(Into::into)
        .expect("Failed to parse URL");
    let network = "mainnet".parse().expect("Failed to parse network id");

    let client = Client::new(
        url,
        None, // Auth
        None, // Rate limit
        network,
        provider_name,
        std::path::PathBuf::new(),
        false, // keep_of1_car_files
        UseArchive::Auto,
        None, // Metrics
        CommitmentConfig::finalized(),
    );

    let start = 0;
    let historical_end = 50;
    let end = historical_end + 20;

    // Stream part of the range as historical blocks.
    let historical = async_stream::stream! {
        for slot in start..=historical_end {
            yield Ok(of1_client::DecodedSlot::dummy(slot));
        }
    };

    let get_block_config = rpc_client::rpc_config::RpcBlockConfig {
        encoding: Some(rpc_client::rpc_config::UiTransactionEncoding::Json),
        transaction_details: Some(rpc_client::rpc_config::TransactionDetails::Full),
        max_supported_transaction_version: Some(0),
        rewards: Some(true),
        commitment: Some(rpc_client::rpc_config::CommitmentConfig::finalized()),
    };
    let block_stream = client.block_stream_impl(start, end, historical, get_block_config);

    let mut expected_block = start;
    let mut stream = std::pin::pin!(block_stream);

    while let Some(rows) = stream.try_next().await.expect("stream should not error") {
        assert_eq!(rows.block_num(), expected_block);
        expected_block += 1;
    }

    assert_eq!(expected_block, end + 1);
}
