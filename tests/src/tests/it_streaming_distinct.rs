//! Integration tests for DISTINCT ON and GROUP BY in streaming queries with Anvil.

use monitoring::logging;

use crate::{steps::run_spec, testlib::ctx::TestCtxBuilder};

#[tokio::test(flavor = "multi_thread")]
async fn streaming_distinct_and_group_by_as_derived_dataset_tables() {
    logging::init();

    let test_ctx = TestCtxBuilder::new("streaming_distinct_and_group_by_derived")
        .with_anvil_ipc()
        .with_dataset_manifest("anvil_rpc")
        .build()
        .await
        .expect("Failed to create test environment");

    let mut client = test_ctx
        .new_flight_client()
        .await
        .expect("Failed to connect FlightClient");

    run_spec(
        "streaming-distinct-on-derived-anvil",
        &test_ctx,
        &mut client,
        None,
    )
    .await
    .expect("Failed to run streaming distinct on derived anvil spec");
}

#[tokio::test(flavor = "multi_thread")]
async fn streaming_distinct_and_group_by() {
    logging::init();

    let test_ctx = TestCtxBuilder::new("streaming_distinct_and_group_by")
        .with_anvil_ipc()
        .with_dataset_manifest("anvil_rpc")
        .build()
        .await
        .expect("Failed to create test environment");

    let mut client = test_ctx
        .new_flight_client()
        .await
        .expect("Failed to connect FlightClient");

    run_spec("streaming-distinct-anvil", &test_ctx, &mut client, None)
        .await
        .expect("Failed to run streaming distinct and group by spec");
}
