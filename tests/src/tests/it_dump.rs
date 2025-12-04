use datasets_common::reference::Reference;
use monitoring::logging;

use crate::testlib::{
    self, ctx::TestCtxBuilder, fixtures::SnapshotContext, helpers as test_helpers,
};

#[tokio::test]
async fn evm_rpc_single_dump() {
    //* Given
    let test = TestCtx::setup("evm_rpc_single_dump", "_/eth_rpc@0.0.0", "rpc_eth_mainnet").await;

    let block = test.get_dataset_start_block().await;
    let reference = test.restore_reference_snapshot().await;

    //* When
    let dumped = test.dump_and_create_snapshot(block).await;

    //* Then
    // Validate table consistency
    for table in dumped.physical_tables() {
        test_helpers::check_table_consistency(table)
            .await
            .expect("Table consistency check failed");
    }

    // Compare snapshots
    test_helpers::assert_snapshots_eq(&dumped, &reference).await;
}

#[tokio::test]
async fn solana_single_dump() {
    logging::init();

    // * Given
    let dataset_ref: Reference = "_/solana@0.0.0".parse().unwrap();
    let test_env = TestCtxBuilder::new("solana_single_dump")
        .with_dataset_manifest(dataset_ref.name().to_string())
        .with_provider_config("solana_mainnet")
        .with_dataset_snapshot(dataset_ref.name().to_string())
        .build()
        .await
        .expect("Failed to build test environment");

    let dataset = test_env
        .daemon_server()
        .dataset_store()
        .get_dataset(&dataset_ref)
        .await
        .expect("Failed to load dataset");

    let block = dataset
        .start_block
        .expect("Dataset should have a start block");

    // Create reference snapshot from pre-loaded snapshot data
    let reference = {
        let ampctl = test_env.new_ampctl();
        let tables = test_helpers::restore_dataset_snapshot(
            &ampctl,
            test_env.daemon_server().dataset_store(),
            test_env.metadata_db(),
            &dataset_ref,
        )
        .await
        .expect("Failed to restore snapshot dataset");

        SnapshotContext::from_tables(test_env.daemon_server().config(), tables)
            .await
            .expect("Failed to create reference snapshot")
    };

    // * When
    // Dump the dataset and create a snapshot from it
    let dumped = {
        let dumped_tables = test_helpers::dump_dataset(
            test_env.daemon_server().config().clone(),
            test_env.metadata_db().clone(),
            dataset_ref,
            block,
        )
        .await
        .expect("Failed to dump dataset");

        SnapshotContext::from_tables(test_env.daemon_server().config(), dumped_tables)
            .await
            .expect("Failed to create temp dump snapshot")
    };

    //* Then
    // Validate table consistency
    for table in dumped.physical_tables() {
        test_helpers::check_table_consistency(table)
            .await
            .expect("Table consistency check failed");
    }

    // Compare snapshots
    test_helpers::assert_solana_snapshots_eq(&dumped, &reference).await;
}

#[tokio::test]
async fn eth_beacon_single_dump() {
    //* Given
    let test = TestCtx::setup(
        "eth_beacon_single_dump",
        "_/eth_beacon@0.0.0",
        "beacon_eth_mainnet",
    )
    .await;

    let block = test.get_dataset_start_block().await;
    let reference = test.restore_reference_snapshot().await;

    //* When
    let dumped = test.dump_and_create_snapshot(block).await;

    //* Then
    // Validate table consistency
    for table in dumped.physical_tables() {
        test_helpers::check_table_consistency(table)
            .await
            .expect("Table consistency check failed");
    }

    // Compare snapshots
    test_helpers::assert_snapshots_eq(&dumped, &reference).await;
}

#[tokio::test]
async fn evm_rpc_single_dump_fetch_receipts_per_tx() {
    //* Given
    let test = TestCtx::setup(
        "evm_rpc_single_dump_fetch_receipts_per_tx",
        "_/eth_rpc@0.0.0",
        "per_tx_receipt/rpc_eth_mainnet",
    )
    .await;

    let block = test.get_dataset_start_block().await;
    let reference = test.restore_reference_snapshot().await;

    //* When
    let dumped = test.dump_and_create_snapshot(block).await;

    //* Then
    // Validate table consistency
    for table in dumped.physical_tables() {
        test_helpers::check_table_consistency(table)
            .await
            .expect("Table consistency check failed");
    }

    // Compare snapshots
    test_helpers::assert_snapshots_eq(&dumped, &reference).await;
}

#[tokio::test]
async fn evm_rpc_base_single_dump() {
    //* Given
    let test = TestCtx::setup(
        "evm_rpc_base_single_dump",
        "_/base_rpc@0.0.0",
        "rpc_eth_base",
    )
    .await;

    let block = test.get_dataset_start_block().await;
    let reference = test.restore_reference_snapshot().await;

    //* When
    let dumped = test.dump_and_create_snapshot(block).await;

    //* Then
    // Validate table consistency
    for table in dumped.physical_tables() {
        test_helpers::check_table_consistency(table)
            .await
            .expect("Table consistency check failed");
    }

    // Compare snapshots
    test_helpers::assert_snapshots_eq(&dumped, &reference).await;
}

#[tokio::test]
async fn evm_rpc_base_single_dump_fetch_receipts_per_tx() {
    //* Given
    let test = TestCtx::setup(
        "evm_rpc_base_single_dump_fetch_receipts_per_tx",
        "_/base_rpc@0.0.0",
        "per_tx_receipt/rpc_eth_base",
    )
    .await;

    let block = test.get_dataset_start_block().await;
    let reference = test.restore_reference_snapshot().await;

    //* When
    let dumped = test.dump_and_create_snapshot(block).await;

    //* Then
    // Validate table consistency
    for table in dumped.physical_tables() {
        test_helpers::check_table_consistency(table)
            .await
            .expect("Table consistency check failed");
    }

    // Compare snapshots
    test_helpers::assert_snapshots_eq(&dumped, &reference).await;
}

#[tokio::test]
async fn eth_firehose_single_dump() {
    //* Given
    let test = TestCtx::setup(
        "eth_firehose_single_dump",
        "_/eth_firehose@0.0.0",
        "firehose_eth_mainnet",
    )
    .await;

    let block = test.get_dataset_start_block().await;
    let reference = test.restore_reference_snapshot().await;

    //* When
    let dumped = test.dump_and_create_snapshot(block).await;

    //* Then
    // Validate table consistency
    for table in dumped.physical_tables() {
        test_helpers::check_table_consistency(table)
            .await
            .expect("Table consistency check failed");
    }

    // Compare snapshots
    test_helpers::assert_snapshots_eq(&dumped, &reference).await;
}

#[tokio::test]
async fn base_firehose_single_dump() {
    //* Given
    let test = TestCtx::setup(
        "base_firehose_single_dump",
        "_/base_firehose@0.0.0",
        "firehose_eth_base",
    )
    .await;

    let block = test.get_dataset_start_block().await;
    let reference = test.restore_reference_snapshot().await;

    //* When
    let dumped = test.dump_and_create_snapshot(block).await;

    //* Then
    // Validate table consistency
    for table in dumped.physical_tables() {
        test_helpers::check_table_consistency(table)
            .await
            .expect("Table consistency check failed");
    }

    // Compare snapshots
    test_helpers::assert_snapshots_eq(&dumped, &reference).await;
}

/// Test context wrapper for dump-related tests.
///
/// This provides convenience methods for testing dataset dump functionality
/// with snapshot comparison.
struct TestCtx {
    ctx: testlib::ctx::TestCtx,
    dataset_ref: Reference,
}

impl TestCtx {
    /// Set up a new test context for dump testing.
    ///
    /// Creates a test environment with the specified dataset manifest,
    /// provider configuration, and snapshot data.
    async fn setup(test_name: &str, dataset: &str, provider: &str) -> Self {
        logging::init();

        let dataset_ref: Reference = dataset.parse().expect("Failed to parse dataset reference");

        let ctx = TestCtxBuilder::new(test_name)
            .with_dataset_manifest(dataset_ref.name().to_string())
            .with_provider_config(provider)
            .with_dataset_snapshot(dataset_ref.name().to_string())
            .build()
            .await
            .expect("Failed to build test environment");

        Self { ctx, dataset_ref }
    }

    /// Get the start block from the dataset.
    async fn get_dataset_start_block(&self) -> u64 {
        let dataset = self
            .ctx
            .daemon_server()
            .dataset_store()
            .get_dataset(&self.dataset_ref)
            .await
            .expect("Failed to load dataset");

        dataset
            .start_block
            .expect("Dataset should have a start block")
    }

    /// Restore reference snapshot from pre-loaded snapshot data.
    async fn restore_reference_snapshot(&self) -> SnapshotContext {
        let ampctl = self.ctx.new_ampctl();
        let tables = test_helpers::restore_dataset_snapshot(
            &ampctl,
            self.ctx.daemon_controller().dataset_store(),
            self.ctx.daemon_controller().metadata_db(),
            &self.dataset_ref,
        )
        .await
        .expect("Failed to restore snapshot dataset");

        SnapshotContext::from_tables(self.ctx.daemon_server().config(), tables)
            .await
            .expect("Failed to create reference snapshot")
    }

    /// Dump dataset and create snapshot from dumped tables.
    async fn dump_and_create_snapshot(&self, block: u64) -> SnapshotContext {
        let dumped_tables = test_helpers::dump_dataset(
            self.ctx.daemon_worker().config().clone(),
            self.ctx.daemon_worker().metadata_db().clone(),
            self.ctx.daemon_worker().dataset_store().clone(),
            self.dataset_ref.clone(),
            block,
        )
        .await
        .expect("Failed to dump dataset");

        SnapshotContext::from_tables(self.ctx.daemon_server().config(), dumped_tables)
            .await
            .expect("Failed to create dumped snapshot")
    }
}
