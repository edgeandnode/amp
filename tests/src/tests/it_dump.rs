use std::{collections::BTreeSet, sync::Arc, time::Duration};

use common::physical_table::PhysicalTable;
use datasets_common::reference::Reference;
use pretty_assertions::assert_eq;
use serde_json::Value;

use crate::testlib::{self, ctx::TestCtxBuilder, helpers as test_helpers};

#[tokio::test]
async fn evm_rpc_single_dump() {
    //* Given
    let test = TestCtx::setup("evm_rpc_single_dump", "_/eth_rpc@0.0.0", "rpc_eth_mainnet").await;

    let block = test.get_dataset_start_block().await;
    let tables = test.restore_reference_snapshot().await;
    let reference_dataset = test.query_all_tables(&tables, "eth_rpc").await;
    test.deactivate_all_revisions(&tables).await;

    //* When
    test.dump_and_create_snapshot(block).await;

    //* Then
    // Validate table consistency
    for table in &tables {
        test_helpers::check_table_consistency(table, test.ctx.daemon_server().data_store())
            .await
            .expect("Table consistency check failed");
    }
    let dumped_dataset = test.query_all_tables(&tables, "eth_rpc").await;
    assert_eq!(
        reference_dataset.len(),
        dumped_dataset.len(),
        "table count mismatch"
    );
    for (reference, dumped) in reference_dataset.iter().zip(dumped_dataset.iter()) {
        assert_json_eq_ignoring_nulls(&reference.0, &dumped.0);
        assert_eq!(reference.1, dumped.1);
    }
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
    let tables = test.restore_reference_snapshot().await;
    let reference_dataset = test.query_all_tables(&tables, "eth_rpc").await;
    test.deactivate_all_revisions(&tables).await;

    //* When
    test.dump_and_create_snapshot(block).await;

    //* Then
    // Validate table consistency
    for table in &tables {
        test_helpers::check_table_consistency(table, test.ctx.daemon_server().data_store())
            .await
            .expect("Table consistency check failed");
    }
    let dumped_dataset = test.query_all_tables(&tables, "eth_rpc").await;
    assert_eq!(
        reference_dataset.len(),
        dumped_dataset.len(),
        "table count mismatch"
    );
    for (reference, dumped) in reference_dataset.iter().zip(dumped_dataset.iter()) {
        assert_json_eq_ignoring_nulls(&reference.0, &dumped.0);
        assert_eq!(reference.1, dumped.1);
    }
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
    let tables = test.restore_reference_snapshot().await;
    let reference_dataset = test.query_all_tables(&tables, "base_rpc").await;
    test.deactivate_all_revisions(&tables).await;

    //* When
    test.dump_and_create_snapshot(block).await;

    //* Then
    // Validate table consistency
    for table in &tables {
        test_helpers::check_table_consistency(table, test.ctx.daemon_server().data_store())
            .await
            .expect("Table consistency check failed");
    }
    let dumped_dataset = test.query_all_tables(&tables, "base_rpc").await;
    assert_eq!(
        reference_dataset.len(),
        dumped_dataset.len(),
        "table count mismatch"
    );
    for (reference, dumped) in reference_dataset.iter().zip(dumped_dataset.iter()) {
        assert_json_eq_ignoring_nulls(&reference.0, &dumped.0);
        assert_eq!(reference.1, dumped.1);
    }
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
    let tables = test.restore_reference_snapshot().await;
    let reference_dataset = test.query_all_tables(&tables, "base_rpc").await;
    test.deactivate_all_revisions(&tables).await;

    //* When
    test.dump_and_create_snapshot(block).await;

    //* Then
    // Validate table consistency
    for table in &tables {
        test_helpers::check_table_consistency(table, test.ctx.daemon_server().data_store())
            .await
            .expect("Table consistency check failed");
    }
    let dumped_dataset = test.query_all_tables(&tables, "base_rpc").await;
    assert_eq!(
        reference_dataset.len(),
        dumped_dataset.len(),
        "table count mismatch"
    );
    for (reference, dumped) in reference_dataset.iter().zip(dumped_dataset.iter()) {
        assert_json_eq_ignoring_nulls(&reference.0, &dumped.0);
        assert_eq!(reference.1, dumped.1);
    }
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
    let tables = test.restore_reference_snapshot().await;
    let reference_dataset = test.query_all_tables(&tables, "eth_firehose").await;
    test.deactivate_all_revisions(&tables).await;

    //* When
    test.dump_and_create_snapshot(block).await;

    //* Then
    // Validate table consistency
    for table in &tables {
        test_helpers::check_table_consistency(table, test.ctx.daemon_server().data_store())
            .await
            .expect("Table consistency check failed");
    }
    let dumped_dataset = test.query_all_tables(&tables, "eth_firehose").await;
    assert_eq!(
        reference_dataset.len(),
        dumped_dataset.len(),
        "table count mismatch"
    );
    for (reference, dumped) in reference_dataset.iter().zip(dumped_dataset.iter()) {
        assert_json_eq_ignoring_nulls(&reference.0, &dumped.0);
        assert_eq!(reference.1, dumped.1);
    }
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
    let tables = test.restore_reference_snapshot().await;
    let reference_dataset = test.query_all_tables(&tables, "base_firehose").await;
    test.deactivate_all_revisions(&tables).await;

    //* When
    test.dump_and_create_snapshot(block).await;

    //* Then
    // Validate table consistency
    for table in &tables {
        test_helpers::check_table_consistency(table, test.ctx.daemon_server().data_store())
            .await
            .expect("Table consistency check failed");
    }
    let dumped_dataset = test.query_all_tables(&tables, "base_firehose").await;
    assert_eq!(
        reference_dataset.len(),
        dumped_dataset.len(),
        "table count mismatch"
    );
    for (reference, dumped) in reference_dataset.iter().zip(dumped_dataset.iter()) {
        assert_json_eq_ignoring_nulls(&reference.0, &dumped.0);
        assert_eq!(reference.1, dumped.1);
    }
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
        let hash_ref = self
            .ctx
            .daemon_server()
            .dataset_store()
            .resolve_revision(&self.dataset_ref)
            .await
            .expect("Failed to resolve dataset reference")
            .expect("Dataset not found");

        let dataset = self
            .ctx
            .daemon_server()
            .dataset_store()
            .get_dataset(&hash_ref)
            .await
            .expect("Failed to load dataset");

        dataset
            .start_block()
            .expect("Dataset should have a start block")
    }

    /// Restore reference snapshot from pre-loaded snapshot data.
    async fn restore_reference_snapshot(&self) -> Vec<Arc<PhysicalTable>> {
        let ampctl = self.ctx.new_ampctl();
        test_helpers::restore_dataset_snapshot(
            &ampctl,
            self.ctx.daemon_controller().dataset_store(),
            self.ctx.daemon_server().data_store(),
            &self.dataset_ref,
        )
        .await
        .expect("Failed to restore snapshot dataset")
    }

    /// Dump dataset via worker/scheduler and create snapshot from dumped tables.
    ///
    /// This method exercises the full production code path by scheduling a job
    /// via the Admin API and waiting for the worker to complete it.
    async fn dump_and_create_snapshot(&self, block: u64) -> Vec<Arc<PhysicalTable>> {
        let ampctl = self.ctx.new_ampctl();

        // Deploy via scheduler/worker
        test_helpers::deploy_and_wait(
            &ampctl,
            &self.dataset_ref,
            Some(block),
            Duration::from_secs(60),
        )
        .await
        .expect("Failed to dump dataset via worker");

        // Load physical tables from dataset store
        test_helpers::load_physical_tables(
            self.ctx.daemon_server().dataset_store(),
            self.ctx.daemon_server().data_store(),
            &self.dataset_ref,
        )
        .await
        .expect("Failed to load physical tables")
    }

    /// Query all tables in the given schema and return the results.
    async fn query_all_tables(
        &self,
        tables: &[Arc<PhysicalTable>],
        schema: &str,
    ) -> Vec<(serde_json::Value, usize)> {
        let mut dataset = Vec::new();
        for table in tables {
            let result = self
                .run_query(&format!(
                    "SELECT * FROM {}.{} ORDER BY block_num",
                    schema,
                    table.table_name()
                ))
                .await
                .expect("Failed to run query");
            dataset.push(result);
        }
        dataset
    }

    /// Deactivate all table revisions for the current dataset.
    async fn deactivate_all_revisions(&self, tables: &[Arc<PhysicalTable>]) {
        let ampctl = self.ctx.new_ampctl();
        let dataset = self.dataset_ref.to_string();
        for table in tables {
            ampctl
                .revisions()
                .deactivate(&dataset, table.table_name().as_ref())
                .await
                .expect("Failed to deactivate revision");
        }
    }

    /// Execute a SQL query via the Flight SQL client.
    async fn run_query(&self, query: &str) -> Result<(serde_json::Value, usize), anyhow::Error> {
        let mut client = self
            .ctx
            .new_flight_client()
            .await
            .expect("failed to create flight client");
        client.run_query(query, None).await
    }
}

/// Compare two JSON arrays row-by-row, ignoring fields where either side is null.
///
/// External data sources (e.g., RPC providers) may intermittently omit nullable fields
/// such as `total_difficulty` (deprecated post-EIP-3675). This comparison skips any field
/// where either the reference or dumped value is null, while still catching mismatches
/// on fields where both sides have non-null values.
fn assert_json_eq_ignoring_nulls(reference: &Value, dumped: &Value) {
    let ref_rows = reference
        .as_array()
        .expect("reference should be a JSON array");
    let dump_rows = dumped.as_array().expect("dumped should be a JSON array");
    assert_eq!(ref_rows.len(), dump_rows.len(), "row count mismatch");

    for (i, (ref_row, dump_row)) in ref_rows.iter().zip(dump_rows.iter()).enumerate() {
        let ref_obj = ref_row
            .as_object()
            .expect("reference row should be a JSON object");
        let dump_obj = dump_row
            .as_object()
            .expect("dumped row should be a JSON object");

        let all_keys: BTreeSet<_> = ref_obj.keys().chain(dump_obj.keys()).collect();
        for key in all_keys {
            let ref_val = ref_obj.get(key).unwrap_or(&Value::Null);
            let dump_val = dump_obj.get(key).unwrap_or(&Value::Null);
            if ref_val.is_null() || dump_val.is_null() {
                continue;
            }
            assert_eq!(ref_val, dump_val, "mismatch at row {i}, field \"{key}\"");
        }
    }
}
