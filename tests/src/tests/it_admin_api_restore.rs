use datasets_common::reference::Reference;
use monitoring::logging;

use crate::testlib::{self, ctx::TestCtxBuilder, fixtures::Ampctl};

/// Test restoring a dataset from legacy storage paths (without namespace prefix).
///
/// The `eth_rpc` snapshot is stored at `eth_rpc/blocks/UUID/file.parquet` (no namespace).
/// The restore handler first tries the namespace-prefixed path, falls back to the legacy path.
#[tokio::test]
async fn restore_dataset_from_legacy_path_succeeds() {
    logging::init();

    //* Given
    let ctx = TestCtx::setup("restore_dataset_from_legacy_path_succeeds").await;

    //* When
    let restored_tables = ctx.restore_dataset().await;

    //* Then
    assert_eq!(
        restored_tables.len(),
        3,
        "should restore 3 tables (blocks, logs, transactions)"
    );

    let table_names: Vec<&str> = restored_tables
        .iter()
        .map(|t| t.table_name.as_str())
        .collect();
    assert!(
        table_names.contains(&"blocks"),
        "should contain blocks table"
    );
    assert!(table_names.contains(&"logs"), "should contain logs table");
    assert!(
        table_names.contains(&"transactions"),
        "should contain transactions table"
    );

    for table in &restored_tables {
        assert!(table.location_id > 0, "location_id should be positive");
        assert!(!table.url.is_empty(), "url should not be empty");
    }

    // Verify tables are queryable
    let (_, row_count) = ctx
        .run_query("SELECT block_num FROM eth_rpc.blocks LIMIT 1")
        .await
        .expect("blocks table should be queryable after restore");
    assert!(row_count > 0, "blocks table should have data");
}

/// Test restoring a dataset from custom registered revision paths.
///
/// Uses `eth_rpc_custom` snapshot with pre-registered revision paths.
/// The restore handler finds existing revisions via `get_table_revision` and re-activates them.
#[tokio::test]
async fn restore_dataset_from_custom_path_succeeds() {
    logging::init();

    //* Given — register revisions at custom paths
    let ctx =
        TestCtx::setup_with_custom_snapshot("restore_dataset_from_custom_path_succeeds").await;

    let tables = ["blocks", "logs", "transactions"];
    for table_name in &tables {
        ctx.ampctl
            .revisions()
            .register(
                "_/eth_rpc@0.0.0",
                table_name,
                &format!("eth_rpc_custom/{}", table_name),
            )
            .await
            .unwrap_or_else(|err| {
                panic!("failed to register revision for {}: {}", table_name, err)
            });
    }

    //* When
    let restored_tables = ctx.restore_dataset().await;

    //* Then
    assert_eq!(
        restored_tables.len(),
        3,
        "should restore 3 tables (blocks, logs, transactions)"
    );

    let table_names: Vec<&str> = restored_tables
        .iter()
        .map(|t| t.table_name.as_str())
        .collect();
    assert!(
        table_names.contains(&"blocks"),
        "should contain blocks table"
    );
    assert!(table_names.contains(&"logs"), "should contain logs table");
    assert!(
        table_names.contains(&"transactions"),
        "should contain transactions table"
    );

    for table in &restored_tables {
        assert!(table.location_id > 0, "location_id should be positive");
        assert!(!table.url.is_empty(), "url should not be empty");
    }

    // Verify tables are queryable
    let (_, row_count) = ctx
        .run_query("SELECT block_num FROM eth_rpc.blocks LIMIT 1")
        .await
        .expect("blocks table should be queryable after restore");
    assert!(row_count > 0, "blocks table should have data");
}

struct TestCtx {
    ctx: testlib::ctx::TestCtx,
    ampctl: Ampctl,
}

impl TestCtx {
    /// Setup for legacy path test: manifest + legacy snapshot (no namespace prefix).
    async fn setup(test_name: &str) -> Self {
        let ctx = TestCtxBuilder::new(test_name)
            .with_dataset_manifests(["eth_rpc"])
            .with_dataset_snapshots(["eth_rpc"])
            .build()
            .await
            .expect("failed to build test context");

        let ampctl = ctx.new_ampctl();

        Self { ctx, ampctl }
    }

    /// Setup for custom path test: manifest + custom snapshot.
    async fn setup_with_custom_snapshot(test_name: &str) -> Self {
        let ctx = TestCtxBuilder::new(test_name)
            .with_dataset_manifests(["eth_rpc"])
            .with_dataset_snapshots(["eth_rpc_custom"])
            .build()
            .await
            .expect("failed to build test context");

        let ampctl = ctx.new_ampctl();

        Self { ctx, ampctl }
    }

    fn dataset_ref(&self) -> Reference {
        "_/eth_rpc@0.0.0".parse().expect("valid reference")
    }

    async fn restore_dataset(&self) -> Vec<ampctl::client::datasets::RestoredTableInfo> {
        self.ampctl
            .restore_dataset(&self.dataset_ref())
            .await
            .expect("failed to restore dataset")
    }

    async fn run_query(&self, query: &str) -> Result<(serde_json::Value, usize), anyhow::Error> {
        let mut client = self
            .ctx
            .new_flight_client()
            .await
            .expect("failed to create flight client");
        client.run_query(query, None).await
    }
}
