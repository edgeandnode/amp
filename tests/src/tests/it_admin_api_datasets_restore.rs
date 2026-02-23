use ampctl::client::{
    self,
    datasets::RestoreTableError,
    revisions::{RegisterError, RegisterResponse, RestoreError, RestoreResponse},
};
use datasets_common::reference::Reference;
use monitoring::logging;

use crate::testlib::ctx::TestCtxBuilder;

#[tokio::test]
async fn restore_table_from_uuid_path_succeeds() {
    logging::init();

    //* Given — register and restore a revision for blocks only
    let ctx = TestCtx::setup("restore_table_from_uuid_path_succeeds").await;
    let reg = ctx
        .register_revision("_/eth_rpc@0.0.0", "blocks", "eth_rpc_custom/blocks")
        .await
        .expect("failed to register blocks revision");
    ctx.restore_revision(reg.location_id)
        .await
        .expect("failed to restore blocks revision files");

    //* When — restore the blocks table via the dataset endpoint (UUID heuristic)
    let result = ctx.restore_table("blocks", None).await;

    //* Then — blocks queries succeed, logs queries fail
    assert!(
        result.is_ok(),
        "restore_table should succeed, got: {:?}",
        result.err()
    );

    let blocks = ctx
        .run_query("SELECT block_num FROM eth_rpc.blocks LIMIT 1")
        .await;
    assert!(
        blocks.is_ok(),
        "query on blocks should succeed after restore: {:?}",
        blocks.err()
    );

    // Query non restored table
    let logs = ctx
        .run_query("SELECT log_index FROM eth_rpc.logs LIMIT 1")
        .await;
    assert!(
        logs.is_err(),
        "query on logs should fail (not restored), but got: {:?}",
        logs.ok()
    );
}

#[tokio::test]
async fn restore_table_with_location_id_succeeds() {
    logging::init();

    //* Given — register and restore a revision for blocks, get its location_id
    let ctx = TestCtx::setup_with_custom_snapshots("restore_table_with_location_id_succeeds").await;
    let reg = ctx
        .register_revision("_/eth_rpc@0.0.0", "blocks", "eth_rpc_custom/blocks")
        .await
        .expect("failed to register blocks revision");
    ctx.restore_revision(reg.location_id)
        .await
        .expect("failed to restore blocks revision files");

    //* When — restore the blocks table using the known location_id
    let result = ctx.restore_table("blocks", Some(reg.location_id)).await;

    //* Then — blocks queries succeed, logs queries fail
    assert!(
        result.is_ok(),
        "restore_table with location_id should succeed, got: {:?}",
        result.err()
    );

    let blocks = ctx
        .run_query("SELECT block_num FROM eth_rpc.blocks LIMIT 1")
        .await;
    assert!(
        blocks.is_ok(),
        "query on blocks should succeed after restore: {:?}",
        blocks.err()
    );

    // Query non restored table
    let logs = ctx
        .run_query("SELECT log_index FROM eth_rpc.logs LIMIT 1")
        .await;
    assert!(
        logs.is_err(),
        "query on logs should fail (not restored), but got: {:?}",
        logs.ok()
    );
}

#[tokio::test]
async fn restore_table_with_nonexistent_table_returns_404() {
    logging::init();

    //* Given
    let ctx = TestCtx::setup("restore_table_with_nonexistent_table_returns_404").await;

    //* When
    let resp = ctx.restore_table("nonexistent_table", None).await;

    //* Then
    assert!(
        resp.is_err(),
        "restore with nonexistent table should return error"
    );
    let err = resp.expect_err("expected error response");
    match err {
        RestoreTableError::TableNotInManifest(api_err) => {
            assert_eq!(
                api_err.error_code, "TABLE_NOT_IN_MANIFEST",
                "Expected TABLE_NOT_IN_MANIFEST error code, got: {}",
                api_err.error_code
            );
        }
        _ => panic!("Expected TableNotInManifest error, got: {:?}", err),
    }
}

#[tokio::test]
async fn restore_table_with_nonexistent_location_id_returns_404() {
    logging::init();

    //* Given
    let ctx = TestCtx::setup("restore_table_with_nonexistent_location_id_returns_404").await;

    //* When
    let resp = ctx.restore_table("blocks", Some(999999)).await;

    //* Then
    assert!(
        resp.is_err(),
        "restore with nonexistent location_id should return error"
    );
    let err = resp.expect_err("expected error response");
    match err {
        RestoreTableError::RevisionNotFound(api_err) => {
            assert_eq!(
                api_err.error_code, "REVISION_NOT_FOUND",
                "Expected REVISION_NOT_FOUND error code, got: {}",
                api_err.error_code
            );
        }
        _ => panic!("Expected RevisionNotFound error, got: {:?}", err),
    }
}

#[tokio::test]
async fn restore_table_with_nonexistent_dataset_returns_404() {
    logging::init();

    //* Given
    let ctx = TestCtx::setup("restore_table_with_nonexistent_dataset_returns_404").await;
    let invalid_ref: Reference = "_/nonexistent@0.0.0".parse().expect("valid reference");

    //* When
    let resp = ctx
        .client
        .datasets()
        .restore_table(&invalid_ref, "blocks", None)
        .await;

    //* Then
    assert!(
        resp.is_err(),
        "restore with nonexistent dataset should return error"
    );
    let err = resp.expect_err("expected error response");
    match err {
        RestoreTableError::DatasetNotFound(api_err) => {
            assert_eq!(
                api_err.error_code, "DATASET_NOT_FOUND",
                "Expected DATASET_NOT_FOUND error code, got: {}",
                api_err.error_code
            );
        }
        _ => panic!("Expected DatasetNotFound error, got: {:?}", err),
    }
}

struct TestCtx {
    ctx: crate::testlib::ctx::TestCtx,
    client: client::Client,
}

impl TestCtx {
    /// Standard setup: eth_rpc manifest + eth_rpc snapshot.
    async fn setup(test_name: &str) -> Self {
        let ctx = TestCtxBuilder::new(test_name)
            .with_dataset_manifests(["eth_rpc"])
            .with_dataset_snapshots(["eth_rpc"])
            .build()
            .await
            .expect("failed to build test context");

        let base_url = ctx
            .daemon_controller()
            .admin_api_url()
            .parse()
            .expect("valid admin API URL");
        let client = client::Client::new(base_url);

        Self { ctx, client }
    }

    /// Setup with custom snapshots: eth_rpc manifest + eth_rpc_custom snapshot.
    async fn setup_with_custom_snapshots(test_name: &str) -> Self {
        let ctx = TestCtxBuilder::new(test_name)
            .with_dataset_manifests(["eth_rpc"])
            .with_dataset_snapshots(["eth_rpc_custom"])
            .build()
            .await
            .expect("failed to build test context");

        let base_url = ctx
            .daemon_controller()
            .admin_api_url()
            .parse()
            .expect("valid admin API URL");
        let client = client::Client::new(base_url);

        Self { ctx, client }
    }

    /// Restore a single table via the dataset restore_table endpoint.
    async fn restore_table(
        &self,
        table_name: &str,
        location_id: Option<i64>,
    ) -> Result<(), RestoreTableError> {
        let dataset_ref: Reference = "_/eth_rpc@0.0.0".parse().expect("valid reference");
        self.client
            .datasets()
            .restore_table(&dataset_ref, table_name, location_id)
            .await
    }

    /// Register a table revision via the revisions API.
    async fn register_revision(
        &self,
        dataset: &str,
        table_name: &str,
        path: &str,
    ) -> Result<RegisterResponse, RegisterError> {
        self.client
            .revisions()
            .register(dataset, table_name, path)
            .await
    }

    /// Restore a revision's files from object storage.
    async fn restore_revision(&self, location_id: i64) -> Result<RestoreResponse, RestoreError> {
        self.client.revisions().restore(location_id).await
    }

    /// Execute a SQL query via the Flight client.
    async fn run_query(&self, query: &str) -> Result<(serde_json::Value, usize), anyhow::Error> {
        let mut client = self
            .ctx
            .new_flight_client()
            .await
            .expect("failed to create flight client");
        client.run_query(query, None).await
    }
}
