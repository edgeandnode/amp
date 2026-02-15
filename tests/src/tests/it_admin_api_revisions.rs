use ampctl::client::revisions::{ActivateError, DeactivateError, GetByIdError, RevisionInfo};
use datasets_common::reference::Reference;
use monitoring::logging;

use crate::testlib::{ctx::TestCtxBuilder, fixtures::Ampctl};

#[tokio::test]
async fn deactivate_revision_for_active_table_succeeds() {
    logging::init();

    //* Given
    let ctx = TestCtx::setup("deactivate_revision_for_active_table_succeeds").await;
    ctx.restore_dataset().await;

    //* When
    ctx.deactivate_revision("_/eth_rpc@0.0.0", "blocks")
        .await
        .expect("failed to deactivate revision");

    //* Then
    let result = ctx
        .run_query("SELECT block_num FROM eth_rpc.blocks LIMIT 1")
        .await;
    assert!(
        result.is_err(),
        "query should fail after deactivation, but got: {:?}",
        result.ok()
    );
}

#[tokio::test]
async fn activate_revision_after_deactivation() {
    logging::init();

    //* Given
    let ctx = TestCtx::setup("activate_revision_after_deactivation").await;
    let restored_tables = ctx.restore_dataset().await;
    let location_id = TestCtx::blocks_location_id(&restored_tables);
    ctx.deactivate_and_verify(
        "_/eth_rpc@0.0.0",
        "blocks",
        "SELECT block_num FROM eth_rpc.blocks LIMIT 1",
    )
    .await;

    //* When
    ctx.activate_revision("_/eth_rpc@0.0.0", "blocks", location_id)
        .await
        .expect("failed to activate revision");

    //* Then
    let result = ctx
        .run_query("SELECT block_num FROM eth_rpc.blocks LIMIT 1")
        .await;
    assert!(
        result.is_ok(),
        "query should succeed after reactivation: {:?}",
        result.err()
    );
}

#[tokio::test]
async fn activate_revision_with_nonexistent_table_name_returns_404() {
    logging::init();

    //* Given
    let ctx = TestCtx::setup("activate_revision_with_nonexistent_table_name_returns_404").await;
    ctx.restore_dataset().await;

    //* When
    let resp = ctx
        .activate_revision("_/eth_rpc@0.0.0", "nonexistent_table", 1)
        .await;

    //* Then
    assert!(
        resp.is_err(),
        "activate with nonexistent table name should return error"
    );
    let err = resp.unwrap_err();
    match err {
        ActivateError::TableNotFound(api_err) => {
            assert_eq!(
                api_err.error_code, "TABLE_NOT_FOUND",
                "Expected TABLE_NOT_FOUND error code, got: {}",
                api_err.error_code
            );
            assert_eq!(
                api_err.error_message,
                "Table 'nonexistent_table' not found for dataset '_/eth_rpc@0.0.0'",
                "Expected error message, got: {}",
                api_err.error_message
            );
        }
        _ => panic!("Expected TableNotFound error, got: {:?}", err),
    }
}

#[tokio::test]
async fn deactivate_revision_with_nonexistent_table_name_returns_404() {
    logging::init();

    //* Given
    let ctx = TestCtx::setup("deactivate_revision_with_nonexistent_table_name_returns_404").await;
    ctx.restore_dataset().await;

    //* When
    let resp = ctx
        .deactivate_revision("_/eth_rpc@0.0.0", "nonexistent_table")
        .await;

    //* Then
    assert!(
        resp.is_err(),
        "deactivate with nonexistent table name should return error"
    );
    let err = resp.unwrap_err();
    match err {
        DeactivateError::TableNotFound(api_err) => {
            assert_eq!(
                api_err.error_code, "TABLE_NOT_FOUND",
                "Expected TABLE_NOT_FOUND error code, got: {}",
                api_err.error_code
            );
            assert_eq!(
                api_err.error_message,
                "Table 'nonexistent_table' not found for dataset '_/eth_rpc@0.0.0'",
                "Expected error message, got: {}",
                api_err.error_message
            );
        }
        _ => panic!("Expected TableNotFound error, got: {:?}", err),
    }
}

#[tokio::test]
async fn activate_fails_with_negative_location_id() {
    logging::init();

    //* Given
    let ctx = TestCtx::setup("activate_fails_with_negative_location_id").await;
    ctx.restore_dataset().await;

    //* When
    let resp = ctx.activate_revision("_/eth_rpc@0.0.0", "blocks", -1).await;

    //* Then
    assert!(
        resp.is_err(),
        "activate with negative location id should return error"
    );
    let err = resp.unwrap_err();
    match err {
        ActivateError::InvalidPath(api_err) => {
            assert_eq!(
                api_err.error_code, "INVALID_PATH_PARAMETERS",
                "Expected INVALID_PATH_PARAMETERS error code, got: {}",
                api_err.error_code
            );
            assert!(
                api_err
                    .error_message
                    .contains("LocationId must be positive"),
                "Expected error message, got: {}",
                api_err.error_message
            );
        }
        _ => panic!("Expected InvalidPath error, got: {:?}", err),
    }
}

#[tokio::test]
async fn get_revision_by_location_id_succeeds() {
    logging::init();

    //* Given
    let ctx = TestCtx::setup("get_revision_by_location_id_succeeds").await;
    let restored_tables = ctx.restore_dataset().await;
    let location_id = TestCtx::blocks_location_id(&restored_tables);

    //* When
    let resp = ctx
        .get_revision(location_id)
        .await
        .expect("failed to get revision");

    //* Then
    assert!(resp.is_some(), "get revision should return some revision");
    let revision = resp.unwrap();
    assert_eq!(
        revision.id, location_id,
        "returned revision id should match requested location_id"
    );
    assert!(revision.active, "revision should be active after restore");
}

#[tokio::test]
async fn get_revision_with_nonexistent_id_returns_404() {
    logging::init();

    //* Given
    let ctx = TestCtx::setup("get_revision_with_nonexistent_id_returns_404").await;
    ctx.restore_dataset().await;

    //* When
    let resp = ctx
        .get_revision(999999)
        .await
        .expect("failed to get revision");

    //* Then
    assert!(resp.is_none(), "get with nonexistent id should return none");
}

#[tokio::test]
async fn get_revision_reflects_deactivation() {
    logging::init();

    //* Given
    let ctx = TestCtx::setup("get_revision_reflects_deactivation").await;
    let restored_tables = ctx.restore_dataset().await;
    let location_id = TestCtx::blocks_location_id(&restored_tables);

    // Deactivate the blocks table
    ctx.deactivate_revision("_/eth_rpc@0.0.0", "blocks")
        .await
        .expect("failed to deactivate revision");

    //* When
    let resp = ctx
        .get_revision(location_id)
        .await
        .expect("failed to get revision");

    //* Then
    assert!(
        resp.is_some(),
        "get revision after deactivation should return some revision"
    );
    let revision = resp.unwrap();
    assert!(
        !revision.active,
        "revision should be inactive after deactivation"
    );
}

#[tokio::test]
async fn get_revision_with_invalid_id_returns_400() {
    logging::init();

    //* Given
    let ctx = TestCtx::setup("get_revision_with_invalid_id_returns_400").await;
    ctx.restore_dataset().await;

    //* When
    let resp = ctx.get_revision(-1).await;

    //* Then
    assert!(resp.is_err(), "get with invalid id should return error");
    let err = resp.unwrap_err();
    match err {
        GetByIdError::InvalidPath(api_err) => {
            assert_eq!(
                api_err.error_code, "INVALID_PATH_PARAMETERS",
                "Expected INVALID_PATH_PARAMETERS error code, got: {}",
                api_err.error_code
            );
        }
        _ => panic!("Expected InvalidPath error, got: {:?}", err),
    }
}

struct TestCtx {
    ctx: crate::testlib::ctx::TestCtx,
    ampctl_client: Ampctl,
}

impl TestCtx {
    async fn setup(test_name: &str) -> Self {
        let ctx = TestCtxBuilder::new(test_name)
            .with_dataset_manifests(["eth_rpc"])
            .with_dataset_snapshots(["eth_rpc"])
            .build()
            .await
            .expect("failed to build test context");

        let ampctl = ctx.new_ampctl();

        Self {
            ctx,
            ampctl_client: ampctl,
        }
    }

    /// Restores the `eth_rpc` dataset and returns info about the restored tables.
    async fn restore_dataset(&self) -> Vec<ampctl::client::datasets::RestoredTableInfo> {
        let dataset_ref: Reference = "_/eth_rpc@0.0.0".parse().expect("valid reference");
        self.ampctl_client
            .restore_dataset(&dataset_ref)
            .await
            .expect("failed to restore dataset")
    }

    /// Extracts the `location_id` of the "blocks" table from the restored tables list.
    fn blocks_location_id(restored_tables: &[ampctl::client::datasets::RestoredTableInfo]) -> i64 {
        restored_tables
            .iter()
            .find(|t| t.table_name == "blocks")
            .expect("blocks table should exist in restored tables")
            .location_id
    }

    /// Deactivates a revision and asserts that the given query fails afterwards.
    async fn deactivate_and_verify(&self, dataset: &str, table_name: &str, query: &str) {
        self.deactivate_revision(dataset, table_name)
            .await
            .expect("failed to deactivate revision");
        let result = self.run_query(query).await;
        assert!(
            result.is_err(),
            "query should fail after deactivation, but got: {:?}",
            result.ok()
        );
    }

    /// Deactivates the revision for the given dataset and table.
    async fn deactivate_revision(
        &self,
        dataset: &str,
        table_name: &str,
    ) -> Result<(), DeactivateError> {
        self.ampctl_client
            .revisions()
            .deactivate(dataset, table_name)
            .await
    }

    /// Activates a revision at the given location for the specified dataset and table.
    async fn activate_revision(
        &self,
        dataset: &str,
        table_name: &str,
        location_id: i64,
    ) -> Result<(), ActivateError> {
        self.ampctl_client
            .revisions()
            .activate(location_id, dataset, table_name)
            .await
    }

    /// Fetches a revision by its location ID, returning `None` if not found.
    async fn get_revision(&self, location_id: i64) -> Result<Option<RevisionInfo>, GetByIdError> {
        self.ampctl_client.revisions().get_by_id(location_id).await
    }

    /// Executes a SQL query via the Flight client and returns the result with row count.
    async fn run_query(&self, query: &str) -> Result<(serde_json::Value, usize), anyhow::Error> {
        let mut client = self
            .ctx
            .new_flight_client()
            .await
            .expect("failed to create flight client");
        client.run_query(query, None).await
    }
}
