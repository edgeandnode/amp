use ampctl::client::revisions::{
    ActivateError, DeactivateError, GetByIdError, ListError, RegisterError, RegisterResponse,
    RevisionInfo,
};
use datasets_common::reference::Reference;
use monitoring::logging;

use crate::testlib::{ctx::TestCtxBuilder, fixtures::Ampctl};

#[tokio::test]
async fn list_revisions_for_restored_dataset_succeeds() {
    logging::init();

    //* Given
    let ctx = TestCtx::setup("list_revisions_for_restored_dataset_succeeds").await;
    ctx.restore_dataset().await;

    //* When
    let revisions = ctx
        .list_revisions(None, None)
        .await
        .expect("failed to list revisions");

    //* Then
    assert!(
        !revisions.is_empty(),
        "revisions should not be empty for a restored dataset"
    );
}

#[tokio::test]
async fn list_revisions_with_active_filter() {
    logging::init();

    //* Given
    let ctx = TestCtx::setup("list_revisions_with_active_filter").await;
    ctx.restore_dataset().await;

    ctx.deactivate_revision("_/eth_rpc@0.0.0", "blocks")
        .await
        .expect("failed to deactivate revision");

    //* When — filter active only
    let active_revisions = ctx
        .list_revisions(Some(true), None)
        .await
        .expect("failed to list active revisions");
    let inactive_revisions = ctx
        .list_revisions(Some(false), None)
        .await
        .expect("failed to list inactive revisions");

    //* Then
    assert!(
        !active_revisions.is_empty(),
        "should have at least one active revision after restore"
    );
    assert!(
        active_revisions.iter().all(|r| r.active),
        "all revisions returned with active=true filter should be active"
    );
    assert!(
        !inactive_revisions.is_empty(),
        "should have at least one inactive revision after deactivation"
    );
    assert!(
        inactive_revisions.iter().all(|r| !r.active),
        "all revisions returned with active=false filter should be inactive"
    );
}

#[tokio::test]
async fn list_revisions_with_limit_succeeds() {
    logging::init();

    //* Given
    let ctx = TestCtx::setup("list_revisions_with_limit_succeeds").await;
    ctx.restore_dataset().await;

    //* When
    let revisions = ctx
        .list_revisions(None, Some(1))
        .await
        .expect("failed to list revisions with limit");

    //* Then
    assert!(
        revisions.len() <= 1,
        "revisions count should be at most 1 when limit=1, got {}",
        revisions.len()
    );
}

#[tokio::test]
async fn list_revisions_with_negative_limit_returns_400() {
    logging::init();

    //* Given
    let ctx = TestCtx::setup("list_revisions_with_negative_limit_returns_400").await;
    ctx.restore_dataset().await;

    //* When
    let resp = ctx.list_revisions(None, Some(-1)).await;

    //* Then
    assert!(resp.is_err(), "negative limit should return error");
    let err = resp.unwrap_err();
    match err {
        ListError::InvalidQueryParams(api_err) => {
            assert_eq!(
                api_err.error_code, "INVALID_QUERY_PARAMETERS",
                "Expected INVALID_QUERY_PARAMETERS error code, got: {}",
                api_err.error_code
            );
        }
        _ => panic!("Expected InvalidQueryParams error, got: {:?}", err),
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

#[tokio::test]
async fn register_revision_for_non_registered_dataset_fails() {
    logging::init();

    //* Given
    let ctx = TestCtx::setup_minimal("register_revision_for_non_registered_dataset_fails").await;

    //* When
    let resp = ctx
        .register_revision("_/nonexistent@0.0.0", "blocks", "some/path")
        .await;

    //* Then
    assert!(
        resp.is_err(),
        "register revision for non-registered dataset should fail"
    );
    let err = resp.unwrap_err();
    match err {
        RegisterError::DatasetNotFound(api_err) => {
            assert_eq!(
                api_err.error_code, "DATASET_NOT_FOUND",
                "Expected DATASET_NOT_FOUND error code, got: {}",
                api_err.error_code
            );
        }
        _ => panic!("Expected DatasetNotFound error, got: {:?}", err),
    }
}

#[tokio::test]
async fn register_revision_succeeds() {
    logging::init();

    //* Given
    let ctx = TestCtx::setup("register_revision_succeeds").await;

    // Create revisions for each table pointing to eth_rpc_custom paths
    let path = "eth_rpc_custom/blocks";
    let res = ctx
        .register_revision("_/eth_rpc@0.0.0", "blocks", path)
        .await
        .expect("failed to register revision");

    //* When
    let restored_tables = ctx
        .get_revision(res.location_id)
        .await
        .expect("failed to get revision");

    //* Then
    assert!(
        restored_tables.is_some(),
        "get revision should return some revision"
    );
    let revision = restored_tables.unwrap();
    assert_eq!(revision.path, path, "revision path should match");
    assert!(!revision.active, "revision should be inactive");
}

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
        ActivateError::TableNotInManifest(api_err) => {
            assert_eq!(
                api_err.error_code, "TABLE_NOT_IN_MANIFEST",
                "Expected TABLE_NOT_IN_MANIFEST error code, got: {}",
                api_err.error_code
            );
            assert_eq!(
                api_err.error_message,
                "Table 'nonexistent_table' not found in manifest for dataset '_/eth_rpc@0.0.0'",
                "Expected error message, got: {}",
                api_err.error_message
            );
        }
        _ => panic!("Expected TableNotInManifest error, got: {:?}", err),
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

    /// Setup with no manifests or snapshots — bare test environment.
    async fn setup_minimal(test_name: &str) -> Self {
        let ctx = TestCtxBuilder::new(test_name)
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

    /// Lists all revisions, optionally filtered by active status and limit.
    async fn list_revisions(
        &self,
        active: Option<bool>,
        limit: Option<i64>,
    ) -> Result<Vec<RevisionInfo>, ListError> {
        self.ampctl_client.revisions().list(active, limit).await
    }

    /// Fetches a revision by its location ID, returning `None` if not found.
    async fn get_revision(&self, location_id: i64) -> Result<Option<RevisionInfo>, GetByIdError> {
        self.ampctl_client.revisions().get_by_id(location_id).await
    }

    /// Registers a table revision.
    async fn register_revision(
        &self,
        dataset: &str,
        table_name: &str,
        path: &str,
    ) -> Result<RegisterResponse, RegisterError> {
        self.ampctl_client
            .revisions()
            .register(dataset, table_name, path)
            .await
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
