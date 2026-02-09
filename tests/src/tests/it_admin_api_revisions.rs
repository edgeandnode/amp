use datasets_common::reference::Reference;
use monitoring::logging;

use crate::testlib::ctx::TestCtxBuilder;

#[tokio::test]
async fn deactivate_revision_for_active_table_succeeds() {
    logging::init();

    //* Given
    let ctx = TestCtx::setup("deactivate_revision_for_active_table_succeeds").await;
    ctx.restore_dataset().await;

    //* When
    let resp = ctx.deactivate_revision("_/eth_rpc@0.0.0", "blocks").await;

    //* Then
    assert!(
        resp.status().is_success(),
        "deactivate setup failed with status: {}",
        resp.status()
    );
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
    let resp = ctx
        .activate_revision("_/eth_rpc@0.0.0", "blocks", location_id)
        .await;

    //* Then
    assert!(
        resp.status().is_success(),
        "activate setup failed with status: {}",
        resp.status()
    );
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
    let body = resp
        .json::<serde_json::Value>()
        .await
        .expect("failed to parse response body");
    assert_eq!(
        body["error_code"].as_str().unwrap(),
        "TABLE_NOT_FOUND",
        "deactivate with nonexistent table name should return TABLE_NOT_FOUND, got: {}",
        body["error_code"].as_str().unwrap()
    );
    assert!(
        body["error_message"]
            .as_str()
            .unwrap()
            .contains("Table 'nonexistent_table' not found for dataset '_/eth_rpc@0.0.0'"),
        "deactivate with nonexistent table name should return contains error message, got: {}",
        body["error_message"].as_str().unwrap()
    );
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
    let body = resp
        .json::<serde_json::Value>()
        .await
        .expect("failed to parse response body");
    assert_eq!(
        body["error_code"].as_str().unwrap(),
        "TABLE_NOT_FOUND",
        "deactivate with nonexistent table name should return TABLE_NOT_FOUND, got: {}",
        body["error_code"].as_str().unwrap()
    );
    assert!(
        body["error_message"]
            .as_str()
            .unwrap()
            .contains("Table 'nonexistent_table' not found for dataset '_/eth_rpc@0.0.0'"),
        "deactivate with nonexistent table name should return contains error message, got: {}",
        body["error_message"].as_str().unwrap()
    );
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
    let body = resp
        .json::<serde_json::Value>()
        .await
        .expect("failed to parse response body");
    assert!(
        body["error_message"]
            .as_str()
            .unwrap()
            .contains("LocationId must be positive"),
        "activate with negative location id should return contains error message, got: {}",
        body["error_message"].as_str().unwrap()
    );
}

struct TestCtx {
    ctx: crate::testlib::ctx::TestCtx,
    http_client: reqwest::Client,
    admin_url: String,
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
        let admin_url = ampctl.admin_url().to_string();
        let http_client = reqwest::Client::new();

        Self {
            ctx,
            http_client,
            admin_url,
        }
    }

    async fn restore_dataset(&self) -> Vec<ampctl::client::datasets::RestoredTableInfo> {
        let ampctl = self.ctx.new_ampctl();
        let dataset_ref: Reference = "_/eth_rpc@0.0.0".parse().expect("valid reference");
        ampctl
            .restore_dataset(&dataset_ref)
            .await
            .expect("failed to restore dataset")
    }

    fn blocks_location_id(restored_tables: &[ampctl::client::datasets::RestoredTableInfo]) -> i64 {
        restored_tables
            .iter()
            .find(|t| t.table_name == "blocks")
            .expect("blocks table should exist in restored tables")
            .location_id
    }

    async fn deactivate_and_verify(&self, dataset: &str, table_name: &str, query: &str) {
        let resp = self.deactivate_revision(dataset, table_name).await;
        assert!(
            resp.status().is_success(),
            "deactivate setup failed with status: {}",
            resp.status()
        );
        let result = self.run_query(query).await;
        assert!(
            result.is_err(),
            "query should fail after deactivation, but got: {:?}",
            result.ok()
        );
    }

    async fn deactivate_revision(&self, dataset: &str, table_name: &str) -> reqwest::Response {
        let payload = serde_json::json!({
            "dataset": dataset,
            "table_name": table_name,
        });
        self.http_client
            .post(format!("{}revisions/deactivate", self.admin_url))
            .json(&payload)
            .send()
            .await
            .expect("failed to send deactivate request")
    }

    async fn activate_revision(
        &self,
        dataset: &str,
        table_name: &str,
        location_id: i64,
    ) -> reqwest::Response {
        let payload = serde_json::json!({
            "dataset": dataset,
            "table_name": table_name,
        });
        self.http_client
            .post(format!(
                "{}revisions/{location_id}/activate",
                self.admin_url
            ))
            .json(&payload)
            .send()
            .await
            .expect("failed to send activate request")
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
