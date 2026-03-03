use std::time::Duration;

use datasets_common::reference::Reference;
use monitoring::logging;

use crate::testlib::{
    self, ctx::TestCtxBuilder, fixtures::DatasetPackage, helpers as test_helpers,
};

#[tokio::test]
async fn dataset_query_with_semver_ref_returns_data() {
    //* Given
    let mut ctx = TestCtx::new("dataset_ref_semver").await;
    ctx.restore_eth_firehose_snapshot().await;
    ctx.register_multi_version_dataset("0.0.1").await;
    ctx.materialize_multi_version_dataset("_/multi_version@0.0.1")
        .await;

    //* When
    let (results, _) = ctx
        .query(r#"SELECT block_num FROM "_/multi_version@0.0.1".blocks ORDER BY block_num LIMIT 1"#)
        .await;

    //* Then
    assert_eq!(results, serde_json::json!([{"block_num": 15000000}]));
}

#[tokio::test]
async fn dataset_query_with_bare_name_ref_returns_latest() {
    //* Given
    let mut ctx = TestCtx::new("dataset_ref_latest").await;
    ctx.restore_eth_firehose_snapshot().await;
    ctx.register_multi_version_dataset("0.0.1").await;
    ctx.materialize_multi_version_dataset("_/multi_version@0.0.1")
        .await;

    //* When
    let (results, _) = ctx
        .query("SELECT block_num FROM multi_version.blocks ORDER BY block_num LIMIT 1")
        .await;

    //* Then
    assert_eq!(results, serde_json::json!([{"block_num": 15000000}]));
}

#[tokio::test]
async fn dataset_query_with_explicit_latest_ref_returns_data() {
    //* Given
    let mut ctx = TestCtx::new("dataset_ref_latest_explicit").await;
    ctx.restore_eth_firehose_snapshot().await;
    ctx.register_multi_version_dataset("0.0.1").await;
    ctx.materialize_multi_version_dataset("_/multi_version@0.0.1")
        .await;

    //* When
    let (results, _) = ctx
        .query(
            r#"SELECT block_num FROM "_/multi_version@latest".blocks ORDER BY block_num LIMIT 1"#,
        )
        .await;

    //* Then
    assert_eq!(results, serde_json::json!([{"block_num": 15000000}]));
}

#[tokio::test]
async fn dataset_query_with_dev_ref_returns_data() {
    //* Given
    let mut ctx = TestCtx::new("dataset_ref_dev").await;
    ctx.restore_eth_firehose_snapshot().await;
    ctx.register_multi_version_dataset_dev_revision().await;
    ctx.materialize_multi_version_dataset("_/multi_version@dev")
        .await;

    //* When
    let (results, _) = ctx
        .query(r#"SELECT block_num FROM "_/multi_version@dev".blocks ORDER BY block_num LIMIT 1"#)
        .await;

    //* Then
    assert_eq!(results, serde_json::json!([{"block_num": 15000000}]));
}

#[tokio::test]
async fn dataset_query_with_hash_ref_returns_data() {
    //* Given
    let mut ctx = TestCtx::new("dataset_ref_hash").await;
    ctx.restore_eth_firehose_snapshot().await;
    ctx.register_multi_version_dataset("0.0.1").await;
    ctx.materialize_multi_version_dataset("_/multi_version@0.0.1")
        .await;
    let hash = ctx.get_manifest_hash("_/multi_version@0.0.1").await;

    //* When
    let (results, _) = ctx
        .query(&format!(
            r#"SELECT block_num FROM "_/multi_version@{hash}".blocks ORDER BY block_num LIMIT 1"#
        ))
        .await;

    //* Then
    assert_eq!(results, serde_json::json!([{"block_num": 15000000}]));
}

#[tokio::test]
async fn dataset_query_with_underscore_namespace_ref_returns_data() {
    //* Given
    let mut ctx = TestCtx::new("dataset_ref_underscore_ns").await;
    ctx.restore_eth_firehose_snapshot().await;
    ctx.register_multi_version_dataset("0.0.1").await;
    ctx.materialize_multi_version_dataset("_/multi_version@0.0.1")
        .await;

    //* When
    let (results, _) = ctx
        .query(r#"SELECT block_num FROM "_/multi_version@0.0.1".blocks ORDER BY block_num LIMIT 1"#)
        .await;

    //* Then
    assert_eq!(results, serde_json::json!([{"block_num": 15000000}]));
}

#[tokio::test]
async fn function_query_with_semver_ref_returns_result() {
    //* Given
    let mut ctx = TestCtx::new("function_ref_semver").await;
    ctx.register_basic_function_dataset("0.0.0").await;

    //* When
    let (results, _) = ctx
        .query(r#"SELECT "_/basic_function@0.0.0".testString()"#)
        .await;

    //* Then
    let row = results.as_array().expect("should be array")[0]
        .as_object()
        .expect("should be object");
    let value = row.values().next().expect("should have a value");
    assert_eq!(value, "I'm a function");
}

#[tokio::test]
async fn function_query_with_bare_name_ref_returns_result() {
    //* Given
    let mut ctx = TestCtx::new("function_ref_latest").await;
    ctx.register_basic_function_dataset("0.0.0").await;

    //* When
    let (results, _) = ctx.query("SELECT basic_function.testString()").await;

    //* Then
    let row = results.as_array().expect("should be array")[0]
        .as_object()
        .expect("should be object");
    let value = row.values().next().expect("should have a value");
    assert_eq!(value, "I'm a function");
}

#[tokio::test]
async fn function_query_with_explicit_latest_ref_returns_result() {
    //* Given
    let mut ctx = TestCtx::new("function_ref_latest_explicit").await;
    ctx.register_basic_function_dataset("0.0.0").await;

    //* When
    let (results, _) = ctx
        .query(r#"SELECT "_/basic_function@latest".testString()"#)
        .await;

    //* Then
    let row = results.as_array().expect("should be array")[0]
        .as_object()
        .expect("should be object");
    let value = row.values().next().expect("should have a value");
    assert_eq!(value, "I'm a function");
}

#[tokio::test]
async fn function_query_with_dev_ref_returns_result() {
    //* Given
    let mut ctx = TestCtx::new("function_ref_dev").await;
    ctx.register_basic_function_dataset_dev_revision().await;

    //* When
    let (results, _) = ctx
        .query(r#"SELECT "_/basic_function@dev".testString()"#)
        .await;

    //* Then
    let row = results.as_array().expect("should be array")[0]
        .as_object()
        .expect("should be object");
    let value = row.values().next().expect("should have a value");
    assert_eq!(value, "I'm a function");
}

#[tokio::test]
async fn function_query_with_hash_ref_returns_result() {
    //* Given
    let mut ctx = TestCtx::new("function_ref_hash").await;
    ctx.register_basic_function_dataset("0.0.0").await;
    let hash = ctx.get_manifest_hash("_/basic_function@0.0.0").await;

    //* When
    let (results, _) = ctx
        .query(&format!(r#"SELECT "_/basic_function@{hash}".testString()"#))
        .await;

    //* Then
    let row = results.as_array().expect("should be array")[0]
        .as_object()
        .expect("should be object");
    let value = row.values().next().expect("should have a value");
    assert_eq!(value, "I'm a function");
}

#[tokio::test]
async fn function_query_with_underscore_namespace_ref_returns_result() {
    //* Given
    let mut ctx = TestCtx::new("function_ref_underscore_ns").await;
    ctx.register_basic_function_dataset("0.0.0").await;

    //* When
    let (results, _) = ctx
        .query(r#"SELECT "_/basic_function@0.0.0".testString()"#)
        .await;

    //* Then
    let row = results.as_array().expect("should be array")[0]
        .as_object()
        .expect("should be object");
    let value = row.values().next().expect("should have a value");
    assert_eq!(value, "I'm a function");
}

/// Test context for SQL reference tests.
///
/// Wraps the test environment and flight client, providing infallible
/// helper methods for common setup steps (snapshot restoration, package
/// registration, dataset dumping, and querying).
struct TestCtx {
    ctx: testlib::ctx::TestCtx,
    amp_cli: testlib::fixtures::AmpCli,
    flight_client: testlib::fixtures::FlightClient,
}

impl TestCtx {
    /// Create a new test context with `eth_firehose` manifest, snapshot, and provider.
    async fn new(test_name: &str) -> Self {
        logging::init();

        let ctx = TestCtxBuilder::new(test_name)
            .with_dataset_manifest("eth_firehose")
            .with_dataset_snapshot("eth_firehose")
            .with_provider_config("firehose_eth_mainnet")
            .build()
            .await
            .expect("failed to create test environment");

        let amp_cli = ctx.new_amp_cli();

        let flight_client = ctx
            .new_flight_client()
            .await
            .expect("failed to create flight client");

        Self {
            ctx,
            amp_cli,
            flight_client,
        }
    }

    /// Restore the `eth_firehose` snapshot so dependency data is available for queries.
    async fn restore_eth_firehose_snapshot(&self) {
        let ampctl = self.ctx.new_ampctl();
        let eth_firehose_ref: Reference = "_/eth_firehose@0.0.0"
            .parse()
            .expect("should parse eth_firehose reference");
        test_helpers::restore_dataset_snapshot(
            &ampctl,
            self.ctx.daemon_controller().dataset_store(),
            self.ctx.daemon_server().data_store(),
            &eth_firehose_ref,
        )
        .await
        .expect("failed to restore eth_firehose snapshot");
    }

    /// Register the `multi_version` package with a semver tag.
    async fn register_multi_version_dataset(&self, tag: &str) {
        let package = DatasetPackage::new("multi_version", Some("v1.config.ts"));
        package
            .register(&self.amp_cli, tag)
            .await
            .expect("failed to register multi_version package");
    }

    /// Register the `multi_version` package as a dev revision (no semver tag).
    async fn register_multi_version_dataset_dev_revision(&self) {
        let package = DatasetPackage::new("multi_version", Some("v1.config.ts"));
        package
            .register(&self.amp_cli, None)
            .await
            .expect("failed to register multi_version package");
    }

    /// Materialize the `multi_version` derived dataset at block 15000000.
    async fn materialize_multi_version_dataset(&self, reference: &str) {
        let dump_ref: Reference = reference
            .parse()
            .expect("should parse multi_version reference");
        let ampctl = self.ctx.new_ampctl();
        test_helpers::deploy_and_wait(&ampctl, &dump_ref, Some(15000000), Duration::from_secs(60))
            .await
            .expect("failed to dump multi_version dataset");
    }

    /// Register the `basic_function` package with a semver tag.
    async fn register_basic_function_dataset(&self, tag: &str) {
        let package = DatasetPackage::new("basic_function", None);
        package
            .register(&self.amp_cli, tag)
            .await
            .expect("failed to register basic_function package");
    }

    /// Register the `basic_function` package as a dev revision (no semver tag).
    async fn register_basic_function_dataset_dev_revision(&self) {
        let package = DatasetPackage::new("basic_function", None);
        package
            .register(&self.amp_cli, None)
            .await
            .expect("failed to register basic_function package");
    }

    /// Execute a SQL query via the Flight SQL client.
    async fn query(&mut self, sql: &str) -> (serde_json::Value, usize) {
        self.flight_client
            .run_query(sql, None)
            .await
            .expect("query should succeed")
    }

    /// Get the latest manifest hash for the given dataset reference.
    async fn get_manifest_hash(&self, dataset: &str) -> String {
        let dataset_ref: Reference = dataset
            .parse()
            .expect("should parse dataset reference for hash lookup");
        self.ctx
            .new_ampctl()
            .get_latest_manifest_hash(&dataset_ref)
            .await
            .expect("should get manifest hash")
            .to_string()
    }
}
