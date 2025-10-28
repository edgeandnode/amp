use admin_api::handlers::{datasets::register::RegisterRequest, error::ErrorResponse};
use datasets_common::{name::Name, namespace::Namespace, version::Version};
use datasets_derived::Manifest as DerivedDatasetManifest;
use reqwest::StatusCode;

use crate::testlib::ctx::TestCtxBuilder;

#[tokio::test]
async fn register_new_dataset_with_manifest_succeeds() {
    //* Given
    let ctx = TestCtx::setup("test_register_with_new_manifest").await;
    let namespace = "_".parse::<Namespace>().expect("valid namespace");
    let name = "register_test_new"
        .parse::<Name>()
        .expect("valid dataset name");
    let version = "1.0.0".parse::<Version>().expect("valid version");

    let manifest = create_test_manifest();
    let manifest = serde_json::to_string(&manifest)
        .expect("failed to serialize manifest to JSON")
        .parse()
        .expect("Valid JSON");
    let register_request = RegisterRequest {
        namespace: namespace.clone(),
        name: name.clone(),
        version: version.clone(),
        manifest,
    };

    //* When
    let resp = ctx.register(register_request).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "registration should succeed with valid manifest"
    );
    assert!(
        ctx.verify_dataset_exists(&namespace, &name, &version).await,
        "dataset should exist after successful registration"
    );
}

#[tokio::test]
async fn register_with_invalid_dataset_name_fails() {
    //* Given
    let ctx = TestCtx::setup("test_register_invalid_dataset_name").await;
    // This test expects deserialization to fail, so we need to construct the JSON manually
    // since .parse() would panic before we can test the HTTP error
    let json_payload = serde_json::json!({
        "namespace": "_",
        "name": "invalid dataset name", // Contains spaces
        "version": "1.0.0",
        "manifest": null
    });

    //* When
    let resp = ctx.register_raw_json(json_payload).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "registration should fail with invalid dataset name"
    );
    let error_response: serde_json::Value = resp
        .json()
        .await
        .expect("failed to parse error response JSON");
    let error_code = error_response["error_code"]
        .as_str()
        .expect("error_code should be a string");
    let error_message = error_response["error_message"]
        .as_str()
        .expect("error_message should be a string");
    assert_eq!(
        error_code, "INVALID_PAYLOAD_FORMAT",
        "should return payload format error"
    );
    assert!(
        error_message.contains("invalid request format"),
        "error message should indicate invalid format"
    );
}

#[tokio::test]
async fn register_with_invalid_version_fails() {
    //* Given
    let ctx = TestCtx::setup("test_register_invalid_version").await;
    // This test expects deserialization to fail, so we need to construct the JSON manually
    let json_payload = serde_json::json!({
        "namespace": "_",
        "name": "test_dataset",
        "version": "not_a_version", // Invalid semver
        "manifest": null
    });

    //* When
    let resp = ctx.register_raw_json(json_payload).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "registration should fail with invalid version"
    );
    let error_response: serde_json::Value = resp
        .json()
        .await
        .expect("failed to parse error response JSON");
    let error_code = error_response["error_code"]
        .as_str()
        .expect("error_code should be a string");
    let error_message = error_response["error_message"]
        .as_str()
        .expect("error_message should be a string");
    assert_eq!(
        error_code, "INVALID_PAYLOAD_FORMAT",
        "should return payload format error"
    );
    assert!(
        error_message.contains("invalid request format"),
        "error message should indicate invalid format"
    );
}

#[tokio::test]
async fn register_with_missing_dependency_fails() {
    //* Given
    let ctx = TestCtx::setup("test_register_invalid_dependency").await;
    let namespace = "_".parse::<Namespace>().expect("valid namespace");
    let name = "missing_dep".parse::<Name>().expect("valid dataset name");
    let version = "1.0.0".parse::<Version>().expect("valid version");

    let mut manifest = create_test_manifest();
    manifest.dependencies.clear();
    let manifest = serde_json::to_string(&manifest)
        .expect("failed to serialize manifest to JSON")
        .parse()
        .expect("Valid JSON");
    let register_request = RegisterRequest {
        namespace: namespace.clone(),
        name: name.clone(),
        version: version.clone(),
        manifest,
    };

    //* When
    let resp = ctx.register(register_request).await;

    //* Then
    let status = resp.status();
    let body: ErrorResponse = resp.json().await.expect("error response body");
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body.error_code, "DEPENDENCY_VALIDATION_ERROR");
    assert_eq!(
        body.error_message,
        r#"Manifest dependency error: undeclared dependencies of SQL query: ["eth_firehose"]"#,
    );
}

#[tokio::test]
async fn register_existing_dataset_is_idempotent() {
    //* Given
    let ctx = TestCtx::setup("test_register_dataset_idempotent").await;
    let namespace = "_".parse::<Namespace>().expect("valid namespace");
    let name = "register_test_existing_dataset"
        .parse::<Name>()
        .expect("valid dataset name");
    let version = "1.0.0".parse::<Version>().expect("valid version");
    let manifest = create_test_manifest();

    // Register dataset first to create existing state
    let register_resp = ctx
        .register_dataset(namespace.clone(), name.clone(), version.clone(), &manifest)
        .await;
    assert_eq!(
        register_resp.status(),
        StatusCode::CREATED,
        "initial registration should succeed"
    );

    // Prepare duplicate registration request with same hash
    let manifest = serde_json::to_string(&manifest)
        .expect("failed to serialize manifest to JSON")
        .parse()
        .expect("valid JSON");
    let register_request = RegisterRequest {
        namespace: namespace.clone(),
        name: name.clone(),
        version: version.clone(),
        manifest,
    };

    //* When - Register same dataset again
    let resp = ctx.register(register_request).await;

    //* Then - Should succeed (idempotent)
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "registration should be idempotent when same dataset is registered again"
    );

    // Verify dataset still exists and is accessible
    assert!(
        ctx.verify_dataset_exists(&namespace, &name, &version).await,
        "dataset should still exist after duplicate registration"
    );
}

#[tokio::test]
async fn register_with_invalid_manifest_json_fails() {
    //* Given
    let ctx = TestCtx::setup("test_register_invalid_manifest_json").await;
    let register_request = RegisterRequest {
        namespace: "_".parse().expect("valid namespace"),
        name: "register_test_dataset".parse().expect("valid dataset name"),
        version: "1.0.0".parse().expect("valid version"),
        manifest: "this is not valid json".parse().expect("non-empty string"),
    };

    //* When
    let resp = ctx.register(register_request).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "registration should fail with invalid manifest JSON"
    );
    let error_response: serde_json::Value = resp
        .json()
        .await
        .expect("failed to parse error response JSON");
    let error_code = error_response["error_code"]
        .as_str()
        .expect("error_code should be a string");
    let error_message = error_response["error_message"]
        .as_str()
        .expect("error_message should be a string");
    assert_eq!(
        error_code, "INVALID_MANIFEST",
        "should return invalid manifest error"
    );
    assert!(
        error_message.contains("invalid manifest"),
        "error message should indicate invalid manifest"
    );
}

#[tokio::test]
async fn register_multiple_versions_of_same_dataset_succeeds() {
    //* Given
    let ctx = TestCtx::setup("test_register_multiple_versions").await;

    let namespace = "_".parse::<Namespace>().expect("valid namespace");
    let name = "register_test_multi_version"
        .parse::<Name>()
        .expect("valid dataset name");
    let versions = vec!["1.0.0", "1.1.0", "2.0.0"]
        .into_iter()
        .map(|s| s.parse().expect("valid version"))
        .collect::<Vec<Version>>();

    //* When
    // Register multiple versions of the same dataset
    for version in &versions {
        let manifest = create_test_manifest();
        let manifest_json =
            serde_json::to_string(&manifest).expect("failed to serialize manifest to JSON");

        let register_request = RegisterRequest {
            namespace: namespace.clone(),
            name: name.clone(),
            version: version.clone(),
            manifest: manifest_json.parse().expect("Valid JSON"),
        };

        let resp = ctx.register(register_request).await;
        assert_eq!(
            resp.status(),
            StatusCode::CREATED,
            "each version registration should succeed"
        );
    }

    //* Then
    // Verify all versions exist
    for version in &versions {
        assert!(
            ctx.verify_dataset_exists(&namespace, &name, version).await,
            "version {} should exist after registration",
            version
        );
    }
}

struct TestCtx {
    ctx: crate::testlib::ctx::TestCtx,
    client: reqwest::Client,
    admin_api_url: String,
}

impl TestCtx {
    async fn setup(test_name: &str) -> Self {
        let ctx = TestCtxBuilder::new(test_name)
            .build()
            .await
            .expect("failed to build test context");

        let client = reqwest::Client::new();
        let admin_api_url = ctx.daemon_controller().admin_api_url();

        Self {
            ctx,
            client,
            admin_api_url,
        }
    }

    async fn register(&self, request: RegisterRequest) -> reqwest::Response {
        self.client
            .post(format!("{}/datasets", self.admin_api_url))
            .json(&request)
            .send()
            .await
            .expect("failed to send register request")
    }

    async fn register_dataset(
        &self,
        namespace: Namespace,
        name: Name,
        version: Version,
        manifest: &DerivedDatasetManifest,
    ) -> reqwest::Response {
        let manifest = serde_json::to_string(manifest)
            .expect("failed to serialize manifest to JSON")
            .parse()
            .expect("Valid JSON");

        let request = RegisterRequest {
            namespace,
            name,
            version,
            manifest,
        };

        self.register(request).await
    }

    async fn register_raw_json(&self, json_payload: serde_json::Value) -> reqwest::Response {
        self.client
            .post(format!("{}/datasets", &self.admin_api_url))
            .json(&json_payload)
            .send()
            .await
            .expect("failed to send HTTP request")
    }

    async fn verify_dataset_exists(
        &self,
        namespace: &Namespace,
        name: &Name,
        version: &Version,
    ) -> bool {
        metadata_db::datasets::get_version_tag(self.ctx.metadata_db(), namespace, name, version)
            .await
            .expect("failed to check if dataset exists")
            .is_some()
    }
}

fn create_test_manifest() -> DerivedDatasetManifest {
    let manifest_json = indoc::indoc! {r#"
        {
            "kind": "manifest",
            "dependencies": {
                "raw_mainnet": "_/eth_firehose@0.0.1"
            },
            "tables": {
                "test_table": {
                    "input": {
                        "sql": "SELECT block_num, miner, hash, parent_hash FROM eth_firehose.blocks"
                    },
                    "schema": {
                        "arrow": {
                            "fields": [
                                {
                                    "name": "_block_num",
                                    "type": "UInt64",
                                    "nullable": false
                                },
                                {
                                    "name": "block_num",
                                    "type": "UInt64",
                                    "nullable": false
                                },
                                {
                                    "name": "miner",
                                    "type": {
                                        "FixedSizeBinary": 20
                                    },
                                    "nullable": false
                                },
                                {
                                    "name": "hash",
                                    "type": {
                                        "FixedSizeBinary": 32
                                    },
                                    "nullable": false
                                },
                                {
                                    "name": "parent_hash",
                                    "type": {
                                        "FixedSizeBinary": 32
                                    },
                                    "nullable": false
                                }
                            ]
                        }
                    },
                    "network": "mainnet"
                }
            },
            "functions": {}
        }
    "#};

    serde_json::from_str(manifest_json).expect("failed to parse manifest JSON")
}
