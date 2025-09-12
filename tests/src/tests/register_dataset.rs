use admin_api::handlers::datasets::register::RegisterRequest;
use common::manifest::derived::Manifest;
use registry_service::handlers::register::RegisterRequest as RegistryRegisterRequest;
use reqwest::StatusCode;

use crate::test_support::TestEnv;

struct RegisterTestCtx {
    env: TestEnv,
    client: reqwest::Client,
}

impl RegisterTestCtx {
    async fn setup(test_name: &str) -> Self {
        let env = TestEnv::temp(test_name).await.unwrap();
        let client = reqwest::Client::new();
        Self { env, client }
    }

    fn admin_url(&self) -> String {
        format!("http://{}", self.env.server_addrs.admin_api_addr)
    }

    fn registry_url(&self) -> String {
        format!("http://{}", self.env.server_addrs.registry_service_addr)
    }

    async fn register(&self, request: RegisterRequest) -> reqwest::Response {
        self.client
            .post(&format!("{}/datasets", self.admin_url()))
            .json(&request)
            .send()
            .await
            .unwrap()
    }

    async fn register_dataset(&self, manifest: &Manifest) -> reqwest::Response {
        let manifest_json = serde_json::to_string(manifest).unwrap();
        let payload = RegistryRegisterRequest {
            manifest: manifest_json,
        };

        self.client
            .post(&format!("{}/register", self.registry_url()))
            .json(&payload)
            .send()
            .await
            .unwrap()
    }

    fn create_test_manifest(name: &str, version: &str, owner: &str) -> Manifest {
        let manifest: Manifest = serde_json::from_str(&format!(
            r#"{{
    "name": "{}",
    "network": "mainnet",
    "version": "{}",
    "kind": "manifest",
    "dependencies": {{
        "raw_mainnet": {{
            "owner": "{}",
            "name": "base_dataset",
            "version": "0.0.1"
        }}
    }},
    "tables": {{
        "test_table": {{
            "input": {{
                "sql": "SELECT block_num, miner, hash, parent_hash FROM eth_firehose.blocks"
            }},
            "schema": {{
                "arrow": {{
                    "fields": [
                        {{
                            "name": "_block_num",
                            "type": "UInt64",
                            "nullable": false
                        }},
                        {{
                            "name": "block_num",
                            "type": "UInt64",
                            "nullable": false
                        }},
                        {{
                            "name": "miner",
                            "type": {{
                                "FixedSizeBinary": 20
                            }},
                            "nullable": false
                        }},
                        {{
                            "name": "hash",
                            "type": {{
                                "FixedSizeBinary": 32
                            }},
                            "nullable": false
                        }},
                        {{
                            "name": "parent_hash",
                            "type": {{
                                "FixedSizeBinary": 32
                            }},
                            "nullable": false
                        }}
                    ]
                }}
            }},
            "network": "mainnet"
        }}
    }},
    "functions": {{}}
}}"#,
            name, version, owner,
        ))
        .unwrap();
        manifest
    }

    async fn verify_dataset_exists(&self, name: &str, version: &str) -> bool {
        self.env
            .metadata_db
            .dataset_exists(name, version)
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn register_existing_dataset_without_manifest_succeeds() {
    let ctx = RegisterTestCtx::setup("test_register_existing_dataset").await;

    // Register dataset
    let manifest = RegisterTestCtx::create_test_manifest("register_test", "1.0.0", "test_owner");
    let register_response = ctx.register_dataset(&manifest).await;
    assert_eq!(register_response.status(), StatusCode::OK);

    // Register the existing dataset without manifest in payload
    let register_request = RegisterRequest {
        name: "register_test".parse().expect("valid dataset name"),
        version: "1.0.0".parse().expect("valid version"),
        manifest: None,
    };

    let response = ctx.register(register_request).await;
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn register_new_dataset_with_manifest_succeeds() {
    let ctx = RegisterTestCtx::setup("test_register_with_new_manifest").await;

    let manifest =
        RegisterTestCtx::create_test_manifest("register_test_new", "1.0.0", "test_owner");
    let manifest_json = serde_json::to_string(&manifest).unwrap();

    let register_request = RegisterRequest {
        name: "register_test_new".parse().expect("valid dataset name"),
        version: "1.0.0".parse().expect("valid version"),
        manifest: Some(manifest_json),
    };

    let response = ctx.register(register_request).await;
    assert_eq!(response.status(), StatusCode::OK);

    assert!(
        ctx.verify_dataset_exists("register_test_new", "1.0.0")
            .await
    );
}

#[tokio::test]
async fn register_with_invalid_dataset_name_fails() {
    let ctx = RegisterTestCtx::setup("test_register_invalid_dataset_name").await;

    // This test expects deserialization to fail, so we need to construct the JSON manually
    // since .parse() would panic before we can test the HTTP error
    let json_payload = serde_json::json!({
        "name": "invalid dataset name", // Contains spaces
        "version": "1.0.0",
        "manifest": null
    });

    let response = ctx
        .client
        .post(&format!("{}/datasets", ctx.admin_url()))
        .json(&json_payload)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let error_response: serde_json::Value = response.json().await.unwrap();
    let error_code = error_response["error_code"].as_str().unwrap();
    let error_message = error_response["error_message"].as_str().unwrap();
    assert_eq!(error_code, "INVALID_PAYLOAD_FORMAT");
    assert!(error_message.contains("invalid request format"));
}

#[tokio::test]
async fn register_with_invalid_version_fails() {
    let ctx = RegisterTestCtx::setup("test_register_invalid_version").await;

    // This test expects deserialization to fail, so we need to construct the JSON manually
    let json_payload = serde_json::json!({
        "name": "test_dataset",
        "version": "not_a_version", // Invalid semver
        "manifest": null
    });

    let response = ctx
        .client
        .post(&format!("{}/datasets", ctx.admin_url()))
        .json(&json_payload)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let error_response: serde_json::Value = response.json().await.unwrap();
    let error_code = error_response["error_code"].as_str().unwrap();
    let error_message = error_response["error_message"].as_str().unwrap();
    assert_eq!(error_code, "INVALID_PAYLOAD_FORMAT");
    assert!(error_message.contains("invalid request format"));
}

#[tokio::test]
async fn register_with_mismatched_manifest_fails() {
    let ctx = RegisterTestCtx::setup("test_register_manifest_validation_error").await;

    let manifest = RegisterTestCtx::create_test_manifest("wrong_name", "2.0.0", "test_owner");
    let manifest_json = serde_json::to_string(&manifest).unwrap();

    // Request with different name and version than in manifest
    let register_request = RegisterRequest {
        name: "different_name".parse().expect("valid dataset name"),
        version: "1.0.0".parse().expect("valid version"),
        manifest: Some(manifest_json),
    };

    let response = ctx.register(register_request).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let error_response: serde_json::Value = response.json().await.unwrap();
    let error_code = error_response["error_code"].as_str().unwrap();
    let error_message = error_response["error_message"].as_str().unwrap();
    assert_eq!(error_code, "MANIFEST_VALIDATION_ERROR");
    assert!(error_message.contains("do not match with manifest"));
}

#[tokio::test]
async fn register_existing_dataset_with_manifest_fails() {
    let ctx = RegisterTestCtx::setup("test_register_dataset_already_exists").await;

    // Register dataset
    let manifest = RegisterTestCtx::create_test_manifest(
        "register_test_existing_dataset",
        "1.0.0",
        "test_owner",
    );
    let register_response = ctx.register_dataset(&manifest).await;
    assert_eq!(register_response.status(), StatusCode::OK);

    // Try to register the same dataset with a manifest (should fail)
    let manifest_json = serde_json::to_string(&manifest).unwrap();
    let register_request = RegisterRequest {
        name: "register_test_existing_dataset"
            .parse()
            .expect("valid dataset name"),
        version: "1.0.0".parse().expect("valid version"),
        manifest: Some(manifest_json),
    };

    let response = ctx.register(register_request).await;
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let error_response: serde_json::Value = response.json().await.unwrap();
    let error_code = error_response["error_code"].as_str().unwrap();
    let error_message = error_response["error_message"].as_str().unwrap();
    assert_eq!(error_code, "DATASET_ALREADY_EXISTS");
    assert!(error_message.contains("already exists"));
}

#[tokio::test]
async fn register_nonexistent_dataset_without_manifest_fails() {
    let ctx = RegisterTestCtx::setup("test_register_missing_manifest").await;

    // Try to register a non-existent dataset without a manifest
    let register_request = RegisterRequest {
        name: "non_existent_dataset".parse().expect("valid dataset name"),
        version: "1.0.0".parse().expect("valid version"),
        manifest: None,
    };

    let response = ctx.register(register_request).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let error_response: serde_json::Value = response.json().await.unwrap();
    let error_code = error_response["error_code"].as_str().unwrap();
    let error_message = error_response["error_message"].as_str().unwrap();
    assert_eq!(error_code, "MANIFEST_REQUIRED");
    assert!(error_message.contains("manifest is not provided with request"));
}

#[tokio::test]
async fn register_with_invalid_manifest_json_fails() {
    let ctx = RegisterTestCtx::setup("test_register_invalid_manifest_json").await;

    let register_request = RegisterRequest {
        name: "register_test_dataset".parse().expect("valid dataset name"),
        version: "1.0.0".parse().expect("valid version"),
        manifest: Some("this is not valid json".to_string()),
    };

    let response = ctx.register(register_request).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let error_response: serde_json::Value = response.json().await.unwrap();
    let error_code = error_response["error_code"].as_str().unwrap();
    let error_message = error_response["error_message"].as_str().unwrap();
    assert_eq!(error_code, "INVALID_MANIFEST");
    assert!(error_message.contains("invalid manifest"));
}

#[tokio::test]
async fn register_multiple_versions_of_same_dataset_succeeds() {
    let ctx = RegisterTestCtx::setup("test_register_multiple_versions").await;

    let versions = vec!["1.0.0", "1.1.0", "2.0.0"];

    // Register multiple versions of the same dataset
    for version in &versions {
        let manifest = RegisterTestCtx::create_test_manifest(
            "register_test_multi_version",
            version,
            "test_owner",
        );
        let manifest_json = serde_json::to_string(&manifest).unwrap();

        let register_request = RegisterRequest {
            name: "register_test_multi_version"
                .parse()
                .expect("valid dataset name"),
            version: version.parse().expect("valid version"),
            manifest: Some(manifest_json),
        };

        let response = ctx.register(register_request).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    // Verify all versions exist
    for version in &versions {
        assert!(
            ctx.verify_dataset_exists("register_test_multi_version", version)
                .await
        );
    }

    // Now try to register an existing version again without manifest
    let register_request = RegisterRequest {
        name: "register_test_multi_version"
            .parse()
            .expect("valid dataset name"),
        version: "1.0.0".parse().expect("valid version"),
        manifest: None,
    };

    let response = ctx.register(register_request).await;
    assert_eq!(response.status(), StatusCode::OK);
}
