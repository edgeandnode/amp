use admin_api::handlers::datasets::deploy::DeployRequest;
use common::manifest::derived::Manifest;
use registry_service::handlers::register::RegisterRequest;
use reqwest::StatusCode;

use crate::test_support::TestEnv;

struct DeployTestContext {
    env: TestEnv,
    client: reqwest::Client,
}

impl DeployTestContext {
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

    async fn deploy(&self, request: DeployRequest) -> reqwest::Response {
        self.client
            .post(&format!("{}/deploy", self.admin_url()))
            .json(&request)
            .send()
            .await
            .unwrap()
    }

    async fn register_dataset(&self, manifest: &Manifest) -> reqwest::Response {
        let manifest_json = serde_json::to_string(manifest).unwrap();
        let payload = RegisterRequest {
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

    async fn verify_dataset_exists(&self, dataset_name: &str, version: &str) -> bool {
        self.env
            .metadata_db
            .dataset_exists(dataset_name, version)
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn test_deploy_registered_dataset() {
    let ctx = DeployTestContext::setup("test_deploy_registered_dataset").await;

    // Register dataset
    let manifest = DeployTestContext::create_test_manifest("deploy_test", "1.0.0", "test_owner");
    let register_response = ctx.register_dataset(&manifest).await;
    assert_eq!(register_response.status(), StatusCode::OK);

    // Deploy the registered dataset without manifest in payload
    let deploy_request = DeployRequest {
        dataset_name: "deploy_test".to_string(),
        version: "1.0.0".to_string(),
        manifest: None,
    };

    let response = ctx.deploy(deploy_request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let response_text = response.text().await.unwrap();
    assert_eq!(response_text, "DEPLOYMENT_SUCCESSFUL");
}

#[tokio::test]
async fn test_deploy_with_new_manifest() {
    let ctx = DeployTestContext::setup("test_deploy_with_new_manifest").await;

    let manifest =
        DeployTestContext::create_test_manifest("deploy_test_new", "1.0.0", "test_owner");
    let manifest_json = serde_json::to_string(&manifest).unwrap();

    let deploy_request = DeployRequest {
        dataset_name: "deploy_test_new".to_string(),
        version: "1.0.0".to_string(),
        manifest: Some(manifest_json),
    };

    let response = ctx.deploy(deploy_request).await;
    assert_eq!(response.status(), StatusCode::OK);

    let response_text = response.text().await.unwrap();
    assert_eq!(response_text, "DEPLOYMENT_SUCCESSFUL");

    assert!(ctx.verify_dataset_exists("deploy_test_new", "1.0.0").await);
}

#[tokio::test]
async fn test_deploy_invalid_dataset_name() {
    let ctx = DeployTestContext::setup("test_deploy_invalid_dataset_name").await;

    let deploy_request = DeployRequest {
        dataset_name: "invalid dataset name".to_string(), // Contains spaces
        version: "1.0.0".to_string(),
        manifest: None,
    };

    let response = ctx.deploy(deploy_request).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let error_response: serde_json::Value = response.json().await.unwrap();
    let error_code = error_response["error_code"].as_str().unwrap();
    let error_message = error_response["error_message"].as_str().unwrap();
    assert_eq!(error_code, "INVALID_REQUEST");
    assert!(error_message.contains("invalid dataset name"));
}

#[tokio::test]
async fn test_deploy_invalid_version() {
    let ctx = DeployTestContext::setup("test_deploy_invalid_version").await;

    let deploy_request = DeployRequest {
        dataset_name: "test_dataset".to_string(),
        version: "not_a_version".to_string(), // Invalid semver
        manifest: None,
    };

    let response = ctx.deploy(deploy_request).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let error_response: serde_json::Value = response.json().await.unwrap();
    let error_code = error_response["error_code"].as_str().unwrap();
    let error_message = error_response["error_message"].as_str().unwrap();
    assert_eq!(error_code, "INVALID_REQUEST");
    assert!(error_message.contains("invalid dataset version"));
}

#[tokio::test]
async fn test_deploy_manifest_validation_error() {
    let ctx = DeployTestContext::setup("test_deploy_manifest_validation_error").await;

    let manifest = DeployTestContext::create_test_manifest("wrong_name", "2.0.0", "test_owner");
    let manifest_json = serde_json::to_string(&manifest).unwrap();

    // Request with different name and version than in manifest
    let deploy_request = DeployRequest {
        dataset_name: "different_name".to_string(),
        version: "1.0.0".to_string(),
        manifest: Some(manifest_json),
    };

    let response = ctx.deploy(deploy_request).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let error_response: serde_json::Value = response.json().await.unwrap();
    let error_code = error_response["error_code"].as_str().unwrap();
    let error_message = error_response["error_message"].as_str().unwrap();
    assert_eq!(error_code, "MANIFEST_VALIDATION_ERROR");
    assert!(error_message.contains("do not match with manifest"));
}

#[tokio::test]
async fn test_deploy_dataset_already_exists() {
    let ctx = DeployTestContext::setup("test_deploy_dataset_already_exists").await;

    // Register dataset
    let manifest = DeployTestContext::create_test_manifest(
        "deploy_test_existing_dataset",
        "1.0.0",
        "test_owner",
    );
    let register_response = ctx.register_dataset(&manifest).await;
    assert_eq!(register_response.status(), StatusCode::OK);

    // Try to deploy the same dataset with a manifest (should fail)
    let manifest_json = serde_json::to_string(&manifest).unwrap();
    let deploy_request = DeployRequest {
        dataset_name: "deploy_test_existing_dataset".to_string(),
        version: "1.0.0".to_string(),
        manifest: Some(manifest_json),
    };

    let response = ctx.deploy(deploy_request).await;
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let error_response: serde_json::Value = response.json().await.unwrap();
    let error_code = error_response["error_code"].as_str().unwrap();
    let error_message = error_response["error_message"].as_str().unwrap();
    assert_eq!(error_code, "DATASET_ALREADY_EXISTS");
    assert!(error_message.contains("already exists"));
}

#[tokio::test]
async fn test_deploy_missing_manifest() {
    let ctx = DeployTestContext::setup("test_deploy_missing_manifest").await;

    // Try to deploy a non-existent dataset without a manifest
    let deploy_request = DeployRequest {
        dataset_name: "non_existent_dataset".to_string(),
        version: "1.0.0".to_string(),
        manifest: None,
    };

    let response = ctx.deploy(deploy_request).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let error_response: serde_json::Value = response.json().await.unwrap();
    let error_code = error_response["error_code"].as_str().unwrap();
    let error_message = error_response["error_message"].as_str().unwrap();
    assert_eq!(error_code, "MANIFEST_REQUIRED");
    assert!(error_message.contains("manifest is not provided with request"));
}

#[tokio::test]
async fn test_deploy_invalid_manifest_json() {
    let ctx = DeployTestContext::setup("test_deploy_invalid_manifest_json").await;

    let deploy_request = DeployRequest {
        dataset_name: "deploy_test_dataset".to_string(),
        version: "1.0.0".to_string(),
        manifest: Some("this is not valid json".to_string()),
    };

    let response = ctx.deploy(deploy_request).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let error_response: serde_json::Value = response.json().await.unwrap();
    let error_code = error_response["error_code"].as_str().unwrap();
    let error_message = error_response["error_message"].as_str().unwrap();
    assert_eq!(error_code, "INVALID_MANIFEST");
    assert!(error_message.contains("invalid manifest"));
}

#[tokio::test]
async fn test_deploy_multiple_versions() {
    let ctx = DeployTestContext::setup("test_deploy_multiple_versions").await;

    let versions = vec!["1.0.0", "1.1.0", "2.0.0"];

    // Deploy multiple versions of the same dataset
    for version in &versions {
        let manifest = DeployTestContext::create_test_manifest(
            "deploy_test_multi_version_deploy",
            version,
            "test_owner",
        );
        let manifest_json = serde_json::to_string(&manifest).unwrap();

        let deploy_request = DeployRequest {
            dataset_name: "deploy_test_multi_version_deploy".to_string(),
            version: version.to_string(),
            manifest: Some(manifest_json),
        };

        let response = ctx.deploy(deploy_request).await;
        assert_eq!(response.status(), StatusCode::OK);

        let response_text = response.text().await.unwrap();
        assert_eq!(response_text, "DEPLOYMENT_SUCCESSFUL");
    }

    // Verify all versions exist
    for version in &versions {
        assert!(
            ctx.verify_dataset_exists("deploy_test_multi_version_deploy", version)
                .await
        );
    }

    // Now try to deploy an existing version again without manifest
    let deploy_request = DeployRequest {
        dataset_name: "deploy_test_multi_version_deploy".to_string(),
        version: "1.0.0".to_string(),
        manifest: None,
    };

    let response = ctx.deploy(deploy_request).await;
    assert_eq!(response.status(), StatusCode::OK);
}
