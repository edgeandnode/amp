use admin_api::handlers::datasets::register::RegisterRequest;
use common::manifest::{common::Version, derived::Manifest};
use reqwest::StatusCode;

use crate::test_support::TestEnv;

struct AdminApiTestContext {
    env: TestEnv,
    client: reqwest::Client,
}

impl AdminApiTestContext {
    async fn setup(test_name: &str) -> Self {
        let env = TestEnv::temp(test_name).await.unwrap();
        let client = reqwest::Client::new();
        Self { env, client }
    }

    fn admin_url(&self) -> String {
        format!("http://{}", self.env.server_addrs.admin_api_addr)
    }

    async fn register_dataset(&self, manifest: &Manifest) -> reqwest::Response {
        let manifest_json = serde_json::to_string(manifest).unwrap();
        let request = RegisterRequest {
            name: manifest.name.clone(),
            version: manifest.version.clone(),
            manifest: manifest_json.parse().expect("Valid JSON"),
        };

        self.client
            .post(&format!("{}/datasets", self.admin_url()))
            .json(&request)
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
        "erc20_transfers": {{
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
        .expect("Failed to create test manifest");
        manifest
    }

    /// Verify dataset exists in database
    async fn verify_dataset_registered(&self, dataset_name: &str, version: &str) -> bool {
        self.env
            .metadata_db
            .dataset_exists(dataset_name, version)
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn test_register_success() {
    let ctx = AdminApiTestContext::setup("test_register_success").await;

    let manifest =
        AdminApiTestContext::create_test_manifest("register_test_dataset", "1.0.0", "test_owner");
    let response = ctx.register_dataset(&manifest).await;

    assert_eq!(response.status(), StatusCode::CREATED);

    assert!(
        ctx.verify_dataset_registered("register_test_dataset", "1.0.0")
            .await
    );
    let registry_info = ctx
        .env
        .metadata_db
        .get_registry_info("register_test_dataset", "1.0.0")
        .await
        .expect("Registry info should exist");

    assert_eq!(registry_info.dataset, "register_test_dataset");
    assert_eq!(registry_info.version, "1.0.0");
    assert_eq!(registry_info.owner, "test_owner");
    assert_eq!(
        registry_info.manifest.to_string(),
        "register_test_dataset__1_0_0.json"
    );
}

#[tokio::test]
async fn test_register_duplicate_dataset() {
    let ctx = AdminApiTestContext::setup("test_register_duplicate").await;

    let manifest = AdminApiTestContext::create_test_manifest(
        "register_test_duplicate_dataset",
        "1.0.0",
        "test_owner",
    );

    let response1 = ctx.register_dataset(&manifest).await;
    assert_eq!(response1.status(), StatusCode::CREATED);

    let response2 = ctx.register_dataset(&manifest).await;
    assert_eq!(response2.status(), StatusCode::CONFLICT);

    let error_response: serde_json::Value = response2.json().await.unwrap();
    let error_code = error_response["error_code"].as_str().unwrap();
    let error_message = error_response["error_message"].as_str().unwrap();
    assert_eq!(error_code, "DATASET_ALREADY_EXISTS");
    assert!(error_message.contains("already exists"));
}

#[tokio::test]
async fn test_register_invalid_json() {
    let ctx = AdminApiTestContext::setup("test_register_invalid_json").await;

    let request = RegisterRequest {
        name: "test_dataset".parse().unwrap(),
        version: "1.0.0".parse().unwrap(),
        manifest: "this is not valid json".parse().expect("non-empty string"),
    };

    let response = ctx
        .client
        .post(&format!("{}/datasets", ctx.admin_url()))
        .json(&request)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let error_response: serde_json::Value = response.json().await.unwrap();
    let error_code = error_response["error_code"].as_str().unwrap();
    let error_message = error_response["error_message"].as_str().unwrap();
    assert_eq!(error_code, "INVALID_MANIFEST");
    assert!(error_message.contains("invalid manifest"));
}

#[tokio::test]
async fn test_register_with_missing_fields() {
    let ctx = AdminApiTestContext::setup("test_register_with_missing_fields").await;

    let request = RegisterRequest {
        name: "test_dataset".parse().unwrap(),
        version: "1.0.0".parse().unwrap(),
        manifest: r#"{"name": "test_dataset"}"#.parse().expect("non-empty string"),
    };

    let response = ctx
        .client
        .post(&format!("{}/datasets", ctx.admin_url()))
        .json(&request)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let error_response: serde_json::Value = response.json().await.unwrap();
    let error_code = error_response["error_code"].as_str().unwrap();
    let error_message = error_response["error_message"].as_str().unwrap();
    assert_eq!(error_code, "INVALID_MANIFEST");
    assert!(error_message.contains("invalid manifest"));
}

#[tokio::test]
async fn test_register_multiple_versions() {
    let ctx = AdminApiTestContext::setup("test_register_multiple_versions").await;

    let test_cases = vec![
        ("1.0.0", "test_owner"),
        ("2.0.0", "test_owner"),
        ("3.0.0", "test_owner_2"),
    ];

    // Register all versions
    for (version, owner) in &test_cases {
        let manifest = AdminApiTestContext::create_test_manifest(
            "register_test_multi_version_dataset",
            version,
            owner,
        );
        let response = ctx.register_dataset(&manifest).await;
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    // Verify all versions are registered and check registry info
    for (version, owner) in test_cases {
        assert!(
            ctx.verify_dataset_registered("register_test_multi_version_dataset", version)
                .await
        );
        let registry_info = ctx
            .env
            .metadata_db
            .get_registry_info("register_test_multi_version_dataset", version)
            .await
            .expect("Registry info should exist");
        assert_eq!(
            registry_info.manifest.to_string(),
            format!(
                "register_test_multi_version_dataset__{}.json",
                Version::version_identifier(version).unwrap()
            )
        );
        assert_eq!(registry_info.owner, *owner);
    }
}
