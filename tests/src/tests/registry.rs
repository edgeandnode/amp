use common::manifest::Manifest;
use registry_service::handlers::register::{RegisterRequest, RegisterResponse};
use reqwest::StatusCode;

use crate::test_support::TestEnv;

struct RegistryTestContext {
    env: TestEnv,
    client: reqwest::Client,
}

impl RegistryTestContext {
    async fn setup(test_name: &str) -> Self {
        let env = TestEnv::temp(test_name).await.unwrap();
        let client = reqwest::Client::new();
        Self { env, client }
    }

    fn registry_url(&self) -> String {
        format!("http://{}", self.env.server_addrs.registry_service_addr)
    }

    async fn test_register(&self, manifest: String) -> reqwest::Response {
        let payload = RegisterRequest { manifest };

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
        "erc20_transfers": {{
            "input": {{
                "sql": ""
            }},
            "schema": {{
                "arrow": {{
                    "fields": []
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
    let ctx = RegistryTestContext::setup("test_register_success").await;

    let manifest = RegistryTestContext::create_test_manifest("test_dataset", "1.0.0", "test_owner");
    let manifest_json = serde_json::to_string(&manifest).unwrap();
    let response = ctx.test_register(manifest_json).await;

    assert_eq!(response.status(), StatusCode::OK);
    let response_body: RegisterResponse = response.json().await.unwrap();
    assert!(response_body.success);

    assert!(ctx.verify_dataset_registered("test_dataset", "1.0.0").await);
    let registry_info = ctx
        .env
        .metadata_db
        .get_registry_info("test_dataset", "1.0.0")
        .await
        .expect("Registry info should exist");

    assert_eq!(registry_info.dataset, "test_dataset");
    assert_eq!(registry_info.version, "1.0.0");
    assert_eq!(registry_info.owner, "test_owner");
    assert_eq!(
        registry_info.manifest.to_string(),
        "test_dataset__1.0.0.json"
    );
}

#[tokio::test]
async fn test_register_duplicate_dataset() {
    let ctx = RegistryTestContext::setup("test_register_duplicate").await;

    let manifest =
        RegistryTestContext::create_test_manifest("duplicate_dataset", "1.0.0", "test_owner");
    let manifest_json = serde_json::to_string(&manifest).unwrap();

    let response1 = ctx.test_register(manifest_json.clone()).await;
    assert_eq!(response1.status(), StatusCode::OK);
    let response1_body: RegisterResponse = response1.json().await.unwrap();
    assert!(response1_body.success);

    let response2 = ctx.test_register(manifest_json).await;
    assert_eq!(response2.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let error_response: serde_json::Value = response2.json().await.unwrap();
    let error_message = error_response["error"].as_str().unwrap();
    assert!(error_message.contains("Dataset already exists"));
    assert!(error_message.contains("duplicate_dataset"));
    assert!(error_message.contains("1.0.0"));
}

#[tokio::test]
async fn test_register_invalid_json() {
    let ctx = RegistryTestContext::setup("test_register_invalid_json").await;

    let invalid_json = "this is not valid json";
    let payload = RegisterRequest {
        manifest: invalid_json.to_string(),
    };

    let response = ctx
        .client
        .post(&format!("{}/register", ctx.registry_url()))
        .json(&payload)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let error_response: serde_json::Value = response.json().await.unwrap();
    let error_message = error_response["error"].as_str().unwrap();
    assert!(error_message.contains("Invalid manifest"));
}

#[tokio::test]
async fn test_register_with_missing_fields() {
    let ctx = RegistryTestContext::setup("test_register_with_missing_fields").await;
    let invalid_manifest = r#"{"name": "test_dataset"}"#;
    let payload = RegisterRequest {
        manifest: invalid_manifest.to_string(),
    };
    let response = ctx
        .client
        .post(&format!("{}/register", ctx.registry_url()))
        .json(&payload)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let error_response: serde_json::Value = response.json().await.unwrap();
    let error_message = error_response["error"].as_str().unwrap();
    assert!(error_message.contains("Invalid manifest"));
}

#[tokio::test]
async fn test_register_multiple_versions() {
    let ctx = RegistryTestContext::setup("test_register_multiple_versions").await;

    let test_cases = vec![
        ("1.0.0", "test_owner"),
        ("2.0.0", "test_owner"),
        ("3.0.0", "test_owner_2"),
    ];

    // Register all versions
    for (version, owner) in &test_cases {
        let manifest =
            RegistryTestContext::create_test_manifest("multi_version_dataset", version, owner);
        let manifest_json = serde_json::to_string(&manifest).unwrap();
        let response = ctx.test_register(manifest_json).await;
        assert_eq!(response.status(), StatusCode::OK);
        let response_body: RegisterResponse = response.json().await.unwrap();
        assert!(response_body.success);
    }

    // Verify all versions are registered and check registry info
    for (version, owner) in &test_cases {
        assert!(
            ctx.verify_dataset_registered("multi_version_dataset", version)
                .await
        );
        let registry_info = ctx
            .env
            .metadata_db
            .get_registry_info("multi_version_dataset", version)
            .await
            .expect("Registry info should exist");
        assert_eq!(
            registry_info.manifest.to_string(),
            format!("multi_version_dataset__{}.json", version)
        );
        assert_eq!(registry_info.owner, *owner);
    }
}
