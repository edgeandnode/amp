use std::sync::Arc;

use object_store::{ObjectStore, memory::InMemory};

use crate::{
    DatasetKind,
    providers::{ProviderConfig, ProviderConfigsStore, RegisterError},
};

#[tokio::test]
async fn get_all_with_empty_store_returns_empty_list() {
    //* Given
    let (store, _raw_store) = create_test_providers_store();

    //* When
    let providers = store.get_all().await;

    //* Then
    assert!(providers.is_empty(), "Expected empty providers list");
}

#[tokio::test]
async fn register_with_valid_provider_stores_and_retrieves() {
    //* Given
    let (store, _raw_store) = create_test_providers_store();
    let provider_name = "test-evm";
    let provider = create_test_evm_provider(provider_name);

    //* When
    let register_result = store.register(provider.clone()).await;

    //* Then
    assert!(
        register_result.is_ok(),
        "Expected successful registration, got: {:?}",
        register_result
    );

    // Verify get_all returns the provider
    let all_providers = store.get_all().await;
    assert_eq!(all_providers.len(), 1, "Expected exactly one provider");
    let stored_provider = all_providers.values().next().unwrap();
    assert_eq!(stored_provider.kind, provider.kind);
    assert_eq!(stored_provider.network, provider.network);

    // Verify get_by_name returns the provider
    let retrieved = store.get_by_name(provider_name).await;
    assert!(retrieved.is_some(), "Expected provider to be found by name");
    let retrieved_provider = retrieved.expect("should have provider");
    assert_eq!(retrieved_provider.kind, provider.kind);
    assert_eq!(retrieved_provider.network, provider.network);

    // Note: With in-memory storage, we don't verify disk files since they don't exist
}

#[tokio::test]
async fn register_with_multiple_providers_all_retrievable() {
    //* Given
    let (store, _raw_store) = create_test_providers_store();
    let evm_provider = create_test_evm_provider("evm-mainnet");
    let fhs_provider = create_test_firehose_provider("fhs-polygon");

    //* When
    let evm_result = store.register(evm_provider.clone()).await;
    let firehose_result = store.register(fhs_provider.clone()).await;

    //* Then
    assert!(
        evm_result.is_ok(),
        "Expected successful EVM registration, got: {:?}",
        evm_result
    );
    assert!(
        firehose_result.is_ok(),
        "Expected successful Firehose registration, got: {:?}",
        firehose_result
    );

    // Verify get_all returns both providers
    let all_providers = store.get_all().await;
    assert_eq!(all_providers.len(), 2, "Expected exactly two providers");

    // Verify both can be retrieved by name
    let evm_retrieved = store.get_by_name("evm-mainnet").await;
    let firehose_retrieved = store.get_by_name("fhs-polygon").await;

    assert!(evm_retrieved.is_some(), "Expected EVM provider to be found");
    assert!(
        firehose_retrieved.is_some(),
        "Expected Firehose provider to be found"
    );

    let evm = evm_retrieved.expect("should have EVM provider");
    let firehose = firehose_retrieved.expect("should have Firehose provider");

    assert_eq!(evm.kind, DatasetKind::EvmRpc);
    assert_eq!(evm.network, "mainnet");
    assert_eq!(firehose.kind, DatasetKind::Firehose);
    assert_eq!(firehose.network, "polygon");

    // Note: With in-memory storage, we don't verify disk files since they don't exist
}

#[tokio::test]
async fn register_with_duplicate_name_returns_conflict_error() {
    //* Given
    let (store, _raw_store) = create_test_providers_store();
    let provider_name = "duplicate-name";
    let evm_provider = create_test_evm_provider(provider_name);
    let fhs_provider = create_test_firehose_provider(provider_name);

    //* When
    let first_result = store.register(evm_provider).await;
    let second_result = store.register(fhs_provider).await;

    //* Then
    assert!(
        first_result.is_ok(),
        "Expected first registration to succeed, got: {:?}",
        first_result
    );
    assert!(
        second_result.is_err(),
        "Expected second registration to fail"
    );

    match second_result {
        Err(RegisterError::Conflict { name }) => {
            assert_eq!(
                name, provider_name,
                "Expected conflict error with correct name"
            );
        }
        other => panic!("Expected RegisterError::Conflict, got: {:?}", other),
    }
}

#[tokio::test]
async fn delete_with_existing_provider_removes_from_store_and_cache() {
    //* Given
    let (store, _raw_store) = create_test_providers_store();
    let provider_name = "test-delete";
    let provider = create_test_evm_provider(provider_name);

    store
        .register(provider)
        .await
        .expect("should register provider");

    // Load providers into cache
    store.load_into_cache().await;

    //* When
    let delete_result = store.delete(provider_name).await;

    //* Then
    assert!(
        delete_result.is_ok(),
        "Expected successful deletion, got: {:?}",
        delete_result
    );

    // Verify provider no longer in cache
    let by_name_result = store.get_by_name(provider_name).await;
    assert!(
        by_name_result.is_none(),
        "Expected provider to be deleted from cache"
    );

    // Verify provider no longer in get_all
    let all_providers = store.get_all().await;
    assert!(
        all_providers.is_empty(),
        "Expected no providers after deletion"
    );

    // Note: With in-memory storage, we don't verify disk files since they don't exist
}

#[tokio::test]
async fn register_with_subdirectory_path_stores_at_correct_location() {
    //* Given
    let (store, raw_store) = create_test_providers_store();
    let provider_name = "networks/mainnet/primary-rpc";
    let provider = create_test_evm_provider(provider_name);

    //* When
    let result = store.register(provider).await;

    //* Then
    assert!(
        result.is_ok(),
        "registration should succeed with subdirectory path"
    );

    // Verify file exists at expected path in underlying store
    let expected_path = format!("{}.toml", provider_name);
    let file_exists = raw_store.head(&expected_path.clone().into()).await.is_ok();
    assert!(file_exists, "file should exist at path: {}", expected_path);
}

#[tokio::test]
async fn delete_with_subdirectory_path_removes_correct_file() {
    //* Given
    let (store, raw_store) = create_test_providers_store();
    let provider_name = "networks/mainnet/primary-rpc";
    let provider = create_test_evm_provider(provider_name);

    // Pre-register the provider
    let register_result = store.register(provider).await;
    assert!(register_result.is_ok(), "setup registration should succeed");

    //* When
    let result = store.delete(provider_name).await;

    //* Then
    assert!(
        result.is_ok(),
        "deletion should succeed with subdirectory path"
    );

    // Verify file was deleted from correct path
    let expected_path = format!("{}.toml", provider_name);
    let file_exists = raw_store.head(&expected_path.clone().into()).await.is_ok();
    assert!(
        !file_exists,
        "file should not exist after deletion at path: {}",
        expected_path
    );
}

/// Create a test ProvidersStore backed by in-memory storage
/// Returns (ProvidersStore, underlying Arc<InMemory>) for testing caching logic
fn create_test_providers_store() -> (ProviderConfigsStore, Arc<dyn ObjectStore>) {
    let in_memory_store = Arc::new(InMemory::new());
    let store: Arc<dyn ObjectStore> = in_memory_store.clone();
    let providers_store = ProviderConfigsStore::new(store);
    (providers_store, in_memory_store)
}

/// Create a test EVM RPC provider with mainnet configuration
fn create_test_evm_provider(name: &str) -> ProviderConfig {
    let toml_str = indoc::formatdoc! {r#"
        name = "{name}"
        kind = "evm-rpc"
        network = "mainnet"
        url = "http://localhost:8545"
        rate_limit_per_minute = 100
    "#};
    toml::from_str(&toml_str).expect("should parse valid EVM provider TOML")
}

/// Create a test Firehose provider with polygon configuration
fn create_test_firehose_provider(name: &str) -> ProviderConfig {
    let toml_str = indoc::formatdoc! {r#"
        name = "{name}"
        kind = "firehose"
        network = "polygon"
        url = "https://polygon.firehose.io"
        token = "secret"
    "#};
    toml::from_str(&toml_str).expect("should parse valid Firehose provider TOML")
}
