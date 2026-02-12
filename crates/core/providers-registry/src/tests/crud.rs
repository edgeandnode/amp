use std::sync::Arc;

use amp_providers_common::{
    ProviderName,
    config::{ConfigHeaderWithNetwork, ProviderConfigRaw, TryIntoConfig},
};
use object_store::{ObjectStore, memory::InMemory};

use crate::{ProviderConfigsStore, ProvidersRegistry};

#[tokio::test]
async fn get_all_with_empty_store_returns_empty_list() {
    //* Given
    let (store, _configs_store, _raw_store) = create_test_providers_store();

    //* When
    let providers = store.get_all().await;

    //* Then
    assert!(providers.is_empty(), "Expected empty providers list");
}

#[tokio::test]
async fn register_with_valid_provider_stores_and_retrieves() {
    //* Given
    let (store, _configs_store, _raw_store) = create_test_providers_store();
    let provider_name = "test_evm";
    let (name, config) = create_test_evm_provider(provider_name);

    //* When
    let register_result = store.register(name.clone(), config.clone()).await;

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
    let stored_header = stored_provider
        .try_into_config::<ConfigHeaderWithNetwork>()
        .expect("should parse header");
    let config_header = config
        .try_into_config::<ConfigHeaderWithNetwork>()
        .expect("should parse header");
    assert_eq!(stored_header.kind, config_header.kind);
    assert_eq!(stored_header.network, config_header.network);

    // Verify get_by_name returns the provider
    let retrieved = store.get_by_name(provider_name).await;
    assert!(retrieved.is_some(), "Expected provider to be found by name");
    let retrieved_provider = retrieved.expect("should have provider");
    let retrieved_header = retrieved_provider
        .try_into_config::<ConfigHeaderWithNetwork>()
        .expect("should parse header");
    assert_eq!(retrieved_header.kind, config_header.kind);
    assert_eq!(retrieved_header.network, config_header.network);

    // Note: With in-memory storage, we don't verify disk files since they don't exist
}

#[tokio::test]
async fn register_with_multiple_providers_all_retrievable() {
    //* Given
    let (store, _configs_store, _raw_store) = create_test_providers_store();
    let (evm_name, evm_config) = create_test_evm_provider("evm_mainnet");
    let (fhs_name, fhs_config) = create_test_firehose_provider("fhs_polygon");

    //* When
    let evm_result = store.register(evm_name.clone(), evm_config).await;
    let firehose_result = store.register(fhs_name.clone(), fhs_config).await;

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
    let evm_retrieved = store.get_by_name("evm_mainnet").await;
    let firehose_retrieved = store.get_by_name("fhs_polygon").await;

    assert!(evm_retrieved.is_some(), "Expected EVM provider to be found");
    assert!(
        firehose_retrieved.is_some(),
        "Expected Firehose provider to be found"
    );

    let evm = evm_retrieved.expect("should have EVM provider");
    let firehose = firehose_retrieved.expect("should have Firehose provider");

    let evm_header = evm
        .try_into_config::<ConfigHeaderWithNetwork>()
        .expect("should parse EVM header");
    let firehose_header = firehose
        .try_into_config::<ConfigHeaderWithNetwork>()
        .expect("should parse Firehose header");

    assert_eq!(evm_header.kind, "evm-rpc");
    assert_eq!(evm_header.network, "mainnet");
    assert_eq!(firehose_header.kind, "firehose");
    assert_eq!(firehose_header.network, "polygon");

    // Note: With in-memory storage, we don't verify disk files since they don't exist
}

#[tokio::test]
async fn register_with_duplicate_name_overwrites_existing() {
    //* Given
    let (store, _configs_store, _raw_store) = create_test_providers_store();
    let provider_name = "duplicate_name";
    let (evm_name, evm_config) = create_test_evm_provider(provider_name);
    let (fhs_name, fhs_config) = create_test_firehose_provider(provider_name);

    //* When
    let first_result = store.register(evm_name.clone(), evm_config).await;
    let second_result = store.register(fhs_name.clone(), fhs_config).await;

    //* Then
    assert!(
        first_result.is_ok(),
        "Expected first registration to succeed, got: {:?}",
        first_result
    );
    assert!(
        second_result.is_ok(),
        "Expected second registration to succeed (overwrite), got: {:?}",
        second_result
    );

    // Verify only one provider exists with the new data
    let all_providers = store.get_all().await;
    assert_eq!(all_providers.len(), 1, "Expected exactly one provider");

    let provider = store
        .get_by_name(provider_name)
        .await
        .expect("should have provider");
    let header = provider
        .try_into_config::<ConfigHeaderWithNetwork>()
        .expect("should parse header");
    assert_eq!(
        header.kind, "firehose",
        "Expected provider to be overwritten with firehose type"
    );
}

#[tokio::test]
async fn delete_with_existing_provider_removes_from_store_and_cache() {
    //* Given
    let (store, _configs_store, _raw_store) = create_test_providers_store();
    let provider_name = "test_delete";
    let (name, config) = create_test_evm_provider(provider_name);

    store
        .register(name.clone(), config)
        .await
        .expect("should register provider");

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

#[test]
fn provider_name_with_path_separator_is_rejected() {
    //* Given
    let name_with_slash = "networks/mainnet/primary_rpc";

    //* When
    let result = name_with_slash.parse::<ProviderName>();

    //* Then
    assert!(
        result.is_err(),
        "Expected ProviderName to reject path separator"
    );
    assert!(
        matches!(
            result,
            Err(amp_providers_common::InvalidProviderName::InvalidCharacter { character: '/', .. })
        ),
        "Expected InvalidCharacter error for '/', got: {:?}",
        result
    );
}

/// Create a test ProvidersRegistry backed by in-memory storage.
///
/// Returns (ProvidersRegistry, ProviderConfigsStore, raw ObjectStore) for testing caching logic.
fn create_test_providers_store() -> (
    ProvidersRegistry,
    ProviderConfigsStore<Arc<dyn ObjectStore>>,
    Arc<dyn ObjectStore>,
) {
    let in_memory_store = Arc::new(InMemory::new());
    let store: Arc<dyn ObjectStore> = in_memory_store.clone();
    let configs_store = ProviderConfigsStore::new(store.clone());
    let providers_registry = ProvidersRegistry::new(configs_store.clone());
    (providers_registry, configs_store, in_memory_store)
}

/// Create a test EVM RPC provider with mainnet configuration
fn create_test_evm_provider(name: &str) -> (ProviderName, ProviderConfigRaw) {
    let toml_str = indoc::formatdoc! {r#"
        kind = "evm-rpc"
        network = "mainnet"
        url = "http://localhost:8545"
        rate_limit_per_minute = 100
    "#};
    let config = toml::from_str(&toml_str).expect("should parse valid EVM provider TOML");
    let provider_name = name
        .parse::<ProviderName>()
        .expect("should be valid provider name");
    (provider_name, config)
}

/// Create a test Firehose provider with polygon configuration
fn create_test_firehose_provider(name: &str) -> (ProviderName, ProviderConfigRaw) {
    let toml_str = indoc::formatdoc! {r#"
        kind = "firehose"
        network = "polygon"
        url = "https://polygon.firehose.io"
        token = "secret"
    "#};
    let config = toml::from_str(&toml_str).expect("should parse valid Firehose provider TOML");
    let provider_name = name
        .parse::<ProviderName>()
        .expect("should be valid provider name");
    (provider_name, config)
}
