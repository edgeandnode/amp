use std::sync::Arc;

use object_store::{ObjectStore, memory::InMemory, path::Path};

use crate::{ProviderConfig, ProviderConfigsStore, ProvidersRegistry, RegisterError};

#[tokio::test]
async fn register_with_valid_provider_makes_immediately_available() {
    //* Given
    let (store, _raw_store) = create_test_providers_store();
    let evm_provider = create_test_evm_provider("evm-mainnet");
    let fhs_provider = create_test_firehose_provider("fhs-polygon");

    // Register first provider
    store
        .register(evm_provider.clone())
        .await
        .expect("should register first provider");

    store.load_into_cache().await;

    //* When
    let register_result = store.register(fhs_provider.clone()).await;

    //* Then
    assert!(
        register_result.is_ok(),
        "Expected successful registration, got: {:?}",
        register_result
    );

    // Verify new provider is immediately available
    let all_providers = store.get_all().await;
    assert_eq!(
        all_providers.len(),
        2,
        "Expected two providers after second registration"
    );

    let fhs_retrieved = store.get_by_name("fhs-polygon").await;
    assert!(
        fhs_retrieved.is_some(),
        "Expected fhs-polygon to be immediately available"
    );

    // Verify files were created in the in-memory store
    use object_store::path::Path;
    let evm_path = Path::from("evm-mainnet.toml");
    let fhs_path = Path::from("fhs-polygon.toml");

    let evm_result = _raw_store.get(&evm_path).await;
    assert!(
        evm_result.is_ok(),
        "Expected evm-mainnet.toml file to exist in store"
    );

    let fhs_result = _raw_store.get(&fhs_path).await;
    assert!(
        fhs_result.is_ok(),
        "Expected fhs-polygon.toml file to exist in store"
    );

    // Verify file contents are correct
    let evm_bytes = evm_result
        .unwrap()
        .bytes()
        .await
        .expect("should read evm bytes");
    let evm_file_contents =
        String::from_utf8(evm_bytes.to_vec()).expect("should convert to string");
    let parsed_evm_provider: ProviderConfig =
        toml::from_str(&evm_file_contents).expect("should parse evm-mainnet file");
    assert_eq!(parsed_evm_provider.kind, evm_provider.kind);
    assert_eq!(parsed_evm_provider.network, evm_provider.network);

    let fhs_bytes = fhs_result
        .unwrap()
        .bytes()
        .await
        .expect("should read fhs bytes");
    let fhs_file_contents =
        String::from_utf8(fhs_bytes.to_vec()).expect("should convert to string");
    let parsed_fhs_provider: ProviderConfig =
        toml::from_str(&fhs_file_contents).expect("should parse fhs-polygon file");
    assert_eq!(parsed_fhs_provider.kind, fhs_provider.kind);
    assert_eq!(parsed_fhs_provider.network, fhs_provider.network);
}

#[tokio::test]
async fn get_by_name_after_registration_returns_provider() {
    //* Given
    let (store, _raw_store) = create_test_providers_store();
    let provider_name = "consistent-provider";
    let provider = create_test_evm_provider(provider_name);

    store
        .register(provider)
        .await
        .expect("should register provider");

    // Load providers into cache
    store.load_into_cache().await;

    //* When
    let provider_option = store.get_by_name(provider_name).await;

    //* Then
    assert!(
        provider_option.is_some(),
        "Expected to find provider by name"
    );
    let retrieved_provider = provider_option.expect("should have retrieved provider");
    assert_eq!(
        retrieved_provider.network, "mainnet",
        "Expected correct provider data"
    );
}

#[tokio::test]
async fn delete_with_existing_provider_removes_from_operations() {
    //* Given
    let (store, _raw_store) = create_test_providers_store();
    let evm_provider = create_test_evm_provider("evm-to-delete");
    let fhs_provider = create_test_firehose_provider("fhs-to-keep");

    store
        .register(evm_provider)
        .await
        .expect("should register first provider");
    store
        .register(fhs_provider)
        .await
        .expect("should register second provider");

    // Ensure both providers are available
    store.load_into_cache().await;

    //* When
    let delete_result = store.delete("evm-to-delete").await;

    //* Then
    assert!(
        delete_result.is_ok(),
        "Expected successful deletion, got: {:?}",
        delete_result
    );

    // Verify deleted provider is no longer available
    let remaining_providers = store.get_all().await;
    assert_eq!(
        remaining_providers.len(),
        1,
        "Expected one provider after deletion"
    );

    let deleted_provider = store.get_by_name("evm-to-delete").await;
    assert!(
        deleted_provider.is_none(),
        "Expected deleted provider to be unavailable"
    );

    let kept_provider = store.get_by_name("fhs-to-keep").await;
    assert!(
        kept_provider.is_some(),
        "Expected kept provider to remain available"
    );

    // Verify deleted file was removed from in-memory store
    use object_store::path::Path;
    let deleted_path = Path::from("evm-to-delete.toml");
    let kept_path = Path::from("fhs-to-keep.toml");

    let deleted_result = _raw_store.get(&deleted_path).await;
    assert!(
        deleted_result.is_err(),
        "Expected deleted provider file to be removed from store"
    );

    let kept_result = _raw_store.get(&kept_path).await;
    assert!(
        kept_result.is_ok(),
        "Expected kept provider file to remain in store"
    );
}

#[tokio::test]
async fn get_all_with_store_providers_loads_on_first_access() {
    //* Given
    let (providers_store, raw_store) = create_test_providers_store();

    // Manually write provider TOML files to the in-memory store
    let evm_toml = indoc::indoc! {r#"
        name = "evm-mainnet"
        kind = "evm-rpc"
        network = "mainnet"
        url = "http://localhost:8545"
        rate_limit_per_minute = 100
    "#};
    let firehose_toml = indoc::indoc! {r#"
        name = "firehose-polygon"
        kind = "firehose"
        network = "polygon"
        url = "https://polygon.firehose.io"
        token = "secret"
    "#};

    raw_store
        .put(&Path::from("evm-mainnet.toml"), evm_toml.as_bytes().into())
        .await
        .expect("should write EVM provider file to store");
    raw_store
        .put(
            &Path::from("firehose-polygon.toml"),
            firehose_toml.as_bytes().into(),
        )
        .await
        .expect("should write Firehose provider file to store");

    //* When
    let providers = providers_store.get_all().await;

    //* Then
    assert_eq!(
        providers.len(),
        2,
        "Expected two providers loaded from store"
    );

    // Verify EVM provider was loaded correctly
    let evm_provider = providers_store.get_by_name("evm-mainnet").await;
    assert!(evm_provider.is_some(), "Expected EVM provider to be loaded");
    let evm = evm_provider.expect("should have EVM provider");
    assert_eq!(evm.kind, "evm-rpc");
    assert_eq!(evm.network, "mainnet");

    // Verify Firehose provider was loaded correctly
    let firehose_provider = providers_store.get_by_name("firehose-polygon").await;
    assert!(
        firehose_provider.is_some(),
        "Expected Firehose provider to be loaded"
    );
    let firehose = firehose_provider.expect("should have Firehose provider");
    assert_eq!(firehose.kind, "firehose");
    assert_eq!(firehose.network, "polygon");
}

#[tokio::test]
async fn get_all_with_invalid_toml_handles_gracefully() {
    //* Given
    let (providers_store, raw_store) = create_test_providers_store();

    // Write one valid and one invalid TOML file to the in-memory store
    let valid_toml = indoc::indoc! {r#"
        name = "valid-provider"
        kind = "evm-rpc"
        network = "mainnet"
        url = "http://localhost:8545"
        rate_limit_per_minute = 100
    "#};

    let invalid_toml = indoc::indoc! {r#"
        name = "invalid-provider"
        kind = "evm-rpc"
        network = "mainnet"
        url = "http://localhost:8545
        # Missing closing quote on url field
        rate_limit_per_minute = 100
    "#};

    raw_store
        .put(
            &Path::from("valid-provider.toml"),
            valid_toml.as_bytes().into(),
        )
        .await
        .expect("should write valid TOML file to store");
    raw_store
        .put(
            &Path::from("invalid-provider.toml"),
            invalid_toml.as_bytes().into(),
        )
        .await
        .expect("should write invalid TOML file to store");

    //* When
    let providers = providers_store.get_all().await;

    //* Then
    // Should succeed and return only the valid provider (graceful degradation)
    assert_eq!(
        providers.len(),
        1,
        "Expected one valid provider loaded, invalid one skipped"
    );

    // Verify the valid provider was loaded correctly
    let provider = providers
        .values()
        .next()
        .expect("should have at least one provider");
    assert_eq!(provider.kind, "evm-rpc");
    assert_eq!(provider.network, "mainnet");
}

#[tokio::test]
async fn get_all_with_externally_removed_file_returns_cached_data() {
    //* Given
    let (providers_store, raw_store) = create_test_providers_store();

    // Write provider files to the in-memory store
    let evm_toml = indoc::indoc! {r#"
        name = "evm-provider"
        kind = "evm-rpc"
        network = "mainnet"
        url = "http://localhost:8545"
        rate_limit_per_minute = 100
    "#};
    let firehose_toml = indoc::indoc! {r#"
        name = "firehose-provider"
        kind = "firehose"
        network = "polygon"
        url = "https://polygon.firehose.io"
        token = "secret"
    "#};

    raw_store
        .put(&Path::from("evm-provider.toml"), evm_toml.as_bytes().into())
        .await
        .expect("should write EVM provider file to store");
    raw_store
        .put(
            &Path::from("firehose-provider.toml"),
            firehose_toml.as_bytes().into(),
        )
        .await
        .expect("should write Firehose provider file to store");

    // Load providers into cache
    providers_store.load_into_cache().await;

    // Remove one file directly from store (bypassing the ProvidersStore)
    raw_store
        .delete(&Path::from("evm-provider.toml"))
        .await
        .expect("should remove EVM provider file from store");

    //* When
    let cached_providers = providers_store.get_all().await;

    //* Then
    assert_eq!(
        cached_providers.len(),
        2,
        "Expected cache to still contain 2 providers despite file removal"
    );
}

#[tokio::test]
async fn get_by_name_with_externally_removed_file_returns_cached_provider() {
    //* Given
    let (providers_store, raw_store) = create_test_providers_store();

    let evm_toml = indoc::indoc! {r#"
        name = "evm-provider"
        kind = "evm-rpc"
        network = "mainnet"
        url = "http://localhost:8545"
        rate_limit_per_minute = 100
    "#};

    raw_store
        .put(&Path::from("evm-provider.toml"), evm_toml.as_bytes().into())
        .await
        .expect("should write EVM provider file to store");

    // Load providers into cache
    providers_store.load_into_cache().await;

    // Remove file directly from store (bypassing the ProvidersStore)
    raw_store
        .delete(&Path::from("evm-provider.toml"))
        .await
        .expect("should remove EVM provider file from store");

    //* When
    let provider_option = providers_store.get_by_name("evm-provider").await;

    //* Then
    assert!(
        provider_option.is_some(),
        "Expected removed provider to still be found in cache"
    );
}

#[tokio::test]
async fn delete_with_externally_removed_file_fails() {
    //* Given
    let (providers_store, raw_store) = create_test_providers_store();

    let evm_toml = indoc::indoc! {r#"
        name = "evm-provider"
        kind = "evm-rpc"
        network = "mainnet"
        url = "http://localhost:8545"
        rate_limit_per_minute = 100
    "#};

    raw_store
        .put(&Path::from("evm-provider.toml"), evm_toml.as_bytes().into())
        .await
        .expect("should write EVM provider file to store");

    // Load providers into cache
    providers_store.load_into_cache().await;

    // Remove file directly from store (bypassing the ProvidersStore)
    raw_store
        .delete(&Path::from("evm-provider.toml"))
        .await
        .expect("should remove EVM provider file from store");

    //* When
    let result = providers_store.delete("evm-provider").await;

    //* Then
    assert!(
        result.is_err(),
        "Expected delete to fail for externally removed file"
    );
}

#[tokio::test]
async fn register_with_externally_removed_file_returns_conflict_error() {
    //* Given
    let (providers_store, raw_store) = create_test_providers_store();

    let evm_toml = indoc::indoc! {r#"
        name = "existing-provider"
        kind = "evm-rpc"
        network = "mainnet"
        url = "http://localhost:8545"
        rate_limit_per_minute = 100
    "#};

    raw_store
        .put(
            &Path::from("existing-provider.toml"),
            evm_toml.as_bytes().into(),
        )
        .await
        .expect("should write EVM provider file to store");

    // Load providers into cache
    providers_store.load_into_cache().await;

    // Remove file directly from store (bypassing the ProvidersStore)
    raw_store
        .delete(&Path::from("existing-provider.toml"))
        .await
        .expect("should remove provider file from store");

    let new_provider = create_test_firehose_provider("existing-provider");

    //* When
    let result = providers_store.register(new_provider).await;

    //* Then
    assert!(
        result.is_err(),
        "Expected register to fail with conflict error"
    );
    let error = result.expect_err("should return register error");
    match error {
        RegisterError::Conflict { name } => {
            assert_eq!(
                name, "existing-provider",
                "Expected conflict for the correct provider name"
            );
        }
        other => panic!("Expected RegisterError::Conflict, got: {:?}", other),
    }
}

/// Create a test ProvidersStore backed by in-memory storage
/// Returns (ProvidersStore, underlying Arc<InMemory>) for testing caching logic
fn create_test_providers_store() -> (ProvidersRegistry, Arc<dyn ObjectStore>) {
    let in_memory_store = Arc::new(InMemory::new());
    let store: Arc<dyn ObjectStore> = in_memory_store.clone();
    let providers_store = ProvidersRegistry::new(ProviderConfigsStore::new(store.clone()));
    (providers_store, store)
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
