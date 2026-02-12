use std::sync::Arc;

use amp_providers_common::{
    ProviderName,
    config::{ConfigHeaderWithNetwork, ProviderConfigRaw, TryIntoConfig},
};
use object_store::{ObjectStore, memory::InMemory, path::Path};

use crate::{ProviderConfigsStore, ProvidersRegistry};

#[tokio::test]
async fn register_with_valid_provider_makes_immediately_available() {
    //* Given
    let (store, _configs_store, _raw_store) = create_test_providers_store();
    let (evm_name, evm_config) = create_test_evm_provider("evm_mainnet");
    let (fhs_name, fhs_config) = create_test_firehose_provider("fhs_polygon");

    // Register first provider
    store
        .register(evm_name.clone(), evm_config.clone())
        .await
        .expect("should register first provider");

    //* When
    let register_result = store.register(fhs_name.clone(), fhs_config.clone()).await;

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

    let fhs_retrieved = store.get_by_name("fhs_polygon").await;
    assert!(
        fhs_retrieved.is_some(),
        "Expected fhs_polygon to be immediately available"
    );

    // Verify files were created in the in-memory store
    use object_store::path::Path;
    let evm_path = Path::from("evm_mainnet.toml");
    let fhs_path = Path::from("fhs_polygon.toml");

    let evm_result = _raw_store.get(&evm_path).await;
    assert!(
        evm_result.is_ok(),
        "Expected evm_mainnet.toml file to exist in store"
    );

    let fhs_result = _raw_store.get(&fhs_path).await;
    assert!(
        fhs_result.is_ok(),
        "Expected fhs_polygon.toml file to exist in store"
    );

    // Verify file contents are correct
    let evm_bytes = evm_result
        .unwrap()
        .bytes()
        .await
        .expect("should read evm bytes");
    let evm_file_contents =
        String::from_utf8(evm_bytes.to_vec()).expect("should convert to string");
    let parsed_evm_provider: ProviderConfigRaw =
        toml::from_str(&evm_file_contents).expect("should parse evm_mainnet file");
    let parsed_evm_header = parsed_evm_provider
        .try_into_config::<ConfigHeaderWithNetwork>()
        .expect("should parse evm header");
    let evm_header = evm_config
        .try_into_config::<ConfigHeaderWithNetwork>()
        .expect("should parse evm header");
    assert_eq!(parsed_evm_header.kind, evm_header.kind);
    assert_eq!(parsed_evm_header.network, evm_header.network);

    let fhs_bytes = fhs_result
        .unwrap()
        .bytes()
        .await
        .expect("should read fhs bytes");
    let fhs_file_contents =
        String::from_utf8(fhs_bytes.to_vec()).expect("should convert to string");
    let parsed_fhs_provider: ProviderConfigRaw =
        toml::from_str(&fhs_file_contents).expect("should parse fhs_polygon file");
    let parsed_fhs_header = parsed_fhs_provider
        .try_into_config::<ConfigHeaderWithNetwork>()
        .expect("should parse fhs header");
    let fhs_header = fhs_config
        .try_into_config::<ConfigHeaderWithNetwork>()
        .expect("should parse fhs header");
    assert_eq!(parsed_fhs_header.kind, fhs_header.kind);
    assert_eq!(parsed_fhs_header.network, fhs_header.network);
}

#[tokio::test]
async fn get_by_name_after_registration_returns_provider() {
    //* Given
    let (store, _configs_store, _raw_store) = create_test_providers_store();
    let provider_name = "consistent_provider";
    let (name, config) = create_test_evm_provider(provider_name);

    store
        .register(name.clone(), config)
        .await
        .expect("should register provider");

    //* When
    let provider_option = store.get_by_name(provider_name).await;

    //* Then
    assert!(
        provider_option.is_some(),
        "Expected to find provider by name"
    );
    let retrieved_provider = provider_option.expect("should have retrieved provider");
    let header = retrieved_provider
        .try_into_config::<ConfigHeaderWithNetwork>()
        .expect("should parse header");
    assert_eq!(header.network, "mainnet", "Expected correct provider data");
}

#[tokio::test]
async fn delete_with_existing_provider_removes_from_operations() {
    //* Given
    let (store, _configs_store, _raw_store) = create_test_providers_store();
    let (evm_name, evm_config) = create_test_evm_provider("evm_to_delete");
    let (fhs_name, fhs_config) = create_test_firehose_provider("fhs_to_keep");

    store
        .register(evm_name.clone(), evm_config)
        .await
        .expect("should register first provider");
    store
        .register(fhs_name.clone(), fhs_config)
        .await
        .expect("should register second provider");

    //* When
    let delete_result = store.delete("evm_to_delete").await;

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

    let deleted_provider = store.get_by_name("evm_to_delete").await;
    assert!(
        deleted_provider.is_none(),
        "Expected deleted provider to be unavailable"
    );

    let kept_provider = store.get_by_name("fhs_to_keep").await;
    assert!(
        kept_provider.is_some(),
        "Expected kept provider to remain available"
    );

    // Verify deleted file was removed from in-memory store
    use object_store::path::Path;
    let deleted_path = Path::from("evm_to_delete.toml");
    let kept_path = Path::from("fhs_to_keep.toml");

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
    let (providers_store, _configs_store, raw_store) = create_test_providers_store();

    // Manually write provider TOML files to the in-memory store
    let evm_toml = indoc::indoc! {r#"
        kind = "evm-rpc"
        network = "mainnet"
        url = "http://localhost:8545"
        rate_limit_per_minute = 100
    "#};
    let firehose_toml = indoc::indoc! {r#"
        kind = "firehose"
        network = "polygon"
        url = "https://polygon.firehose.io"
        token = "secret"
    "#};

    raw_store
        .put(&Path::from("evm_mainnet.toml"), evm_toml.as_bytes().into())
        .await
        .expect("should write EVM provider file to store");
    raw_store
        .put(
            &Path::from("firehose_polygon.toml"),
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
    let evm_provider = providers_store.get_by_name("evm_mainnet").await;
    assert!(evm_provider.is_some(), "Expected EVM provider to be loaded");
    let evm = evm_provider.expect("should have EVM provider");
    let evm_header = evm
        .try_into_config::<ConfigHeaderWithNetwork>()
        .expect("should parse evm header");
    assert_eq!(evm_header.kind, "evm-rpc");
    assert_eq!(evm_header.network, "mainnet");

    // Verify Firehose provider was loaded correctly
    let firehose_provider = providers_store.get_by_name("firehose_polygon").await;
    assert!(
        firehose_provider.is_some(),
        "Expected Firehose provider to be loaded"
    );
    let firehose = firehose_provider.expect("should have Firehose provider");
    let firehose_header = firehose
        .try_into_config::<ConfigHeaderWithNetwork>()
        .expect("should parse firehose header");
    assert_eq!(firehose_header.kind, "firehose");
    assert_eq!(firehose_header.network, "polygon");
}

#[tokio::test]
async fn get_all_with_invalid_toml_handles_gracefully() {
    //* Given
    let (providers_store, _configs_store, raw_store) = create_test_providers_store();

    // Write one valid and one invalid TOML file to the in-memory store
    let valid_toml = indoc::indoc! {r#"
        kind = "evm-rpc"
        network = "mainnet"
        url = "http://localhost:8545"
        rate_limit_per_minute = 100
    "#};

    let invalid_toml = indoc::indoc! {r#"
        kind = "evm-rpc"
        network = "mainnet"
        url = "http://localhost:8545
        # Missing closing quote on url field
        rate_limit_per_minute = 100
    "#};

    raw_store
        .put(
            &Path::from("valid_provider.toml"),
            valid_toml.as_bytes().into(),
        )
        .await
        .expect("should write valid TOML file to store");
    raw_store
        .put(
            &Path::from("invalid_provider.toml"),
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
    let header = provider
        .try_into_config::<ConfigHeaderWithNetwork>()
        .expect("should parse header");
    assert_eq!(header.kind, "evm-rpc");
    assert_eq!(header.network, "mainnet");
}

#[tokio::test]
async fn get_all_with_externally_removed_file_returns_cached_data() {
    //* Given
    let (providers_store, configs_store, raw_store) = create_test_providers_store();

    // Write provider files to the in-memory store
    let evm_toml = indoc::indoc! {r#"
        kind = "evm-rpc"
        network = "mainnet"
        url = "http://localhost:8545"
        rate_limit_per_minute = 100
    "#};
    let firehose_toml = indoc::indoc! {r#"
        kind = "firehose"
        network = "polygon"
        url = "https://polygon.firehose.io"
        token = "secret"
    "#};

    raw_store
        .put(&Path::from("evm_provider.toml"), evm_toml.as_bytes().into())
        .await
        .expect("should write EVM provider file to store");
    raw_store
        .put(
            &Path::from("firehose_provider.toml"),
            firehose_toml.as_bytes().into(),
        )
        .await
        .expect("should write Firehose provider file to store");

    // Load providers into cache
    configs_store.load_into_cache().await;

    // Remove one file directly from store (bypassing the ProvidersStore)
    raw_store
        .delete(&Path::from("evm_provider.toml"))
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
    let (providers_store, configs_store, raw_store) = create_test_providers_store();

    let evm_toml = indoc::indoc! {r#"
        kind = "evm-rpc"
        network = "mainnet"
        url = "http://localhost:8545"
        rate_limit_per_minute = 100
    "#};

    raw_store
        .put(&Path::from("evm_provider.toml"), evm_toml.as_bytes().into())
        .await
        .expect("should write EVM provider file to store");

    // Load providers into cache
    configs_store.load_into_cache().await;

    // Remove file directly from store (bypassing the ProvidersStore)
    raw_store
        .delete(&Path::from("evm_provider.toml"))
        .await
        .expect("should remove EVM provider file from store");

    //* When
    let provider_option = providers_store.get_by_name("evm_provider").await;

    //* Then
    assert!(
        provider_option.is_some(),
        "Expected removed provider to still be found in cache"
    );
}

#[tokio::test]
async fn delete_with_externally_removed_file_succeeds_idempotent() {
    //* Given
    let (providers_store, configs_store, raw_store) = create_test_providers_store();

    let evm_toml = indoc::indoc! {r#"
        kind = "evm-rpc"
        network = "mainnet"
        url = "http://localhost:8545"
        rate_limit_per_minute = 100
    "#};

    raw_store
        .put(&Path::from("evm_provider.toml"), evm_toml.as_bytes().into())
        .await
        .expect("should write EVM provider file to store");

    // Load providers into cache
    configs_store.load_into_cache().await;

    // Remove file directly from store (bypassing the ProvidersStore)
    raw_store
        .delete(&Path::from("evm_provider.toml"))
        .await
        .expect("should remove EVM provider file from store");

    //* When
    let result = providers_store.delete("evm_provider").await;

    //* Then
    assert!(
        result.is_ok(),
        "Expected delete to succeed (idempotent) for externally removed file"
    );
}

#[tokio::test]
async fn register_with_externally_removed_file_overwrites_cached_entry() {
    //* Given
    let (providers_store, configs_store, raw_store) = create_test_providers_store();

    let evm_toml = indoc::indoc! {r#"
        name = "existing_provider"
        kind = "evm-rpc"
        network = "mainnet"
        url = "http://localhost:8545"
        rate_limit_per_minute = 100
    "#};

    raw_store
        .put(
            &Path::from("existing_provider.toml"),
            evm_toml.as_bytes().into(),
        )
        .await
        .expect("should write EVM provider file to store");

    // Load providers into cache
    configs_store.load_into_cache().await;

    // Remove file directly from store (bypassing the ProvidersStore)
    raw_store
        .delete(&Path::from("existing_provider.toml"))
        .await
        .expect("should remove provider file from store");

    let (name, new_config) = create_test_firehose_provider("existing_provider");

    //* When
    let result = providers_store.register(name.clone(), new_config).await;

    //* Then
    assert!(
        result.is_ok(),
        "Expected register to succeed with overwrite, got: {:?}",
        result
    );

    // Verify the provider was overwritten with new data
    let provider = providers_store
        .get_by_name("existing_provider")
        .await
        .expect("should have provider");
    let header = provider
        .try_into_config::<ConfigHeaderWithNetwork>()
        .expect("should parse header");
    assert_eq!(
        header.kind, "firehose",
        "Expected provider to be overwritten with firehose type"
    );

    // Verify the file was recreated in the store
    let file_exists = raw_store
        .head(&Path::from("existing_provider.toml"))
        .await
        .is_ok();
    assert!(file_exists, "Expected file to be recreated in store");
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
    (providers_registry, configs_store, store)
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
