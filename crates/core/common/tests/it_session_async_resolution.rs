use std::{
    any::Any,
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use amp_data_store::{
    DataStore, PhyTableRevision,
    physical_table::{PhyTableRevisionPath, PhyTableUrl},
};
use amp_datasets_registry::{DatasetsRegistry, manifests::DatasetManifestsStore};
use amp_object_store::url::ObjectStoreUrl;
use amp_providers_registry::{ProviderConfigsStore, ProvidersRegistry};
use async_trait::async_trait;
use common::{
    catalog::physical::{Catalog, CatalogTable},
    context::{
        exec::ExecContextBuilder,
        session::{SessionStateBuilder, is_user_input_error},
    },
    dataset_store::DatasetStore,
    exec_env::{ExecEnv, default_session_config},
    func_catalog::{
        catalog_provider::{
            AsyncCatalogProvider as FuncAsyncCatalogProvider,
            CatalogProvider as FuncCatalogProvider,
        },
        function_provider::{FunctionProvider, ScalarFunctionProvider},
        schema_provider::{
            AsyncSchemaProvider as FuncAsyncSchemaProvider, SchemaProvider as FuncSchemaProvider,
        },
    },
    physical_table::PhysicalTable,
    sql,
    sql_str::SqlStr,
};
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    catalog::{
        AsyncCatalogProvider as TableAsyncCatalogProvider,
        AsyncSchemaProvider as TableAsyncSchemaProvider, CatalogProvider as TableCatalogProvider,
        SchemaProvider as TableSchemaProvider, TableProvider,
    },
    common::{DFSchemaRef, DataFusionError},
    datasource::empty::EmptyTable,
    execution::runtime_env::RuntimeEnv,
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};
use datasets_common::{
    dataset::Table as DatasetTable, hash_reference::HashReference, network_id::NetworkId,
    table_name::TableName,
};
use js_runtime::isolate_pool::IsolatePool;
use metadata_db::{config::DEFAULT_POOL_MAX_CONNECTIONS, physical_table_revision::LocationId};
use object_store::{ObjectStore, memory::InMemory};
use pgtemp::PgTempDB;
type RequestLog = Arc<Mutex<Vec<String>>>;
type TableCatalogFixture = (Arc<MockTableCatalogProvider>, RequestLog, RequestLog);
type FuncCatalogFixture = (Arc<MockFuncCatalogProvider>, RequestLog, RequestLog);

#[tokio::test]
async fn statement_to_plan_with_qualified_function_resolves_async_catalogs() {
    //* Given
    let (amp_table_catalog, amp_table_schema_requests, amp_table_requests) =
        create_mock_table_catalog("test_schema", "blocks");
    let (unused_table_catalog, unused_table_schema_requests) = create_empty_mock_table_catalog();

    let (amp_func_catalog, amp_func_schema_requests, amp_func_requests) =
        create_mock_func_catalog("test_schema", "identity_udf");
    let (unused_func_catalog, unused_func_schema_requests) = create_empty_mock_func_catalog();

    let session_config = default_session_config().expect("default session config should be valid");
    let session_ctx = SessionStateBuilder::new(session_config)
        .with_table_catalog(
            "amp",
            amp_table_catalog as Arc<dyn TableAsyncCatalogProvider>,
        )
        .with_table_catalog(
            "unused",
            unused_table_catalog as Arc<dyn TableAsyncCatalogProvider>,
        )
        .with_func_catalog("amp", amp_func_catalog as Arc<dyn FuncAsyncCatalogProvider>)
        .with_func_catalog(
            "unused",
            unused_func_catalog as Arc<dyn FuncAsyncCatalogProvider>,
        )
        .build();

    let query = indoc::indoc! {r#"
        SELECT test_schema.identity_udf(value) AS projected
        FROM test_schema.blocks
    "#};
    let stmt = parse_statement(query);

    //* When
    let plan_result = session_ctx.statement_to_plan(stmt).await;

    //* Then
    assert!(
        plan_result.is_ok(),
        "planning should succeed with async-resolved table and function: {plan_result:?}"
    );

    let amp_table_schemas = amp_table_schema_requests
        .lock()
        .expect("table schema request mutex should not be poisoned")
        .clone();
    assert!(
        !amp_table_schemas.is_empty(),
        "referenced table catalog should be resolved"
    );
    assert!(
        amp_table_schemas.iter().all(|name| name == "test_schema"),
        "table catalog resolution should only request referenced schema"
    );

    let amp_tables = amp_table_requests
        .lock()
        .expect("table request mutex should not be poisoned")
        .clone();
    assert!(
        amp_tables.iter().any(|name| name == "blocks"),
        "table resolution should load the referenced table"
    );

    let amp_func_schemas = amp_func_schema_requests
        .lock()
        .expect("function schema request mutex should not be poisoned")
        .clone();
    assert!(
        !amp_func_schemas.is_empty(),
        "referenced function catalog should be resolved"
    );
    assert!(
        amp_func_schemas.iter().all(|name| name == "test_schema"),
        "function catalog resolution should only request referenced schema"
    );

    let amp_functions = amp_func_requests
        .lock()
        .expect("function request mutex should not be poisoned")
        .clone();
    assert!(
        amp_functions.iter().any(|name| name == "identity_udf"),
        "function resolution should load the referenced function"
    );

    let unused_table_schemas = unused_table_schema_requests
        .lock()
        .expect("unused table schema request mutex should not be poisoned")
        .clone();
    assert!(
        unused_table_schemas.is_empty(),
        "unreferenced table catalog should not be resolved"
    );

    let unused_func_schemas = unused_func_schema_requests
        .lock()
        .expect("unused function schema request mutex should not be poisoned")
        .clone();
    assert!(
        unused_func_schemas.is_empty(),
        "unreferenced function catalog should not be resolved"
    );
}

#[tokio::test]
async fn statement_to_schema_with_qualified_function_resolves_async_catalogs() {
    //* Given
    let (amp_table_catalog, amp_table_schema_requests, amp_table_requests) =
        create_mock_table_catalog("test_schema", "blocks");
    let (unused_table_catalog, unused_table_schema_requests) = create_empty_mock_table_catalog();

    let (amp_func_catalog, amp_func_schema_requests, amp_func_requests) =
        create_mock_func_catalog("test_schema", "identity_udf");
    let (unused_func_catalog, unused_func_schema_requests) = create_empty_mock_func_catalog();

    let session_config = default_session_config().expect("default session config should be valid");
    let session_ctx = SessionStateBuilder::new(session_config)
        .with_table_catalog(
            "amp",
            amp_table_catalog as Arc<dyn TableAsyncCatalogProvider>,
        )
        .with_table_catalog(
            "unused",
            unused_table_catalog as Arc<dyn TableAsyncCatalogProvider>,
        )
        .with_func_catalog("amp", amp_func_catalog as Arc<dyn FuncAsyncCatalogProvider>)
        .with_func_catalog(
            "unused",
            unused_func_catalog as Arc<dyn FuncAsyncCatalogProvider>,
        )
        .build();

    let query = indoc::indoc! {r#"
        SELECT test_schema.identity_udf(value) AS projected
        FROM test_schema.blocks
    "#};
    let stmt = parse_statement(query);

    //* When
    let schema_result = session_ctx
        .statement_to_plan(stmt)
        .await
        .map(|p| p.schema().clone());

    //* Then
    let output_schema = schema_result
        .expect("schema inference should succeed with async-resolved table and function");
    assert_schema_contains_projected_field(&output_schema, "projected");

    let amp_table_schemas = amp_table_schema_requests
        .lock()
        .expect("table schema request mutex should not be poisoned")
        .clone();
    assert!(
        !amp_table_schemas.is_empty(),
        "referenced table catalog should be resolved"
    );
    assert!(
        amp_table_schemas.iter().all(|name| name == "test_schema"),
        "table catalog resolution should only request referenced schema"
    );

    let amp_tables = amp_table_requests
        .lock()
        .expect("table request mutex should not be poisoned")
        .clone();
    assert!(
        amp_tables.iter().any(|name| name == "blocks"),
        "table resolution should load the referenced table"
    );

    let amp_func_schemas = amp_func_schema_requests
        .lock()
        .expect("function schema request mutex should not be poisoned")
        .clone();
    assert!(
        !amp_func_schemas.is_empty(),
        "referenced function catalog should be resolved"
    );
    assert!(
        amp_func_schemas.iter().all(|name| name == "test_schema"),
        "function catalog resolution should only request referenced schema"
    );

    let amp_functions = amp_func_requests
        .lock()
        .expect("function request mutex should not be poisoned")
        .clone();
    assert!(
        amp_functions.iter().any(|name| name == "identity_udf"),
        "function resolution should load the referenced function"
    );

    let unused_table_schemas = unused_table_schema_requests
        .lock()
        .expect("unused table schema request mutex should not be poisoned")
        .clone();
    assert!(
        unused_table_schemas.is_empty(),
        "unreferenced table catalog should not be resolved"
    );

    let unused_func_schemas = unused_func_schema_requests
        .lock()
        .expect("unused function schema request mutex should not be poisoned")
        .clone();
    assert!(
        unused_func_schemas.is_empty(),
        "unreferenced function catalog should not be resolved"
    );
}

#[tokio::test]
async fn statement_to_plan_with_bare_function_does_not_trigger_async_function_resolution() {
    //* Given
    let (amp_table_catalog, _amp_table_schema_requests, _amp_table_requests) =
        create_mock_table_catalog("test_schema", "blocks");
    let (amp_func_catalog, amp_func_schema_requests, amp_func_requests) =
        create_mock_func_catalog("test_schema", "identity_udf");

    let session_config = default_session_config().expect("default session config should be valid");
    let session_ctx = SessionStateBuilder::new(session_config)
        .with_table_catalog(
            "amp",
            amp_table_catalog as Arc<dyn TableAsyncCatalogProvider>,
        )
        .with_func_catalog("amp", amp_func_catalog as Arc<dyn FuncAsyncCatalogProvider>)
        .build();

    let query = "SELECT abs(value) AS projected FROM test_schema.blocks";
    let stmt = parse_statement(query);

    //* When
    let plan_result = session_ctx.statement_to_plan(stmt).await;

    //* Then
    assert!(
        plan_result.is_ok(),
        "planning should succeed with built-in bare function"
    );

    let resolved_function_schemas = amp_func_schema_requests
        .lock()
        .expect("function schema request mutex should not be poisoned")
        .clone();
    assert!(
        resolved_function_schemas.is_empty(),
        "bare built-in functions should not trigger async function catalog resolution"
    );

    let resolved_functions = amp_func_requests
        .lock()
        .expect("function request mutex should not be poisoned")
        .clone();
    assert!(
        resolved_functions.is_empty(),
        "no async function fetch should run for bare built-in functions"
    );
}

#[tokio::test]
async fn statement_to_plan_with_catalog_qualified_table_in_pre_resolution_is_invalid_plan_error() {
    //* Given
    let (amp_table_catalog, _amp_table_schema_requests) = create_empty_mock_table_catalog();
    let session_config = default_session_config().expect("default session config should be valid");
    let session_ctx = SessionStateBuilder::new(session_config)
        .with_table_catalog(
            "amp",
            amp_table_catalog as Arc<dyn TableAsyncCatalogProvider>,
        )
        .build();

    let stmt = parse_statement("SELECT * FROM amp.test_schema.blocks");

    //* When
    let result = session_ctx.statement_to_plan(stmt).await;

    //* Then
    let error = result.expect_err("catalog-qualified table should fail pre-resolution");
    assert!(
        is_user_input_error(&error),
        "catalog-qualified table references should be classified as invalid user input: {error:?}"
    );
}

#[tokio::test]
async fn statement_to_schema_with_catalog_qualified_function_in_pre_resolution_is_invalid_plan_error()
 {
    //* Given
    let (amp_func_catalog, _amp_func_schema_requests) = create_empty_mock_func_catalog();
    let session_config = default_session_config().expect("default session config should be valid");
    let session_ctx = SessionStateBuilder::new(session_config)
        .with_func_catalog("amp", amp_func_catalog as Arc<dyn FuncAsyncCatalogProvider>)
        .build();

    let stmt = parse_statement("SELECT amp.test_schema.identity_udf(1) AS projected");

    //* When
    let result = session_ctx
        .statement_to_plan(stmt)
        .await
        .map(|p| p.schema().clone());

    //* Then
    let error = result.expect_err("catalog-qualified function should fail pre-resolution");
    assert!(
        is_user_input_error(&error),
        "catalog-qualified function references should be classified as invalid user input: {error:?}"
    );
}

#[tokio::test]
async fn exec_statement_to_plan_with_qualified_function_uses_async_pre_resolution_flow() {
    //* Given
    let temp_db = PgTempDB::new();
    let metadata_db = metadata_db::connect_pool_with_retry(
        &temp_db.connection_uri(),
        DEFAULT_POOL_MAX_CONNECTIONS,
    )
    .await
    .expect("metadata database should connect");

    let data_dir = tempfile::tempdir().expect("temporary data directory should be created");
    let object_store_url = ObjectStoreUrl::new(data_dir.path().to_string_lossy().to_string())
        .expect("object store URL should be created from temp dir");
    let data_store =
        DataStore::new(metadata_db.clone(), object_store_url, 16).expect("data store should build");

    let manifests_store =
        DatasetManifestsStore::new(Arc::new(InMemory::new()) as Arc<dyn ObjectStore>);
    let datasets_registry = DatasetsRegistry::new(metadata_db.clone(), manifests_store);
    let provider_configs =
        ProviderConfigsStore::new(Arc::new(InMemory::new()) as Arc<dyn ObjectStore>);
    let providers_registry = ProvidersRegistry::new(provider_configs);
    let dataset_store = DatasetStore::new(datasets_registry, providers_registry);

    let runtime_env: Arc<RuntimeEnv> = Default::default();
    let exec_env = ExecEnv {
        session_config: default_session_config().expect("default session config should be valid"),
        global_memory_pool: runtime_env.memory_pool.clone(),
        disk_manager: runtime_env.disk_manager.clone(),
        cache_manager: runtime_env.cache_manager.clone(),
        object_store_registry: runtime_env.object_store_registry.clone(),
        isolate_pool: IsolatePool::new(),
        query_max_mem_mb: 64,
        store: data_store,
        dataset_store,
    };

    let (amp_table_catalog, amp_table_schema_requests, _amp_table_requests) =
        create_mock_table_catalog("test_schema", "blocks");
    let (amp_func_catalog, amp_func_schema_requests, _amp_func_requests) =
        create_mock_func_catalog("test_schema", "identity_udf");

    let catalog = Catalog::default();

    let query_ctx = ExecContextBuilder::new(exec_env)
        .with_table_catalog(
            "amp",
            amp_table_catalog as Arc<dyn TableAsyncCatalogProvider>,
        )
        .with_func_catalog("amp", amp_func_catalog as Arc<dyn FuncAsyncCatalogProvider>)
        .for_catalog(catalog, false)
        .await
        .expect("exec context should build");

    let query = indoc::indoc! {r#"
        SELECT test_schema.identity_udf(value) AS projected
        FROM test_schema.blocks
    "#};
    let stmt = parse_statement(query);

    //* When
    let plan_result = query_ctx.statement_to_plan(stmt).await;

    //* Then
    assert!(
        plan_result.is_ok(),
        "exec SQL planning should succeed through async pre-resolution flow: {plan_result:?}"
    );

    let table_schema_requests = amp_table_schema_requests
        .lock()
        .expect("table schema request mutex should not be poisoned")
        .clone();
    assert!(
        !table_schema_requests.is_empty(),
        "exec path should resolve referenced async table catalog"
    );

    let function_schema_requests = amp_func_schema_requests
        .lock()
        .expect("function schema request mutex should not be poisoned")
        .clone();
    assert!(
        !function_schema_requests.is_empty(),
        "exec path should resolve referenced async function catalog"
    );
}

/// Verifies that exec SQL planning succeeds when async pre-resolution and
/// physical catalog registration reference the same table names.
///
/// The overlap is handled by the `needs_writable_schema` check in
/// `register_catalog`. Async pre-resolution registers a
/// `ResolvedSchemaProvider` (read-only) for planning-only tables. When
/// `register_catalog` encounters an existing schema that is not a
/// `MemorySchemaProvider`, it replaces the entire schema with a fresh empty
/// `MemorySchemaProvider`, discarding the planning-only tables. Physical
/// tables then register on the new empty schema without conflict.
#[tokio::test]
async fn exec_statement_to_plan_with_overlapping_async_and_physical_tables_succeeds() {
    //* Given
    let temp_db = PgTempDB::new();
    let metadata_db = metadata_db::connect_pool_with_retry(
        &temp_db.connection_uri(),
        DEFAULT_POOL_MAX_CONNECTIONS,
    )
    .await
    .expect("metadata database should connect");

    let data_dir = tempfile::tempdir().expect("temporary data directory should be created");
    let object_store_url = ObjectStoreUrl::new(data_dir.path().to_string_lossy().to_string())
        .expect("object store URL should be created from temp dir");
    let data_store =
        DataStore::new(metadata_db.clone(), object_store_url, 16).expect("data store should build");

    let manifests_store =
        DatasetManifestsStore::new(Arc::new(InMemory::new()) as Arc<dyn ObjectStore>);
    let datasets_registry = DatasetsRegistry::new(metadata_db.clone(), manifests_store);
    let provider_configs =
        ProviderConfigsStore::new(Arc::new(InMemory::new()) as Arc<dyn ObjectStore>);
    let providers_registry = ProvidersRegistry::new(provider_configs);
    let dataset_store = DatasetStore::new(datasets_registry, providers_registry);

    let runtime_env: Arc<RuntimeEnv> = Default::default();
    let exec_env = ExecEnv {
        session_config: default_session_config().expect("default session config should be valid"),
        global_memory_pool: runtime_env.memory_pool.clone(),
        disk_manager: runtime_env.disk_manager.clone(),
        cache_manager: runtime_env.cache_manager.clone(),
        object_store_registry: runtime_env.object_store_registry.clone(),
        isolate_pool: IsolatePool::new(),
        query_max_mem_mb: 64,
        store: data_store.clone(),
        dataset_store,
    };

    // Create a physical table under "test_schema.blocks" — the same name
    // that the async table catalog will also resolve.
    let table_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let table_name: TableName = "blocks".parse().expect("table name should be valid");
    let network = NetworkId::new_unchecked("mainnet".to_string());
    let dataset_table = DatasetTable::new(table_name, table_schema, network, vec![]);

    let hash_ref: HashReference =
        "_/test_dataset@b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("hash reference should be valid");

    let revision_path: PhyTableRevisionPath =
        "test_dataset/blocks/00000000-0000-4000-8000-000000000001"
            .parse()
            .expect("revision path should be valid");
    let revision_url = PhyTableUrl::new(data_store.url(), &revision_path);
    let location_id = LocationId::try_from(999_999_i64).expect("location ID should be valid");
    let revision = PhyTableRevision {
        location_id,
        path: revision_path,
        url: revision_url,
    };

    let physical_table = Arc::new(PhysicalTable::from_revision(
        data_store.clone(),
        hash_ref,
        None,
        dataset_table,
        revision,
    ));

    // Async table catalog resolves the same "test_schema.blocks" table.
    let (amp_table_catalog, _, _) = create_mock_table_catalog("test_schema", "blocks");

    let catalog = Catalog::new(
        vec![],
        vec![],
        vec![CatalogTable::new(physical_table, "test_schema".to_string())],
        Default::default(),
    );

    // Use ignore_canonical_segments=true so that the empty revision
    // produces an empty (but valid) TableSnapshot.
    let query_ctx = ExecContextBuilder::new(exec_env)
        .with_table_catalog(
            "amp",
            amp_table_catalog as Arc<dyn TableAsyncCatalogProvider>,
        )
        .for_catalog(catalog, true)
        .await
        .expect("exec context should build with overlapping catalogs");

    let query = "SELECT value FROM test_schema.blocks";
    let stmt = parse_statement(query);

    //* When
    let plan_result = query_ctx.statement_to_plan(stmt).await;

    //* Then
    assert!(
        plan_result.is_ok(),
        "exec planning should succeed when async pre-resolution and physical catalog \
         register the same table name: {plan_result:?}"
    );
}

/// Verifies that a table provider registered under a name that does not
/// match the session's default catalog is never consulted during
/// pre-resolution, causing the query to fail with a "table not found"
/// planning error.
///
/// Misconfigured provider names silently degrade to late planning errors.
/// The builder emits a warning at build time; this test locks the runtime
/// behavior.
#[tokio::test]
async fn statement_to_plan_with_mismatched_table_provider_name_fails_with_table_not_found() {
    //* Given
    let (table_catalog, schema_requests, _table_requests) =
        create_mock_table_catalog("test_schema", "blocks");

    let session_config = default_session_config().expect("default session config should be valid");
    // Register the provider under "wrong_catalog" while the default catalog is "amp".
    let session_ctx = SessionStateBuilder::new(session_config)
        .with_table_catalog(
            "wrong_catalog",
            table_catalog as Arc<dyn TableAsyncCatalogProvider>,
        )
        .build();

    let query = "SELECT value FROM test_schema.blocks";
    let stmt = parse_statement(query);

    //* When
    let result = session_ctx.statement_to_plan(stmt).await;

    //* Then
    assert!(
        result.is_err(),
        "planning should fail because the mismatched provider is never consulted"
    );

    let schemas_requested = schema_requests
        .lock()
        .expect("schema request mutex should not be poisoned")
        .clone();
    assert!(
        schemas_requested.is_empty(),
        "mismatched provider should never be consulted during pre-resolution"
    );
}

/// Verifies that a function provider registered under a name that does
/// not match the session's default catalog is never consulted, causing
/// qualified function calls to fail at planning time.
#[tokio::test]
async fn statement_to_plan_with_mismatched_func_provider_name_fails_with_function_not_found() {
    //* Given
    let (table_catalog, _, _) = create_mock_table_catalog("test_schema", "blocks");
    let (func_catalog, func_schema_requests, _func_requests) =
        create_mock_func_catalog("test_schema", "identity_udf");

    let session_config = default_session_config().expect("default session config should be valid");
    // Table provider matches the default catalog ("amp"), so the table resolves.
    // Function provider is registered under "wrong_catalog", so the function won't resolve.
    let session_ctx = SessionStateBuilder::new(session_config)
        .with_table_catalog("amp", table_catalog as Arc<dyn TableAsyncCatalogProvider>)
        .with_func_catalog(
            "wrong_catalog",
            func_catalog as Arc<dyn FuncAsyncCatalogProvider>,
        )
        .build();

    let query = "SELECT test_schema.identity_udf(value) AS projected FROM test_schema.blocks";
    let stmt = parse_statement(query);

    //* When
    let result = session_ctx.statement_to_plan(stmt).await;

    //* Then
    assert!(
        result.is_err(),
        "planning should fail because the mismatched function provider is never consulted"
    );

    let func_schemas_requested = func_schema_requests
        .lock()
        .expect("function schema request mutex should not be poisoned")
        .clone();
    assert!(
        func_schemas_requested.is_empty(),
        "mismatched function provider should never be consulted during pre-resolution"
    );
}

#[derive(Debug)]
struct MockTableSchemaProvider {
    tables: BTreeMap<String, Arc<dyn TableProvider>>,
    requested_tables: Arc<Mutex<Vec<String>>>,
}

impl MockTableSchemaProvider {
    fn new(
        tables: BTreeMap<String, Arc<dyn TableProvider>>,
        requested_tables: Arc<Mutex<Vec<String>>>,
    ) -> Self {
        Self {
            tables,
            requested_tables,
        }
    }
}

#[async_trait]
impl TableSchemaProvider for MockTableSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        self.requested_tables
            .lock()
            .expect("table request mutex should not be poisoned")
            .push(name.to_string());
        Ok(self.tables.get(name).cloned())
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

#[async_trait]
impl TableAsyncSchemaProvider for MockTableSchemaProvider {
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        <Self as TableSchemaProvider>::table(self, name).await
    }
}

#[derive(Debug)]
struct MockTableCatalogProvider {
    schemas: BTreeMap<String, Arc<MockTableSchemaProvider>>,
    requested_schemas: Arc<Mutex<Vec<String>>>,
}

impl MockTableCatalogProvider {
    fn new(
        schemas: BTreeMap<String, Arc<MockTableSchemaProvider>>,
        requested_schemas: Arc<Mutex<Vec<String>>>,
    ) -> Self {
        Self {
            schemas,
            requested_schemas,
        }
    }
}

impl TableCatalogProvider for MockTableCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn TableSchemaProvider>> {
        self.schemas
            .get(name)
            .cloned()
            .map(|schema| schema as Arc<dyn TableSchemaProvider>)
    }
}

#[async_trait]
impl TableAsyncCatalogProvider for MockTableCatalogProvider {
    async fn schema(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn TableAsyncSchemaProvider>>, DataFusionError> {
        self.requested_schemas
            .lock()
            .expect("schema request mutex should not be poisoned")
            .push(name.to_string());
        Ok(self
            .schemas
            .get(name)
            .cloned()
            .map(|schema| schema as Arc<dyn TableAsyncSchemaProvider>))
    }
}

#[derive(Debug)]
struct MockFuncSchemaProvider {
    functions: BTreeMap<String, Arc<dyn FunctionProvider>>,
    requested_functions: Arc<Mutex<Vec<String>>>,
}

impl MockFuncSchemaProvider {
    fn new(
        functions: BTreeMap<String, Arc<dyn FunctionProvider>>,
        requested_functions: Arc<Mutex<Vec<String>>>,
    ) -> Self {
        Self {
            functions,
            requested_functions,
        }
    }
}

#[async_trait]
impl FuncSchemaProvider for MockFuncSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn function_names(&self) -> Vec<String> {
        self.functions.keys().cloned().collect()
    }

    async fn function(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn FunctionProvider>>, DataFusionError> {
        self.requested_functions
            .lock()
            .expect("function request mutex should not be poisoned")
            .push(name.to_string());
        Ok(self.functions.get(name).cloned())
    }

    fn function_exist(&self, name: &str) -> bool {
        self.functions.contains_key(name)
    }
}

#[async_trait]
impl FuncAsyncSchemaProvider for MockFuncSchemaProvider {
    async fn function(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn FunctionProvider>>, DataFusionError> {
        <Self as FuncSchemaProvider>::function(self, name).await
    }
}

#[derive(Debug)]
struct MockFuncCatalogProvider {
    schemas: BTreeMap<String, Arc<MockFuncSchemaProvider>>,
    requested_schemas: Arc<Mutex<Vec<String>>>,
}

impl MockFuncCatalogProvider {
    fn new(
        schemas: BTreeMap<String, Arc<MockFuncSchemaProvider>>,
        requested_schemas: Arc<Mutex<Vec<String>>>,
    ) -> Self {
        Self {
            schemas,
            requested_schemas,
        }
    }
}

impl FuncCatalogProvider for MockFuncCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn FuncSchemaProvider>> {
        self.schemas
            .get(name)
            .cloned()
            .map(|schema| schema as Arc<dyn FuncSchemaProvider>)
    }
}

#[async_trait]
impl FuncAsyncCatalogProvider for MockFuncCatalogProvider {
    async fn schema(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn FuncAsyncSchemaProvider>>, DataFusionError> {
        self.requested_schemas
            .lock()
            .expect("function schema request mutex should not be poisoned")
            .push(name.to_string());
        Ok(self
            .schemas
            .get(name)
            .cloned()
            .map(|schema| schema as Arc<dyn FuncAsyncSchemaProvider>))
    }
}

fn parse_statement(sql_text: &str) -> datafusion::sql::parser::Statement {
    let sql = sql_text
        .parse::<SqlStr>()
        .expect("SQL should be valid SqlStr");
    sql::parse(&sql).expect("SQL should parse into a statement")
}

fn create_identity_udf(name: &str) -> Arc<ScalarUDF> {
    Arc::new(create_udf(
        name,
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| match args.first() {
            Some(first) => Ok(first.clone()),
            None => Err(DataFusionError::Execution(
                "identity UDF expects one argument".to_string(),
            )),
        }),
    ))
}

fn create_mock_table_catalog(schema_name: &str, table_name: &str) -> TableCatalogFixture {
    let schema_requests = Arc::new(Mutex::new(Vec::new()));
    let table_requests = Arc::new(Mutex::new(Vec::new()));

    let table_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let table_provider: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(table_schema));

    let mut tables = BTreeMap::new();
    tables.insert(table_name.to_string(), table_provider);

    let schema_provider = Arc::new(MockTableSchemaProvider::new(tables, table_requests.clone()));
    let mut schemas = BTreeMap::new();
    schemas.insert(schema_name.to_string(), schema_provider);

    (
        Arc::new(MockTableCatalogProvider::new(
            schemas,
            schema_requests.clone(),
        )),
        schema_requests,
        table_requests,
    )
}

fn create_empty_mock_table_catalog() -> (Arc<MockTableCatalogProvider>, Arc<Mutex<Vec<String>>>) {
    let schema_requests = Arc::new(Mutex::new(Vec::new()));
    (
        Arc::new(MockTableCatalogProvider::new(
            Default::default(),
            schema_requests.clone(),
        )),
        schema_requests,
    )
}

fn create_mock_func_catalog(schema_name: &str, function_name: &str) -> FuncCatalogFixture {
    let schema_requests = Arc::new(Mutex::new(Vec::new()));
    let function_requests = Arc::new(Mutex::new(Vec::new()));

    let udf = create_identity_udf(&format!("{schema_name}.{function_name}"));
    let function_provider: Arc<dyn FunctionProvider> = Arc::new(ScalarFunctionProvider::from(udf));

    let mut functions = BTreeMap::new();
    functions.insert(function_name.to_string(), function_provider);

    let schema_provider = Arc::new(MockFuncSchemaProvider::new(
        functions,
        function_requests.clone(),
    ));
    let mut schemas = BTreeMap::new();
    schemas.insert(schema_name.to_string(), schema_provider);

    (
        Arc::new(MockFuncCatalogProvider::new(
            schemas,
            schema_requests.clone(),
        )),
        schema_requests,
        function_requests,
    )
}

fn create_empty_mock_func_catalog() -> (Arc<MockFuncCatalogProvider>, Arc<Mutex<Vec<String>>>) {
    let schema_requests = Arc::new(Mutex::new(Vec::new()));
    (
        Arc::new(MockFuncCatalogProvider::new(
            Default::default(),
            schema_requests.clone(),
        )),
        schema_requests,
    )
}

fn assert_schema_contains_projected_field(schema: &DFSchemaRef, field_name: &str) {
    assert_eq!(schema.fields().len(), 1, "expected one projected field");

    let projected = schema.field(0);
    assert_eq!(
        projected.name(),
        field_name,
        "projected field name should match alias"
    );
    assert_eq!(
        projected.data_type(),
        &DataType::Int64,
        "projected field type should be int64"
    );
}
