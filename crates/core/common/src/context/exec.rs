use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{Arc, LazyLock},
};

use amp_data_store::DataStore;
use arrow::{array::ArrayRef, compute::concat_batches};
use datafusion::{
    self,
    arrow::array::RecordBatch,
    catalog::{AsyncCatalogProvider as TableAsyncCatalogProvider, MemorySchemaProvider},
    common::tree_node::{Transformed, TreeNode},
    datasource::{DefaultTableSource, TableType},
    error::DataFusionError,
    execution::{
        SendableRecordBatchStream, TaskContext,
        cache::cache_manager::CacheManager,
        config::SessionConfig,
        disk_manager::DiskManager,
        memory_pool::MemoryPool,
        object_store::ObjectStoreRegistry,
    },
    logical_expr::{LogicalPlan, ScalarUDF, TableScan, expr::ScalarFunction},
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{ExecutionPlan, displayable, execute_stream, stream::RecordBatchStreamAdapter},
    prelude::Expr,
    sql::parser,
};
use datafusion_tracing::{
    InstrumentationOptions, instrument_with_info_spans, pretty_format_compact_batch,
};
use datasets_common::network_id::NetworkId;
use futures::{TryStreamExt, stream};
use js_runtime::isolate_pool::IsolatePool;
use regex::Regex;
use tracing::field;
use tracing_futures::Instrument as _;

use crate::{
    BlockNum, BlockRange, arrow,
    catalog::physical::{
        Catalog,
        snapshot::{CatalogSnapshot, FromCatalogError, QueryableSnapshot},
    },
    context::{
        common::{INVALID_INPUT_CONTEXT, ReadOnlyCheckError, read_only_check},
        session::{SessionContext, SessionState, SessionStateBuilder},
    },
    datasets_cache::DatasetsCache,
    exec_env::ExecEnv,
    func_catalog::catalog_provider::AsyncCatalogProvider as FuncAsyncCatalogProvider,
    memory_pool::{MemoryPoolKind, TieredMemoryPool, make_memory_pool},
    physical_table::MultiNetworkSegmentsError,
    plan_visitors::{
        extract_table_references_from_plan, forbid_duplicate_field_names,
        forbid_underscore_prefixed_aliases,
    },
    sql::{TableReference, TableReferenceConversionError},
    udfs::{builtin_udfs, eth_call::EthCallUdfsCache, plan::PlanJsUdf},
};

/// A context for executing queries against a catalog.
///
/// Holds everything needed to plan and execute a single query: the catalog
/// snapshot, JavaScript UDF runtime, memory budget, and the DataFusion session
/// that ties them together.
#[derive(Clone)]
pub struct ExecContext {
    /// Shared execution environment (network, block range, data store, etc.).
    pub env: ExecEnv,
    /// Pool of V8 isolates used to run JavaScript UDFs during execution.
    isolate_pool: IsolatePool,
    /// Point-in-time snapshot of the physical catalog (segments and tables).
    physical_table: CatalogSnapshot,
    /// Queryable views derived from `physical_table`, one per registered table.
    query_snapshots: Vec<Arc<QueryableSnapshot>>,
    /// Per-query memory pool (if per-query limits are enabled).
    tiered_memory_pool: Arc<TieredMemoryPool>,
    /// Custom session context that owns the runtime environment and optimizer
    /// rules. All session creation goes through `session_ctx.state()`
    /// so that runtime configuration is never duplicated across call sites.
    session_ctx: SessionContext,
}

impl ExecContext {
    /// Returns the isolate pool for JavaScript UDF execution.
    pub fn isolate_pool(&self) -> &IsolatePool {
        &self.isolate_pool
    }

    /// Returns the tiered memory pool for this query context.
    pub fn memory_pool(&self) -> &Arc<TieredMemoryPool> {
        &self.tiered_memory_pool
    }

    /// Attaches a detached logical plan to this query context in a single
    /// traversal that, for each plan node:
    ///
    /// 1. Replaces `PlanTable` table sources in `TableScan` nodes with actual
    ///    `QueryableSnapshot` providers from the catalog.
    /// 2. Rewrites `PlanJsUdf`-backed scalar functions inside the node's
    ///    expression tree into execution-ready UDFs by attaching the
    ///    `IsolatePool`.
    #[tracing::instrument(skip_all, err)]
    pub(crate) fn attach(&self, plan: LogicalPlan) -> Result<LogicalPlan, DataFusionError> {
        let mut cache: HashMap<String, Arc<ScalarUDF>> = HashMap::new();
        let pool = self.isolate_pool.clone();

        plan.transform_with_subqueries(|node| {
            let node = attach_table_node(node, self)?;
            node.transform_data(|n| attach_js_udf_exprs(n, &pool, &mut cache))
        })
        .map(|t| t.data)
    }

    /// Returns the physical catalog snapshot backing this query context.
    ///
    /// Exposes segment-level data for streaming query consumers that need to
    /// walk segments (e.g. watermark computation, compaction). The execution
    /// layer accesses tables via `get_table()` instead.
    pub fn physical_table(&self) -> &CatalogSnapshot {
        &self.physical_table
    }

    /// Converts a parsed SQL statement into a logical plan against the physical catalog.
    ///
    /// Performs async pre-resolution, registers physical catalog tables,
    /// then enforces consumer-level policies: forbidden underscore-prefixed
    /// aliases and read-only constraints.
    pub async fn statement_to_plan(
        &self,
        query: parser::Statement,
    ) -> Result<LogicalPlan, SqlError> {
        let mut state = self
            .session_ctx
            .resolved_state(&query)
            .await
            .map_err(SqlError::Planning)?;
        register_catalog(
            &self.env,
            &mut state,
            &self.physical_table,
            &self.query_snapshots,
        )
        .map_err(SqlError::RegisterTable)?;

        let plan = state.statement_to_plan(query).await.map_err(|err| {
            SqlError::Planning(StatementToPlanError::StatementToPlan(err).into_datafusion_error())
        })?;

        read_only_check(&plan).map_err(|err| {
            SqlError::Planning(StatementToPlanError::ReadOnlyCheck(err).into_datafusion_error())
        })?;
        forbid_underscore_prefixed_aliases(&plan).map_err(|err| {
            SqlError::Planning(StatementToPlanError::ForbiddenAliases(err).into_datafusion_error())
        })?;

        Ok(plan)
    }

    /// Executes a logical plan and returns a streaming result set.
    pub async fn execute_plan(
        &self,
        plan: LogicalPlan,
        logical_optimize: bool,
    ) -> Result<SendableRecordBatchStream, ExecutePlanError> {
        let mut state = self.session_ctx.state();
        register_catalog(
            &self.env,
            &mut state,
            &self.physical_table,
            &self.query_snapshots,
        )
        .map_err(ExecutePlanError::RegisterTable)?;

        let result = execute_plan(&state, plan, logical_optimize)
            .await
            .map_err(ExecutePlanError::Execute)?;

        Ok(result)
    }

    /// This will load the result set entirely in memory, so it should be used with caution.
    pub async fn execute_and_concat(
        &self,
        plan: LogicalPlan,
    ) -> Result<RecordBatch, ExecuteAndConcatError> {
        let schema = plan.schema().inner().clone();
        let mut state = self.session_ctx.state();
        register_catalog(
            &self.env,
            &mut state,
            &self.physical_table,
            &self.query_snapshots,
        )
        .map_err(ExecuteAndConcatError::RegisterTable)?;

        let batch_stream = execute_plan(&state, plan, true)
            .await
            .map_err(ExecuteAndConcatError::Execute)?
            .try_collect::<Vec<_>>()
            .await
            .map_err(ExecuteAndConcatError::CollectResults)?;

        concat_batches(&schema, &batch_stream)
            .map_err(|err| ExecuteAndConcatError::ConcatBatches(err.into()))
    }

    /// Looks up a queryable snapshot by reference. Fails if the table is not in the catalog.
    pub fn get_table(
        &self,
        table_ref: &TableReference,
    ) -> Result<Arc<QueryableSnapshot>, TableNotFoundError> {
        self.query_snapshots
            .iter()
            .find(|qs| qs.table_ref() == *table_ref)
            .cloned()
            .ok_or_else(|| TableNotFoundError(table_ref.clone()))
    }

    /// Return the block ranges that are synced for all tables referenced by the given plan,
    /// grouped by network. For each network, returns the intersection of all tables' ranges
    /// (maximum start block to minimum end block).
    ///
    /// Returns an empty Vec if any referenced table has no synced data.
    #[tracing::instrument(skip_all, err)]
    pub async fn common_ranges(
        &self,
        plan: &LogicalPlan,
    ) -> Result<Vec<BlockRange>, CommonRangesError> {
        let mut ranges_by_network: BTreeMap<NetworkId, BlockRange> = BTreeMap::new();
        for df_table_ref in extract_table_references_from_plan(plan)
            .map_err(CommonRangesError::ExtractTableReferences)?
        {
            let table_ref: TableReference<String> = df_table_ref
                .try_into()
                .map_err(CommonRangesError::TableReferenceConversion)?;
            let Some(table_range) = self
                .get_table(&table_ref)
                .map_err(CommonRangesError::TableNotFound)?
                .synced_range()
            else {
                // If any table has no synced data, return empty ranges
                return Ok(vec![]);
            };
            ranges_by_network
                .entry(table_range.network.clone())
                .and_modify(|existing| {
                    // Intersect: take max start, min end
                    if table_range.start() > existing.start() {
                        existing.numbers = table_range.start()..=existing.end();
                        existing.prev_hash = table_range.prev_hash;
                    }
                    if table_range.end() < existing.end() {
                        existing.numbers = existing.start()..=table_range.end();
                        existing.hash = table_range.hash;
                    }
                })
                .or_insert(table_range);
        }
        Ok(ranges_by_network.into_values().collect())
    }

    /// Get the most recent block that has been synced for all tables in the plan,
    /// grouped by network. Returns a map from network name to the maximum end block
    /// for that network.
    ///
    /// Returns an empty map if any referenced table has no synced data.
    #[tracing::instrument(skip_all, err)]
    pub async fn max_end_blocks(
        &self,
        plan: &LogicalPlan,
    ) -> Result<BTreeMap<NetworkId, BlockNum>, CommonRangesError> {
        Ok(self
            .common_ranges(plan)
            .await?
            .into_iter()
            .map(|range| (range.network, *range.numbers.end()))
            .collect())
    }
}

/// Failed to create a `ExecContext` from a catalog
///
/// This error covers failures during `ExecContext::for_catalog()`.
#[derive(Debug, thiserror::Error)]
pub enum CreateContextError {
    /// Failed to create a catalog snapshot from the physical catalog
    ///
    /// This occurs when creating snapshots of physical tables, typically
    /// due to segment metadata fetch failures or canonical chain computation errors.
    #[error("failed to create catalog snapshot")]
    CatalogSnapshot(#[source] FromCatalogError),
    /// A physical table has multi-network segments, which the query layer does not support.
    ///
    /// `QueryableSnapshot::from_snapshot()` requires single-network segments because
    /// `synced_range()` can only express a single-network block range. This error is
    /// returned when that invariant is violated.
    #[error("failed to build query snapshot")]
    MultiNetworkSegments(#[source] MultiNetworkSegmentsError),
}

/// Builder for [`ExecContext`].
///
/// Accepts the decomposed components that were previously combined into an
/// umbrella [`ExecEnv`] object. Internally composes a [`SessionStateBuilder`]
/// for session construction; no session builder internals are exposed through
/// the public API.
///
/// Construction is a two-step process: configure the builder with
/// [`new`](Self::new), then call [`for_catalog`](Self::for_catalog) to
/// asynchronously create the [`ExecContext`] (catalog snapshot creation
/// requires async canonical-chain resolution that cannot happen in a sync
/// builder).
///
/// # Async catalog providers (planning-only)
///
/// The builder exposes [`with_table_catalog`](Self::with_table_catalog) and
/// [`with_func_catalog`](Self::with_func_catalog) for registering async
/// catalog providers. These feed into the session's
/// `resolved_state()` pre-resolution pipeline, which is
/// invoked by [`ExecContext::statement_to_plan`].
///
/// **Current production state**: no production call site wires async catalog
/// providers on exec. The eager [`CatalogSnapshot`] path (physical tables
/// registered via [`register_catalog`]) is the primary exec-path table/function
/// source. When both catalog maps are empty the session's pre-resolution
/// early-outs to the eager path, so production exec planning incurs no async
/// overhead. The async APIs are retained for testing and future use.
pub struct ExecContextBuilder {
    session_config: SessionConfig,
    store: DataStore,
    datasets_cache: DatasetsCache,
    ethcall_udfs_cache: EthCallUdfsCache,
    isolate_pool: Option<IsolatePool>,
    global_memory_pool: Arc<dyn MemoryPool>,
    query_max_mem_mb: usize,
    disk_manager: Arc<DiskManager>,
    cache_manager: Arc<CacheManager>,
    object_store_registry: Arc<dyn ObjectStoreRegistry>,
    table_catalogs: BTreeMap<String, Arc<dyn TableAsyncCatalogProvider>>,
    func_catalogs: BTreeMap<String, Arc<dyn FuncAsyncCatalogProvider>>,
}

impl ExecContextBuilder {
    /// Creates a new builder from an [`ExecEnv`].
    ///
    /// All mandatory execution components are sourced from the environment.
    /// Optional async catalog providers can be added via
    /// [`with_table_catalog`](Self::with_table_catalog) and
    /// [`with_func_catalog`](Self::with_func_catalog).
    pub fn new(env: ExecEnv) -> Self {
        Self {
            session_config: env.session_config,
            store: env.store,
            datasets_cache: env.datasets_cache,
            ethcall_udfs_cache: env.ethcall_udfs_cache,
            isolate_pool: None,
            global_memory_pool: env.global_memory_pool,
            query_max_mem_mb: env.query_max_mem_mb,
            disk_manager: env.disk_manager,
            cache_manager: env.cache_manager,
            object_store_registry: env.object_store_registry,
            table_catalogs: Default::default(),
            func_catalogs: Default::default(),
        }
    }

    /// Registers a named async table catalog provider for SQL pre-resolution.
    ///
    /// Providers are consulted by the custom session during `statement_to_plan` before
    /// SQL-to-plan conversion. Only catalogs referenced by the query are
    /// resolved. The `name` must match the `SessionConfig` default catalog
    /// for the provider to be reachable — see [`SessionStateBuilder`] docs.
    /// Not wired at production call sites — see struct-level docs.
    pub fn with_table_catalog(
        mut self,
        name: impl Into<String>,
        provider: Arc<dyn TableAsyncCatalogProvider>,
    ) -> Self {
        self.table_catalogs.insert(name.into(), provider);
        self
    }

    /// Registers a named async function catalog provider for SQL pre-resolution.
    ///
    /// Providers are consulted by the custom session during `statement_to_plan` before
    /// SQL-to-plan conversion. Only schema-qualified function references trigger
    /// async function resolution. The `name` must match the `SessionConfig`
    /// default catalog for the provider to be reachable — see
    /// [`SessionStateBuilder`] docs. Not wired at production call sites —
    /// see struct-level docs.
    pub fn with_func_catalog(
        mut self,
        name: impl Into<String>,
        provider: Arc<dyn FuncAsyncCatalogProvider>,
    ) -> Self {
        self.func_catalogs.insert(name.into(), provider);
        self
    }

    /// Sets the isolate pool for JavaScript UDF execution.
    ///
    /// Required before calling [`for_catalog`](Self::for_catalog). Panics at
    /// context construction time if not set.
    pub fn with_isolate_pool(mut self, pool: IsolatePool) -> Self {
        self.isolate_pool = Some(pool);
        self
    }

    /// Creates an [`ExecContext`] backed by a physical catalog.
    ///
    /// This is the async construction step that cannot be performed in the
    /// synchronous builder: it locks in the canonical chain via
    /// [`CatalogSnapshot::from_catalog`] and assembles the full execution
    /// context.
    ///
    /// Internally composes a [`SessionStateBuilder`] from the stored
    /// components (including the per-query tiered memory pool). The session
    /// builder and its internals are not exposed through the
    /// [`ExecContextBuilder`] API.
    pub async fn for_catalog(
        self,
        catalog: Catalog,
        ignore_canonical_segments: bool,
    ) -> Result<ExecContext, CreateContextError> {
        // Create the per-query tiered memory pool from global + per-query limit.
        let per_query_bytes = self.query_max_mem_mb * 1024 * 1024;
        let child_pool = make_memory_pool(MemoryPoolKind::Greedy, per_query_bytes);
        let tiered_memory_pool = Arc::new(TieredMemoryPool::new(
            self.global_memory_pool.clone(),
            child_pool,
        ));

        // Create the catalog snapshot (the async step that requires canonical
        // chain resolution from segment metadata).
        let (entries, tables, udfs) = catalog.into_parts();
        let physical_table =
            CatalogSnapshot::from_catalog(tables, udfs, &entries, ignore_canonical_segments)
                .await
                .map_err(CreateContextError::CatalogSnapshot)?;

        let query_snapshots = physical_table
            .table_snapshots()
            .map(|(s, sql_schema_name)| {
                QueryableSnapshot::from_snapshot(s, self.store.clone(), sql_schema_name.to_string())
                    .map(Arc::new)
                    .map_err(CreateContextError::MultiNetworkSegments)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let isolate_pool = self
            .isolate_pool
            .expect("IsolatePool is required — call .with_isolate_pool() before .for_catalog()");

        let env = ExecEnv {
            session_config: self.session_config.clone(),
            global_memory_pool: self.global_memory_pool.clone(),
            disk_manager: self.disk_manager.clone(),
            cache_manager: self.cache_manager.clone(),
            object_store_registry: self.object_store_registry.clone(),
            query_max_mem_mb: self.query_max_mem_mb,
            store: self.store,
            datasets_cache: self.datasets_cache,
            ethcall_udfs_cache: self.ethcall_udfs_cache,
        };

        // Compose a SessionStateBuilder from the stored components (including
        // the per-query tiered pool). All session creation in ExecContext goes
        // through `self.session_ctx.state()`.
        let mut session_builder = SessionStateBuilder::new(self.session_config)
            .with_scalar_udfs(builtin_udfs())
            .with_memory_pool(tiered_memory_pool.clone())
            .with_disk_manager(env.disk_manager.clone())
            .with_cache_manager(env.cache_manager.clone())
            .with_object_store_registry(env.object_store_registry.clone())
            .with_physical_optimizer_rule(create_instrumentation_rule());

        for (name, provider) in self.table_catalogs {
            session_builder = session_builder.with_table_catalog(name, provider);
        }
        for (name, provider) in self.func_catalogs {
            session_builder = session_builder.with_func_catalog(name, provider);
        }

        let session_ctx = SessionContext::new_with_state(session_builder.build());

        Ok(ExecContext {
            env,
            isolate_pool,
            physical_table,
            query_snapshots,
            tiered_memory_pool,
            session_ctx,
        })
    }
}

/// Failed to plan a SQL query in the query context
///
/// This error covers failures during `ExecContext::statement_to_plan()`.
#[derive(Debug, thiserror::Error)]
pub enum SqlError {
    /// Failed during SQL planning (pre-resolution or plan conversion)
    ///
    /// Covers async catalog pre-resolution failures and SQL-to-logical-plan
    /// conversion errors. Use [`is_user_input_error`](super::session::is_user_input_error)
    /// on the inner `DataFusionError` to classify user-input vs internal errors.
    #[error("failed to plan SQL query")]
    Planning(#[source] DataFusionError),

    /// Failed to create an exec session context
    ///
    /// This occurs when building a `SessionState` for query execution fails,
    /// typically due to a table registration error during state setup.
    #[error("failed to create exec session context")]
    RegisterTable(#[source] RegisterTableError),
}

/// Failed to convert a SQL statement into a validated logical plan.
///
/// Covers all failure modes during policy-enforcing SQL planning in
/// [`ExecContext::statement_to_plan`]: statement conversion, alias validation,
/// and read-only enforcement. Flattened to [`DataFusionError`] before wrapping
/// in [`SqlError::Planning`].
#[derive(Debug, thiserror::Error)]
enum StatementToPlanError {
    /// DataFusion failed to convert the SQL statement into a logical plan
    #[error("failed to convert SQL statement to logical plan: {0}")]
    StatementToPlan(#[source] DataFusionError),

    /// Query uses underscore-prefixed column aliases which are reserved
    #[error("query uses forbidden underscore-prefixed aliases")]
    ForbiddenAliases(#[source] DataFusionError),

    /// Query plan violates read-only constraints
    #[error("query plan violates read-only constraints")]
    ReadOnlyCheck(#[source] ReadOnlyCheckError),
}

impl StatementToPlanError {
    /// Converts into a [`DataFusionError`] with appropriate user-input tagging.
    fn into_datafusion_error(self) -> DataFusionError {
        match self {
            Self::StatementToPlan(err) => {
                err.context("failed to convert SQL statement to logical plan")
            }
            Self::ForbiddenAliases(err) => DataFusionError::Plan(format!(
                "query uses forbidden underscore-prefixed aliases: {err}"
            ))
            .context(INVALID_INPUT_CONTEXT),
            Self::ReadOnlyCheck(err) => {
                DataFusionError::Plan(format!("query plan violates read-only constraints: {err}"))
                    .context(INVALID_INPUT_CONTEXT)
            }
        }
    }
}

/// Errors that occur during inner `execute_plan` function
///
/// This error covers failures in read-only check, optimization,
/// physical plan creation, and stream execution.
#[derive(Debug, thiserror::Error)]
pub enum ExecuteError {
    /// Query plan violates read-only constraints
    #[error("query plan violates read-only constraints")]
    ReadOnlyCheck(#[source] ReadOnlyCheckError),

    /// Failed to optimize the logical plan
    #[error("failed to optimize logical plan")]
    Optimize(#[source] DataFusionError),

    /// Failed to create a physical execution plan
    #[error("failed to create physical execution plan")]
    CreatePhysicalPlan(#[source] DataFusionError),

    /// Output schema contains duplicate field names
    #[error("output schema contains duplicate field names")]
    DuplicateFieldNames(#[source] DataFusionError),

    /// Failed to start query execution stream
    #[error("failed to start query execution stream")]
    ExecuteStream(#[source] DataFusionError),

    /// Failed to collect explain results
    #[error("failed to collect explain results")]
    CollectExplainResults(#[source] DataFusionError),

    /// Failed to concatenate collected explain result batches
    ///
    /// This occurs when schema mismatch or allocation failure prevents
    /// concatenation of EXPLAIN output batches.
    #[error("failed to concatenate explain result batches")]
    ConcatExplainResults(#[source] DataFusionError),
}

/// Failed to execute a plan via `ExecContext::execute_plan`
///
/// This error wraps session context creation and inner execution errors.
#[derive(Debug, thiserror::Error)]
pub enum ExecutePlanError {
    /// Failed to create an exec session context
    #[error("failed to create exec session context")]
    RegisterTable(#[source] RegisterTableError),

    /// Failed during plan execution
    #[error("failed to execute plan")]
    Execute(#[source] ExecuteError),
}

/// Failed to execute a plan and concatenate results
///
/// This error covers `ExecContext::execute_and_concat()`.
#[derive(Debug, thiserror::Error)]
pub enum ExecuteAndConcatError {
    /// Failed to create an exec session context
    #[error("failed to create exec session context")]
    RegisterTable(#[source] RegisterTableError),

    /// Failed during plan execution
    #[error("failed to execute plan")]
    Execute(#[source] ExecuteError),

    /// Failed to collect result batches from the execution stream
    ///
    /// This occurs after planning completes successfully, during the actual
    /// execution when materializing record batches from the result stream.
    #[error("failed to collect query results")]
    CollectResults(#[source] DataFusionError),

    /// Failed to concatenate collected result batches into a single batch
    ///
    /// This occurs when schema mismatch or allocation failure prevents
    /// concatenation of the materialized record batches.
    #[error("failed to concatenate query result batches")]
    ConcatBatches(#[source] DataFusionError),
}

/// Referenced table does not exist in the catalog
///
/// This occurs when a query references a table that is not registered
/// in the current query context's catalog.
#[derive(Debug, thiserror::Error)]
#[error("table not found: {0}")]
pub struct TableNotFoundError(pub TableReference);

/// Failed to compute common block ranges across referenced tables
///
/// This error is shared by `common_ranges` and `max_end_blocks` because
/// `max_end_blocks` only delegates to `common_ranges`.
#[derive(Debug, thiserror::Error)]
pub enum CommonRangesError {
    /// Failed to extract table references from the logical plan
    #[error("failed to extract table references from plan")]
    ExtractTableReferences(#[source] DataFusionError),

    /// Failed to convert a DataFusion table reference to the project's format
    #[error("failed to convert table reference")]
    TableReferenceConversion(#[source] TableReferenceConversionError),

    /// Referenced table does not exist in the catalog
    #[error("table not found")]
    TableNotFound(#[source] TableNotFoundError),
}

/// Registers the tables and UDFs from a [`CatalogSnapshot`] into a [`SessionState`].
///
/// For each unique schema name a [`MemorySchemaProvider`] is created when the
/// schema does not already exist **or** when the existing schema is a read-only
/// provider (e.g. `ResolvedSchemaProvider` left by async pre-resolution).
/// Physical catalog tables are then registered into the writable schema.
fn register_catalog(
    env: &ExecEnv,
    state: &mut SessionState,
    catalog: &CatalogSnapshot,
    query_snapshots: &[Arc<QueryableSnapshot>],
) -> Result<(), RegisterTableError> {
    let default_catalog_name = state.config().options().catalog.default_catalog.clone();
    let default_catalog = state
        .catalog_list()
        .catalog(&default_catalog_name)
        .ok_or_else(|| RegisterTableError::MissingDefaultCatalog {
            catalog: default_catalog_name.clone(),
        })?;

    // Always register fresh schemas to ensure idempotent table registration.
    // The catalog's `CatalogProviderList` is `Arc`-shared across `DfSessionState`
    // clones, so schemas from a prior planning phase (`register_logical_catalog`)
    // may persist with their planning-only tables. A fresh `MemorySchemaProvider`
    // avoids "table already exists" errors from DF 52's strict duplicate rejection.
    {
        let schema_names = query_snapshots
            .iter()
            .map(|t| t.sql_schema_name())
            .collect::<BTreeSet<_>>();
        for schema_name in schema_names {
            let schema = Arc::new(MemorySchemaProvider::new());
            default_catalog
                .register_schema(schema_name, schema)
                .map_err(|source| RegisterTableError::RegisterSchema {
                    catalog: default_catalog_name.clone(),
                    schema: schema_name.to_string(),
                    source,
                })?;
        }
    }

    for table in query_snapshots {
        let table_ref = table.table_ref();

        // This may overwrite a previously registered store, but that should not make a difference.
        // The only segment of the `table.url()` that matters here is the schema and bucket name.
        state.runtime_env().register_object_store(
            table.physical_table().url(),
            env.store.as_datafusion_object_store().clone(),
        );

        let table_name = table_ref.table().to_string();
        let table_ref: datafusion::common::TableReference = table_ref.into();
        let schema_provider = state
            .schema_for_ref(table_ref)
            .map_err(RegisterTableError::RegisterTable)?;

        schema_provider
            .register_table(table_name, table.clone())
            .map_err(RegisterTableError::RegisterTable)?;
    }

    // Register catalog UDFs, skipping planning-only JS UDFs.
    //
    // Catalog UDFs may include `PlanJsUdf`-backed entries that are
    // non-executable (they panic on invoke). The execution-boundary JS UDF
    // attach (`ExecContext::attach`) rewrites inline plan references to
    // `ExecJsUdf`, so session-level registration of planning JS UDFs is
    // unnecessary and would re-introduce panic-guarded objects into the
    // execution session.
    for udf in catalog.udfs() {
        let is_plan_js_udf = udf.inner().as_any().downcast_ref::<PlanJsUdf>().is_some();
        if is_plan_js_udf {
            continue;
        }
        state
            .register_udf(Arc::new(udf.clone()))
            .map_err(RegisterTableError::RegisterUdf)?;
    }

    Ok(())
}

/// Failed to register catalog content with the exec session context
#[derive(Debug, thiserror::Error)]
pub enum RegisterTableError {
    /// The configured default catalog is missing from the DataFusion session
    #[error("default catalog '{catalog}' is not registered in exec session context")]
    MissingDefaultCatalog { catalog: String },

    /// Failed to create a schema in the configured default catalog
    #[error("failed to register schema '{schema}' in default catalog '{catalog}'")]
    RegisterSchema {
        catalog: String,
        schema: String,
        #[source]
        source: DataFusionError,
    },

    /// Failed to register a dataset table in the exec session context
    #[error("failed to register dataset table with exec session context")]
    RegisterTable(#[source] DataFusionError),

    /// Failed to register a catalog UDF in the exec session context
    #[error("failed to register catalog UDF in exec session context")]
    RegisterUdf(#[source] DataFusionError),
}

/// Replaces `PlanTable` table sources with actual `QueryableSnapshot` providers
/// in a single plan node.
fn attach_table_node(
    mut node: LogicalPlan,
    ctx: &ExecContext,
) -> Result<Transformed<LogicalPlan>, DataFusionError> {
    match &mut node {
        LogicalPlan::TableScan(TableScan {
            table_name, source, ..
        }) if source.table_type() == TableType::Base && source.get_logical_plan().is_none() => {
            let table_ref: TableReference<String> = table_name
                .clone()
                .try_into()
                .map_err(|err| DataFusionError::External(Box::new(err)))?;
            let provider = ctx
                .get_table(&table_ref)
                .map_err(|err| DataFusionError::External(Box::new(err)))?;
            *source = Arc::new(DefaultTableSource::new(provider));
            Ok(Transformed::yes(node))
        }
        _ => Ok(Transformed::no(node)),
    }
}

/// Rewrites all [`PlanJsUdf`]-backed UDF references in a single plan node's
/// expressions to execution-ready UDF references.
///
/// Each unique `PlanJsUdf` (keyed by `name()`) is attached exactly once;
/// subsequent occurrences reuse the same `Arc<ScalarUDF>`.
fn attach_js_udf_exprs(
    node: LogicalPlan,
    pool: &IsolatePool,
    cache: &mut HashMap<String, Arc<ScalarUDF>>,
) -> Result<Transformed<LogicalPlan>, DataFusionError> {
    node.map_expressions(|expr| expr.transform(|e| rewrite_single_expr(e, pool, cache)))
}

/// Rewrites a single `Expr` node if it is a `PlanJsUdf`-backed scalar function.
fn rewrite_single_expr(
    expr: Expr,
    pool: &IsolatePool,
    cache: &mut HashMap<String, Arc<ScalarUDF>>,
) -> Result<Transformed<Expr>, DataFusionError> {
    let Expr::ScalarFunction(ref sf) = expr else {
        return Ok(Transformed::no(expr));
    };

    let Some(plan_udf) = sf.func.inner().as_any().downcast_ref::<PlanJsUdf>() else {
        return Ok(Transformed::no(expr));
    };

    let name = sf.func.name().to_string();

    let exec_udf = if let Some(cached) = cache.get(&name) {
        cached.clone()
    } else {
        let exec = plan_udf.attach(pool.clone());
        let udf = Arc::new(exec.into_scalar_udf());
        cache.insert(name, udf.clone());
        udf
    };

    Ok(Transformed::yes(Expr::ScalarFunction(ScalarFunction {
        func: exec_udf,
        args: sf.args.clone(),
    })))
}

/// `logical_optimize` controls whether logical optimizations should be applied to `plan`.
#[tracing::instrument(skip_all, err)]
async fn execute_plan(
    state: &SessionState,
    mut plan: LogicalPlan,
    logical_optimize: bool,
) -> Result<SendableRecordBatchStream, ExecuteError> {
    read_only_check(&plan).map_err(ExecuteError::ReadOnlyCheck)?;

    tracing::debug!(logical_plan = %plan.to_string().replace('\n', "\\n"), "planned SQL");

    if logical_optimize {
        plan = state.optimize(&plan).map_err(ExecuteError::Optimize)?;
    }

    let is_explain = matches!(plan, LogicalPlan::Explain(_) | LogicalPlan::Analyze(_));

    let physical_plan = state
        .create_physical_plan(&plan)
        .await
        .map_err(ExecuteError::CreatePhysicalPlan)?;

    forbid_duplicate_field_names(&physical_plan, &plan)
        .map_err(ExecuteError::DuplicateFieldNames)?;
    tracing::debug!(physical_plan = %print_physical_plan(&*physical_plan), "optimized plan");

    let task_ctx = state.task_ctx();
    let stream = if is_explain {
        execute_explain(physical_plan, task_ctx).await?
    } else {
        execute_stream(physical_plan, task_ctx).map_err(ExecuteError::ExecuteStream)?
    };

    let span = tracing::Span::current();
    let schema = stream.schema();
    Ok(Box::pin(RecordBatchStreamAdapter::new(
        schema,
        stream.instrument(span),
    )))
}

// We do special handling for `Explain` plans to ensure that the output is sanitized from full paths.
async fn execute_explain(
    physical_plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
) -> Result<SendableRecordBatchStream, ExecuteError> {
    use datafusion::physical_plan::execution_plan;

    let schema = physical_plan.schema().clone();
    let output = execution_plan::collect(physical_plan, task_ctx)
        .await
        .map_err(ExecuteError::CollectExplainResults)?;

    let concatenated = concat_batches(&schema, &output)
        .map_err(|err| ExecuteError::ConcatExplainResults(err.into()))?;
    let sanitized = sanitize_explain(concatenated);

    let stream =
        RecordBatchStreamAdapter::new(schema, stream::iter(std::iter::once(Ok(sanitized))));
    Ok(Box::pin(stream))
}

// Regex to match full paths to .parquet files and capture just the filename
// This handles paths with forward or backward slashes
static PARQUET_PATH_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?:[^\s\[,]+[/\\])?([^\s\[,/\\]+\.parquet)").unwrap());

/// Sanitizes a string by replacing full paths to .parquet files with just the filename
fn sanitize_parquet_paths(text: &str) -> String {
    PARQUET_PATH_REGEX.replace_all(text, "$1").into_owned()
}

// Sanitize the explain output by removing full paths and keeping only the filenames.
//
// Uses best-effort passthrough: if the expected "plan" column is absent, is not a
// StringArray, or if constructing the output batch fails (e.g., on a future DataFusion
// EXPLAIN schema change), the original batch is returned unchanged rather than panicking.
fn sanitize_explain(batch: RecordBatch) -> RecordBatch {
    use arrow::array::StringArray;

    let Ok(plan_idx) = batch.schema().index_of("plan") else {
        return batch;
    };
    let Some(plan_column) = batch
        .column(plan_idx)
        .as_any()
        .downcast_ref::<StringArray>()
    else {
        return batch;
    };

    let transformed: StringArray = plan_column
        .iter()
        .map(|value| value.map(sanitize_parquet_paths))
        .collect();

    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns[plan_idx] = Arc::new(transformed);

    RecordBatch::try_new(batch.schema(), columns).unwrap_or(batch)
}

/// Prints the physical plan to a single line, for logging.
fn print_physical_plan(plan: &dyn ExecutionPlan) -> String {
    let plan_str = displayable(plan)
        .indent(false)
        .to_string()
        .replace('\n', "\\n");
    sanitize_parquet_paths(&plan_str)
}

/// Creates an instrumentation rule that captures metrics and provides previews of data during execution.
pub fn create_instrumentation_rule() -> Arc<dyn PhysicalOptimizerRule + Send + Sync> {
    let options_builder = InstrumentationOptions::builder()
        .record_metrics(true)
        .preview_limit(5)
        .preview_fn(Arc::new(|batch: &RecordBatch| {
            pretty_format_compact_batch(batch, 64, 3, 10).map(|fmt| fmt.to_string())
        }));

    instrument_with_info_spans!(
        options: options_builder.build(),
        env = field::Empty,
        region = field::Empty,
    )
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{Array, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::{
        common::tree_node::{TreeNode, TreeNodeRecursion},
        logical_expr::{LogicalPlanBuilder, ScalarUDF, ScalarUDFImpl, expr::ScalarFunction},
        prelude::Expr,
    };
    use datasets_derived::function::Function;
    use js_runtime::isolate_pool::IsolatePool;

    use super::*;

    mod sanitize_explain_tests {
        use super::*;

        #[test]
        fn sanitize_explain_with_no_plan_column_returns_batch_unchanged() {
            //* Given
            let schema = Arc::new(Schema::new(vec![Field::new("type", DataType::Utf8, false)]));
            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["physical"]))])
                    .expect("test batch should be constructible");

            //* When
            let result = sanitize_explain(batch.clone());

            //* Then
            assert_eq!(
                result.num_rows(),
                batch.num_rows(),
                "row count should be preserved when plan column is absent"
            );
            assert_eq!(
                result.schema(),
                batch.schema(),
                "schema should be unchanged when plan column is absent"
            );
        }

        #[test]
        fn sanitize_explain_with_non_string_plan_column_returns_batch_unchanged() {
            //* Given
            let schema = Arc::new(Schema::new(vec![Field::new(
                "plan",
                DataType::Int64,
                false,
            )]));
            let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![42]))])
                .expect("test batch should be constructible");

            //* When
            let result = sanitize_explain(batch.clone());

            //* Then
            assert_eq!(
                result.num_rows(),
                batch.num_rows(),
                "row count should be preserved when plan column is not StringArray"
            );
            assert_eq!(
                result.schema(),
                batch.schema(),
                "schema should be unchanged when plan column is not StringArray"
            );
        }

        #[test]
        fn sanitize_explain_with_empty_batch_returns_empty_batch() {
            //* Given
            let schema = Arc::new(Schema::new(vec![Field::new("plan", DataType::Utf8, false)]));
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(StringArray::from(Vec::<&str>::new()))],
            )
            .expect("empty test batch should be constructible");

            //* When
            let result = sanitize_explain(batch);

            //* Then
            assert_eq!(result.num_rows(), 0, "empty batch should remain empty");
        }

        #[test]
        fn sanitize_explain_with_plan_column_sanitizes_parquet_paths() {
            //* Given
            let schema = Arc::new(Schema::new(vec![Field::new("plan", DataType::Utf8, false)]));
            let plan_text = "ParquetExec: file_groups={1 group: [/data/store/subdir/file.parquet]}";
            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![plan_text]))])
                    .expect("test batch should be constructible");

            //* When
            let result = sanitize_explain(batch);

            //* Then
            let plan_col = result
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("plan column should remain StringArray after sanitization");
            let sanitized = plan_col.value(0);
            assert!(
                !sanitized.contains("/data/store/subdir/"),
                "full directory path should be removed from explain output: {sanitized}"
            );
            assert!(
                sanitized.contains("file.parquet"),
                "parquet filename should be preserved: {sanitized}"
            );
        }

        #[test]
        fn sanitize_explain_with_multiple_columns_preserves_non_plan_columns() {
            //* Given
            let schema = Arc::new(Schema::new(vec![
                Field::new("type", DataType::Utf8, false),
                Field::new("plan", DataType::Utf8, false),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(vec!["physical"])),
                    Arc::new(StringArray::from(vec!["Scan: /long/path/to/data.parquet"])),
                ],
            )
            .expect("multi-column test batch should be constructible");

            //* When
            let result = sanitize_explain(batch);

            //* Then
            let type_col = result
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("type column should remain StringArray");
            assert_eq!(
                type_col.value(0),
                "physical",
                "non-plan columns should be unchanged"
            );

            let plan_col = result
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("plan column should remain StringArray");
            assert!(
                plan_col.value(0).contains("data.parquet"),
                "parquet filename should be preserved in plan column"
            );
        }

        #[test]
        fn sanitize_explain_with_null_values_in_plan_column_preserves_nulls() {
            //* Given
            let schema = Arc::new(Schema::new(vec![Field::new("plan", DataType::Utf8, true)]));
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(StringArray::from(vec![
                    Some("Scan: /path/to/data.parquet"),
                    None,
                    Some("Filter: col > 0"),
                ]))],
            )
            .expect("test batch with nulls should be constructible");

            //* When
            let result = sanitize_explain(batch);

            //* Then
            let plan_col = result
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("plan column should remain StringArray");
            assert_eq!(result.num_rows(), 3, "all rows should be preserved");
            assert!(
                plan_col.value(0).contains("data.parquet"),
                "non-null parquet path should be sanitized"
            );
            assert!(plan_col.is_null(1), "null values should be preserved");
            assert_eq!(
                plan_col.value(2),
                "Filter: col > 0",
                "rows without parquet paths should be unchanged"
            );
        }
    }

    mod sanitize_parquet_paths_tests {
        use super::*;

        #[test]
        fn sanitize_parquet_paths_with_full_path_returns_filename_only() {
            //* When
            let result = sanitize_parquet_paths("/data/store/subdir/file.parquet");

            //* Then
            assert_eq!(
                result, "file.parquet",
                "full path should be replaced with filename only"
            );
        }

        #[test]
        fn sanitize_parquet_paths_with_filename_only_returns_unchanged() {
            //* When
            let result = sanitize_parquet_paths("file.parquet");

            //* Then
            assert_eq!(result, "file.parquet", "bare filename should be unchanged");
        }

        #[test]
        fn sanitize_parquet_paths_with_no_parquet_returns_unchanged() {
            //* Given
            let text = "Filter: column > 0";

            //* When
            let result = sanitize_parquet_paths(text);

            //* Then
            assert_eq!(
                result, text,
                "text without parquet paths should be unchanged"
            );
        }

        #[test]
        fn sanitize_parquet_paths_with_multiple_paths_sanitizes_all() {
            //* Given
            let text = "files: [/a/b/one.parquet, /c/d/two.parquet]";

            //* When
            let result = sanitize_parquet_paths(text);

            //* Then
            assert!(
                result.contains("one.parquet"),
                "first filename should be preserved: {result}"
            );
            assert!(
                result.contains("two.parquet"),
                "second filename should be preserved: {result}"
            );
            assert!(
                !result.contains("/a/b/"),
                "first directory path should be removed: {result}"
            );
            assert!(
                !result.contains("/c/d/"),
                "second directory path should be removed: {result}"
            );
        }

        #[test]
        fn sanitize_parquet_paths_with_empty_string_returns_empty() {
            //* When
            let result = sanitize_parquet_paths("");

            //* Then
            assert_eq!(result, "", "empty string should remain empty");
        }
    }

    #[test]
    fn attach_js_udfs_single_plan_udf_rewrites_to_exec() {
        //* Given
        let (_, expr) = plan_udf_expr("my_func");
        let plan = LogicalPlanBuilder::empty(false)
            .project(vec![expr])
            .expect("project should succeed")
            .build()
            .expect("build should succeed");

        //* When
        let result = attach_js_udfs(plan, IsolatePool::new()).expect("attach should succeed");

        //* Then
        assert_no_plan_js_udfs(&result);
    }

    #[test]
    fn attach_js_udfs_duplicate_udf_refs_deduplicates() {
        //* Given
        let (udf, _) = plan_udf_expr("my_func");
        let plan = LogicalPlanBuilder::empty(false)
            .project(vec![
                Expr::ScalarFunction(ScalarFunction::new_udf(
                    udf.clone(),
                    vec![datafusion::prelude::lit("a")],
                )),
                Expr::ScalarFunction(ScalarFunction::new_udf(
                    udf,
                    vec![datafusion::prelude::lit("b")],
                )),
            ])
            .expect("project should succeed")
            .build()
            .expect("build should succeed");

        //* When
        let result = attach_js_udfs(plan, IsolatePool::new()).expect("attach should succeed");

        //* Then
        assert_no_plan_js_udfs(&result);
    }

    #[test]
    fn attach_js_udfs_no_js_udfs_passes_through() {
        //* Given
        let plan = LogicalPlanBuilder::empty(false)
            .project(vec![datafusion::prelude::lit(1)])
            .expect("project should succeed")
            .build()
            .expect("build should succeed");

        //* When
        let result = attach_js_udfs(plan, IsolatePool::new());

        //* Then
        assert!(
            result.is_ok(),
            "attach should succeed for plan without JS UDFs"
        );
    }

    #[test]
    fn attach_js_udfs_schema_qualified_udf_preserves_name() {
        //* Given
        let function = sample_function();
        let plan_udf = PlanJsUdf::from_function("my_func", &function, Some("ns/dataset@1.0"));
        let expected_name = plan_udf.name().to_string();
        let udf = Arc::new(ScalarUDF::new_from_impl(plan_udf));
        let expr = Expr::ScalarFunction(ScalarFunction::new_udf(
            udf,
            vec![datafusion::prelude::lit("test")],
        ));
        let plan = LogicalPlanBuilder::empty(false)
            .project(vec![expr])
            .expect("project should succeed")
            .build()
            .expect("build should succeed");

        //* When
        let result = attach_js_udfs(plan, IsolatePool::new()).expect("attach should succeed");

        //* Then
        assert_no_plan_js_udfs(&result);
        // Verify the rewritten UDF preserves the schema-qualified name.
        result
            .apply(|node| {
                for expr in node.expressions() {
                    expr.apply(|e| {
                        if let Expr::ScalarFunction(sf) = e {
                            assert_eq!(
                                sf.func.name(),
                                expected_name,
                                "attached UDF should preserve schema-qualified name"
                            );
                        }
                        Ok(TreeNodeRecursion::Continue)
                    })
                    .expect("expression traversal should succeed");
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .expect("plan traversal should succeed");
    }

    #[test]
    fn attach_js_udfs_multiple_distinct_udfs_rewrites_all() {
        //* Given
        let (_, expr_a) = plan_udf_expr("func_a");
        let (_, expr_b) = plan_udf_expr("func_b");
        let plan = LogicalPlanBuilder::empty(false)
            .project(vec![expr_a, expr_b])
            .expect("project should succeed")
            .build()
            .expect("build should succeed");

        //* When
        let result = attach_js_udfs(plan, IsolatePool::new()).expect("attach should succeed");

        //* Then
        assert_no_plan_js_udfs(&result);
    }

    #[test]
    fn attach_js_udfs_nested_case_expression_rewrites_inner_udf() {
        //* Given
        let (_, udf_expr) = plan_udf_expr("my_func");
        // Wrap the JS UDF call inside a CASE expression.
        let nested = Expr::Case(datafusion::logical_expr::expr::Case {
            expr: None,
            when_then_expr: vec![(Box::new(udf_expr), Box::new(datafusion::prelude::lit(1)))],
            else_expr: Some(Box::new(datafusion::prelude::lit(0))),
        });
        let plan = LogicalPlanBuilder::empty(false)
            .project(vec![nested])
            .expect("project should succeed")
            .build()
            .expect("build should succeed");

        //* When
        let result = attach_js_udfs(plan, IsolatePool::new()).expect("attach should succeed");

        //* Then
        assert_no_plan_js_udfs(&result);
    }

    #[test]
    fn attach_js_udfs_mixed_js_and_non_js_udfs_rewrites_only_js() {
        //* Given
        let (_, js_expr) = plan_udf_expr("js_func");
        // A non-JS built-in UDF expression (abs).
        let non_js_expr = datafusion::prelude::abs(datafusion::prelude::lit(42));
        let plan = LogicalPlanBuilder::empty(false)
            .project(vec![js_expr, non_js_expr])
            .expect("project should succeed")
            .build()
            .expect("build should succeed");

        //* When
        let result = attach_js_udfs(plan, IsolatePool::new()).expect("attach should succeed");

        //* Then
        assert_no_plan_js_udfs(&result);
        // Verify the non-JS UDF (abs) is still present and unchanged.
        let mut found_abs = false;
        result
            .apply(|node| {
                for expr in node.expressions() {
                    expr.apply(|e| {
                        if let Expr::ScalarFunction(sf) = e
                            && sf.func.name() == "abs"
                        {
                            found_abs = true;
                        }
                        Ok(TreeNodeRecursion::Continue)
                    })
                    .expect("expression traversal should succeed");
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .expect("plan traversal should succeed");
        assert!(found_abs, "non-JS UDF (abs) should be preserved in plan");
    }

    /// Test helper: wraps the node-level `attach_js_udf_exprs` in a full plan
    /// traversal, mirroring the plan-level attach in `ExecContext::attach`.
    fn attach_js_udfs(
        plan: LogicalPlan,
        pool: IsolatePool,
    ) -> Result<LogicalPlan, DataFusionError> {
        let mut cache: HashMap<String, Arc<ScalarUDF>> = HashMap::new();
        plan.transform_with_subqueries(|node| attach_js_udf_exprs(node, &pool, &mut cache))
            .map(|t| t.data)
    }

    /// Builds a minimal [`Function`] with a single `Utf8 -> Boolean` signature.
    fn sample_function() -> Function {
        serde_json::from_value(serde_json::json!({
            "inputTypes": ["Utf8"],
            "outputType": "Boolean",
            "source": {
                "source": "function f(a) { return true; }",
                "filename": "test.js"
            }
        }))
        .expect("test function should deserialize")
    }

    /// Creates a [`PlanJsUdf`]-backed `ScalarUDF` and a matching `Expr` for
    /// use in test plans.
    fn plan_udf_expr(name: &str) -> (Arc<ScalarUDF>, Expr) {
        let function = sample_function();
        let plan_udf = PlanJsUdf::from_function(name, &function, None);
        let udf = Arc::new(ScalarUDF::new_from_impl(plan_udf));
        let expr = Expr::ScalarFunction(ScalarFunction::new_udf(
            udf.clone(),
            vec![datafusion::prelude::lit("test")],
        ));
        (udf, expr)
    }

    /// Asserts no `PlanJsUdf` references remain anywhere in the plan.
    fn assert_no_plan_js_udfs(plan: &LogicalPlan) {
        plan.apply(|node| {
            node.expressions().iter().for_each(|expr| {
                expr.apply(|e| {
                    if let Expr::ScalarFunction(sf) = e {
                        assert!(
                            sf.func
                                .inner()
                                .as_any()
                                .downcast_ref::<PlanJsUdf>()
                                .is_none(),
                            "found PlanJsUdf in rewritten plan"
                        );
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
                .expect("expression traversal should succeed");
            });
            Ok(TreeNodeRecursion::Continue)
        })
        .expect("plan traversal should succeed");
    }
}
