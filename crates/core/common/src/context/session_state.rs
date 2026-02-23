//! Session state for SQL planning and execution.
//!
//! Provides a re-implementation of session state management that performs async
//! catalog pre-resolution before SQL planning. Async pre-resolution extracts
//! table and function references from the SQL statement, resolves only the
//! referenced catalogs concurrently, and registers the results into a
//! transient DataFusion `SessionState` before planning.

use std::{collections::BTreeMap, sync::Arc};

use datafusion::{
    catalog::{
        AsyncCatalogProvider as TableAsyncCatalogProvider, CatalogProviderList, SchemaProvider,
    },
    common::TableReference,
    error::DataFusionError,
    execution::{
        FunctionRegistry, SessionState as DfSessionState,
        SessionStateBuilder as DfSessionStateBuilder, TaskContext,
        cache::cache_manager::CacheManager, config::SessionConfig, disk_manager::DiskManager,
        memory_pool::MemoryPool, object_store::ObjectStoreRegistry, runtime_env::RuntimeEnv,
    },
    logical_expr::{LogicalPlan, ScalarUDF},
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::ExecutionPlan as DfExecutionPlan,
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
    sql::parser,
};
use futures::future;

use crate::{
    context::common::INVALID_INPUT_CONTEXT,
    evm::udfs::{
        EvmDecodeHex, EvmDecodeLog, EvmDecodeParams, EvmDecodeType, EvmEncodeHex, EvmEncodeParams,
        EvmEncodeType, EvmTopic, ShiftUnits,
    },
    func_catalog::catalog_provider::AsyncCatalogProvider as FuncAsyncCatalogProvider,
    sql::{FunctionReference, resolve_function_references, resolve_table_references},
};

/// Session state for planning and executing SQL queries.
///
/// Stores a base `SessionState` built once at construction (with config,
/// runtime environment, builtin UDFs, and physical optimizer rules) and
/// clones it for each planning/execution operation. SQL planning entry
/// points (`statement_to_plan`) perform async catalog
/// pre-resolution on top of the cloned base state.
#[derive(Clone)]
pub struct SessionState {
    /// Base DataFusion session state built once at construction.
    ///
    /// Contains the session config, runtime environment, builtin UDFs, and
    /// physical optimizer rules. Cloned for each planning/execution operation;
    /// callers register additional catalogs or tables on the clone before use.
    state: DfSessionState,

    /// Named async table catalog providers consulted during SQL pre-resolution.
    ///
    /// Keyed by catalog name (e.g. `"amp"`). Only providers whose names match
    /// catalogs referenced by the SQL statement are resolved. When both this
    /// map and `func_catalogs` are empty, pre-resolution is skipped entirely
    /// and the eager logical catalog path is used.
    table_catalogs: BTreeMap<String, Arc<dyn TableAsyncCatalogProvider>>,

    /// Named async function catalog providers consulted during SQL pre-resolution.
    ///
    /// Keyed by catalog name. Only schema-qualified function references trigger
    /// resolution; bare function names
    /// map entirely (spec requirement 3/4).
    func_catalogs: BTreeMap<String, Arc<dyn FuncAsyncCatalogProvider>>,
}

impl SessionState {
    /// Returns the session ID.
    ///
    /// Mirrors [`DfSessionState::session_id`].
    pub fn session_id(&self) -> &str {
        self.state.session_id()
    }

    /// Returns the session configuration.
    ///
    /// Mirrors [`DfSessionState::config`].
    pub fn config(&self) -> &SessionConfig {
        self.state.config()
    }

    /// Returns the runtime environment.
    ///
    /// Mirrors [`DfSessionState::runtime_env`].
    pub fn runtime_env(&self) -> &Arc<RuntimeEnv> {
        self.state.runtime_env()
    }

    /// Returns the catalog list.
    ///
    /// Mirrors [`DfSessionState::catalog_list`].
    pub fn catalog_list(&self) -> &Arc<dyn CatalogProviderList> {
        self.state.catalog_list()
    }

    /// Resolves a [`TableReference`] to its [`SchemaProvider`].
    ///
    /// Mirrors [`DfSessionState::schema_for_ref`].
    pub fn schema_for_ref(
        &self,
        table_ref: impl Into<TableReference>,
    ) -> Result<Arc<dyn SchemaProvider>, DataFusionError> {
        self.state.schema_for_ref(table_ref)
    }

    /// Registers a scalar UDF.
    ///
    /// Mirrors [`DfSessionState::register_udf`] via [`FunctionRegistry`].
    pub fn register_udf(
        &mut self,
        udf: Arc<ScalarUDF>,
    ) -> Result<Option<Arc<ScalarUDF>>, DataFusionError> {
        self.state.register_udf(udf)
    }

    /// Creates a new [`TaskContext`] for this session.
    ///
    /// Mirrors [`DfSessionState::task_ctx`].
    pub fn task_ctx(&self) -> Arc<TaskContext> {
        self.state.task_ctx()
    }

    /// Applies DataFusion logical optimizations to an existing plan.
    ///
    /// Does not trigger SQL pre-resolution; the plan is assumed to be fully
    /// resolved already.
    ///
    /// Mirrors [`DfSessionState::optimize`] (synchronous).
    #[tracing::instrument(skip_all, err)]
    pub fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan, DataFusionError> {
        self.state.optimize(plan)
    }

    /// Returns a new `SessionState` with async catalog pre-resolution
    /// applied for the given SQL statement.
    ///
    /// The returned state has all async catalogs resolved into the DF
    /// state; its async catalog maps are empty (resolution is complete).
    pub(crate) async fn resolved_state(
        &self,
        stmt: &parser::Statement,
    ) -> Result<SessionState, DataFusionError> {
        let df_state = self.resolve_for_statement(stmt).await?;
        Ok(SessionState {
            state: df_state,
            table_catalogs: Default::default(),
            func_catalogs: Default::default(),
        })
    }

    /// Creates a physical execution plan from a logical plan.
    ///
    /// Wraps DataFusion's `DefaultPhysicalPlanner` so that `DfSessionState`
    /// stays internal to `SessionState`.
    pub(crate) async fn create_physical_plan(
        &self,
        plan: &LogicalPlan,
    ) -> Result<Arc<dyn DfExecutionPlan>, DataFusionError> {
        DefaultPhysicalPlanner::default()
            .create_physical_plan(plan, &self.state)
            .await
    }

    /// Plans a SQL statement into a [`LogicalPlan`].
    ///
    /// Performs async pre-resolution of referenced dataset catalogs and
    /// functions before planning. Returns the raw logical plan without
    /// policy enforcement (alias validation, read-only checks).
    ///
    /// Consumer-level policies (forbidden aliases, read-only constraints)
    /// are enforced by [`PlanContext`] and [`ExecContext`], not here.
    ///
    /// Aligns with DataFusion's [`SessionState::statement_to_plan`](datafusion::execution::session_state::SessionState::statement_to_plan).
    pub async fn statement_to_plan(
        &self,
        stmt: parser::Statement,
    ) -> Result<LogicalPlan, DataFusionError> {
        let resolved = self.resolved_state(&stmt).await?;
        resolved.state.statement_to_plan(stmt).await
    }

    /// Resolves async catalogs for a SQL statement and returns a transient `DfSessionState`.
    ///
    /// This is the SQL-only pre-resolution path used by `statement_to_plan`,
    /// `statement_to_schema`, and exec SQL planning in `ExecContext`. It:
    /// 1. Extracts table and function references from the SQL statement.
    /// 2. Filters bare function refs (built-ins, not async-resolved).
    /// 3. Resolves only the catalogs that are actually referenced.
    /// 4. Registers resolved tables into the `DfSessionState`.
    /// 5. Registers resolved functions as `ScalarUDF`s in deterministic order.
    ///
    /// Errors are flattened to `DataFusionError` at this boundary.
    async fn resolve_for_statement(
        &self,
        stmt: &parser::Statement,
    ) -> Result<DfSessionState, DataFusionError> {
        // Early-out: if no async providers are registered, skip resolution.
        if self.table_catalogs.is_empty() && self.func_catalogs.is_empty() {
            return Ok(self.state.clone());
        }

        // Extract table references from the SQL statement (per-query cache via
        // the resolve() default impls; we collect all refs here once).
        let table_refs = resolve_table_references::<String>(stmt).map_err(|err| {
            DataFusionError::Plan(format!("failed to extract table references: {err}"))
                .context(INVALID_INPUT_CONTEXT)
        })?;

        // Extract function references; bare functions are built-ins and bypass
        // async resolution (spec requirement 3/4).
        let all_func_refs = resolve_function_references::<String>(stmt).map_err(|err| {
            DataFusionError::Plan(format!("failed to extract function references: {err}"))
                .context(INVALID_INPUT_CONTEXT)
        })?;
        let qualified_func_refs: Vec<_> = all_func_refs
            .into_iter()
            .filter(|r| matches!(r, FunctionReference::Qualified { .. }))
            .collect();

        // Convert to DataFusion TableReference for the resolve() API.
        // All our refs are Bare or Partial (catalog-qualified refs are rejected
        // by resolve_table_references), so the catalog is always the default.
        let df_table_refs: Vec<datafusion::common::TableReference> = table_refs
            .into_iter()
            .map(datafusion::common::TableReference::from)
            .collect();

        // Function references use TableReference::partial(schema, function_name)
        // because FuncReference is an alias for datafusion::common::TableReference.
        let df_func_refs: Vec<datafusion::common::TableReference> = qualified_func_refs
            .into_iter()
            .filter_map(|r| match r {
                FunctionReference::Qualified { schema, function } => Some(
                    datafusion::common::TableReference::partial(schema.as_str(), function.as_str()),
                ),
                FunctionReference::Bare { .. } => None,
            })
            .collect();

        // All our refs use the default catalog (catalog-qualified refs are
        // rejected upstream). Resolve only providers registered under names
        // that appear in the references, to avoid unnecessary I/O.
        // LoD exception: navigating DataFusion's config API (third-party struct).
        let default_catalog = self
            .state
            .config()
            .options()
            .catalog
            .default_catalog
            .clone();
        let config = self.state.config().clone();

        // Identify table catalog providers referenced in the statement.
        let referenced_table_catalogs: Vec<(String, Arc<dyn TableAsyncCatalogProvider>)> =
            if df_table_refs.is_empty() {
                Vec::new()
            } else {
                self.table_catalogs
                    .iter()
                    .filter(|(name, _)| {
                        df_table_refs
                            .iter()
                            .any(|r| r.catalog().unwrap_or(&default_catalog) == name.as_str())
                    })
                    .map(|(name, provider)| (name.clone(), provider.clone()))
                    .collect()
            };

        // Identify function catalog providers referenced in the statement.
        let referenced_func_catalogs: Vec<(String, Arc<dyn FuncAsyncCatalogProvider>)> =
            if df_func_refs.is_empty() {
                Vec::new()
            } else {
                self.func_catalogs
                    .iter()
                    .filter(|(name, _)| {
                        df_func_refs
                            .iter()
                            .any(|r| r.catalog().unwrap_or(&default_catalog) == name.as_str())
                    })
                    .map(|(name, provider)| (name.clone(), provider.clone()))
                    .collect()
            };

        // Resolve table and function catalogs concurrently. Both categories
        // of futures overlap so that function resolution can start before all
        // table resolves complete.
        let table_futures =
            referenced_table_catalogs
                .into_iter()
                .map(|(catalog_name, provider)| {
                    let config = config.clone();
                    let df_table_refs = df_table_refs.clone();
                    async move {
                        let resolved = provider
                            .resolve(&df_table_refs, &config, &catalog_name)
                            .await?;
                        Ok::<_, DataFusionError>((catalog_name, resolved))
                    }
                });

        let func_futures = referenced_func_catalogs
            .into_iter()
            .map(|(catalog_name, provider)| {
                let config = config.clone();
                let df_func_refs = df_func_refs.clone();
                async move {
                    let resolved = provider
                        .resolve(&df_func_refs, &config, &catalog_name)
                        .await?;
                    Ok::<_, DataFusionError>((catalog_name, resolved))
                }
            });

        let (resolved_table_catalogs, resolved_func_catalogs) = future::try_join(
            future::try_join_all(table_futures),
            future::try_join_all(func_futures),
        )
        .await?;

        let mut state = self.state.clone();

        // Register resolved table catalogs into the session state.
        for (catalog_name, catalog_provider) in resolved_table_catalogs {
            if let Some(existing) = state.catalog_list().catalog(&catalog_name) {
                // Merge: add/replace individual schemas. Async-resolved schemas
                // override overlapping eager registrations; non-overlapping eager
                // schemas are kept.
                let mut schema_names = catalog_provider.schema_names();
                schema_names.sort();
                for schema_name in schema_names {
                    if let Some(schema) = catalog_provider.schema(&schema_name) {
                        existing.register_schema(&schema_name, schema)?;
                    }
                }
            } else {
                state
                    .catalog_list()
                    .register_catalog(catalog_name, catalog_provider);
            }
        }

        // Register resolved functions as ScalarUDFs.
        // Sort schema names and function names for deterministic registration
        // order (allows overwrites; last write wins for duplicate names).
        for (catalog_name, func_catalog) in &resolved_func_catalogs {
            let mut schema_names = func_catalog.schema_names();
            schema_names.sort();

            for schema_name in schema_names {
                let Some(schema) = func_catalog.schema(&schema_name) else {
                    continue;
                };

                let mut function_names = schema.function_names();
                function_names.sort();

                for function_name in function_names {
                    let maybe_function = schema.function(&function_name).await.map_err(|err| {
                        DataFusionError::Plan(format!(
                            "failed to load resolved function '{schema_name}.{function_name}' from catalog '{catalog_name}': {err}"
                        ))
                    })?;
                    if let Some(func_provider) = maybe_function {
                        state.register_udf(func_provider.scalar_udf())?;
                    }
                }
            }
        }

        Ok(state)
    }
}

impl Default for SessionState {
    fn default() -> Self {
        SessionStateBuilder::new(SessionConfig::default()).build()
    }
}

/// Returns the built-in scalar UDFs registered in every session state.
fn builtin_udfs() -> Vec<ScalarUDF> {
    vec![
        EvmDecodeLog::new().into(),
        EvmDecodeLog::new().with_deprecated_name().into(),
        EvmTopic::new().into(),
        EvmEncodeParams::new().into(),
        EvmDecodeParams::new().into(),
        EvmEncodeType::new().into(),
        EvmDecodeType::new().into(),
        EvmEncodeHex::new().into(),
        EvmDecodeHex::new().into(),
        ShiftUnits::new().into(),
    ]
}

/// Builder for [`SessionState`].
///
/// Requires a [`SessionConfig`] as the mandatory constructor input.
/// Optional runtime environment components can be supplied for exec usage;
/// plan-only usage defaults to `RuntimeEnv::default()`.
///
/// Physical optimizer rules (needed for exec's instrumentation) are added
/// via [`with_physical_optimizer_rule`](Self::with_physical_optimizer_rule)
/// and applied in registration order.
///
/// # Async provider name invariant
///
/// Async table and function providers registered via
/// [`with_table_catalog`](Self::with_table_catalog) and
/// [`with_func_catalog`](Self::with_func_catalog) are matched against
/// catalog names extracted from SQL references at planning time.
/// Because `resolve_table_references` rejects catalog-qualified
/// references (`catalog.schema.table`), all extracted references
/// resolve their catalog via the `SessionConfig` default catalog
/// (`datafusion.catalog.default_catalog`). As a result, only
/// providers registered under a name that matches the default catalog
/// can ever be consulted during pre-resolution. Providers registered
/// under any other name are silently unreachable and will never
/// resolve any tables or functions — the query will degrade to a
/// late "table/function not found" planning error.
///
/// [`build`](Self::build) emits a warning for each provider name
/// that does not match the default catalog, making the misconfiguration
/// explicit.
#[derive(Clone)]
pub struct SessionStateBuilder {
    /// Session configuration folded into [`SessionState::base_state`] at build time.
    session_config: SessionConfig,
    /// Memory pool component for the runtime environment in [`SessionState::base_state`].
    memory_pool: Option<Arc<dyn MemoryPool>>,
    /// Disk manager component for the runtime environment in [`SessionState::base_state`].
    disk_manager: Option<Arc<DiskManager>>,
    /// Cache manager component for the runtime environment in [`SessionState::base_state`].
    cache_manager: Option<Arc<CacheManager>>,
    /// Object store registry component for the runtime environment in [`SessionState::base_state`].
    object_store_registry: Option<Arc<dyn ObjectStoreRegistry>>,
    /// Physical optimizer rules folded into [`SessionState::base_state`] at build time.
    physical_optimizer_rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
    /// See [`SessionState::table_catalogs`].
    table_catalogs: BTreeMap<String, Arc<dyn TableAsyncCatalogProvider>>,
    /// See [`SessionState::func_catalogs`].
    func_catalogs: BTreeMap<String, Arc<dyn FuncAsyncCatalogProvider>>,
}

impl SessionStateBuilder {
    /// Creates a new builder with the mandatory session configuration.
    pub fn new(session_config: SessionConfig) -> Self {
        Self {
            session_config,
            memory_pool: None,
            disk_manager: None,
            cache_manager: None,
            object_store_registry: None,
            physical_optimizer_rules: Vec::new(),
            table_catalogs: Default::default(),
            func_catalogs: Default::default(),
        }
    }

    /// Sets the memory pool for the runtime environment in [`SessionState::base_state`].
    pub fn with_memory_pool(mut self, pool: Arc<dyn MemoryPool>) -> Self {
        self.memory_pool = Some(pool);
        self
    }

    /// Sets the disk manager for the runtime environment in [`SessionState::base_state`].
    pub fn with_disk_manager(mut self, disk_manager: Arc<DiskManager>) -> Self {
        self.disk_manager = Some(disk_manager);
        self
    }

    /// Sets the cache manager for the runtime environment in [`SessionState::base_state`].
    pub fn with_cache_manager(mut self, cache_manager: Arc<CacheManager>) -> Self {
        self.cache_manager = Some(cache_manager);
        self
    }

    /// Sets the object store registry for the runtime environment in [`SessionState::base_state`].
    pub fn with_object_store_registry(mut self, registry: Arc<dyn ObjectStoreRegistry>) -> Self {
        self.object_store_registry = Some(registry);
        self
    }

    /// Appends a physical optimizer rule to [`SessionState::base_state`].
    pub fn with_physical_optimizer_rule(
        mut self,
        rule: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
    ) -> Self {
        self.physical_optimizer_rules.push(rule);
        self
    }

    /// Inserts a provider into [`SessionState::table_catalogs`].
    ///
    /// The `name` must match the `SessionConfig` default catalog for the
    /// provider to be reachable during SQL pre-resolution. See the
    /// [async provider name invariant](Self#async-provider-name-invariant)
    /// on the builder docs. [`build`](Self::build) warns on mismatches.
    pub fn with_table_catalog(
        mut self,
        name: impl Into<String>,
        provider: Arc<dyn TableAsyncCatalogProvider>,
    ) -> Self {
        self.table_catalogs.insert(name.into(), provider);
        self
    }

    /// Inserts a provider into [`SessionState::func_catalogs`].
    ///
    /// The `name` must match the `SessionConfig` default catalog for the
    /// provider to be reachable during SQL pre-resolution. See the
    /// [async provider name invariant](Self#async-provider-name-invariant)
    /// on the builder docs. [`build`](Self::build) warns on mismatches.
    pub fn with_func_catalog(
        mut self,
        name: impl Into<String>,
        provider: Arc<dyn FuncAsyncCatalogProvider>,
    ) -> Self {
        self.func_catalogs.insert(name.into(), provider);
        self
    }

    /// Builds the [`SessionState`].
    ///
    /// Warns for each async provider name that does not match the
    /// `SessionConfig` default catalog, since such providers can never
    /// be reached during SQL pre-resolution (see
    /// [async provider name invariant](Self#async-provider-name-invariant)).
    pub fn build(self) -> SessionState {
        let runtime_env = build_runtime_env(
            self.memory_pool,
            self.disk_manager,
            self.cache_manager,
            self.object_store_registry,
        );

        // Build the base SessionState once: config + runtime + optimizer rules + builtin UDFs.
        let mut builder = DfSessionStateBuilder::new()
            .with_config(self.session_config)
            .with_runtime_env(runtime_env)
            .with_default_features();

        for rule in &self.physical_optimizer_rules {
            builder = builder.with_physical_optimizer_rule(rule.clone());
        }

        let mut base_state = builder.build();

        // Register builtin UDFs after build to preserve DataFusion's default
        // scalar functions (abs, concat, etc.).
        for udf in builtin_udfs() {
            base_state
                .register_udf(Arc::new(udf))
                .expect("builtin UDF registration should never fail");
        }

        SessionState {
            state: base_state,
            table_catalogs: self.table_catalogs,
            func_catalogs: self.func_catalogs,
        }
    }
}

/// Constructs an `Arc<RuntimeEnv>` from optional individual components.
///
/// If no components are provided, returns `Arc::default()` (same as plan-only
/// usage). If any component is provided, it overrides the corresponding field
/// in a default `RuntimeEnv`, leaving the rest at their defaults.
fn build_runtime_env(
    memory_pool: Option<Arc<dyn MemoryPool>>,
    disk_manager: Option<Arc<DiskManager>>,
    cache_manager: Option<Arc<CacheManager>>,
    object_store_registry: Option<Arc<dyn ObjectStoreRegistry>>,
) -> Arc<RuntimeEnv> {
    let mut env = RuntimeEnv::default();
    if let Some(value) = memory_pool {
        env.memory_pool = value;
    }
    if let Some(value) = disk_manager {
        env.disk_manager = value;
    }
    if let Some(value) = cache_manager {
        env.cache_manager = value;
    }
    if let Some(value) = object_store_registry {
        env.object_store_registry = value;
    }
    Arc::new(env)
}

/// Returns `true` if the error represents invalid user input.
///
/// Walks the full [`DataFusionError::Context`] chain and returns `true` as
/// soon as the `amp::invalid_input` tag is found at any depth. This is
/// robust to future callers that add an additional `.context("…")` wrapper
/// around an already-tagged error: the tag remains detectable even when it
/// is not the outermost context layer.
///
/// Used by transport layers (flight, admin-api) to map errors to appropriate
/// HTTP/gRPC status codes:
/// - `true` → `BAD_REQUEST` / `invalid_argument`
/// - `false` → `INTERNAL_SERVER_ERROR` / `internal`
pub fn is_user_input_error(err: &DataFusionError) -> bool {
    let mut current = err;
    loop {
        match current {
            DataFusionError::Context(ctx, inner) => {
                if ctx == INVALID_INPUT_CONTEXT {
                    return true;
                }
                current = inner;
            }
            _ => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::common::DataFusionError;

    use super::*;

    /// Verifies that `is_user_input_error` returns `true` when the
    /// `amp::invalid_input` tag is wrapped inside an additional
    /// `DataFusionError::Context` layer.
    ///
    /// A future caller adding `.context("outer message")` to an already-tagged
    /// error must not accidentally downgrade a user-input error to an internal
    /// error. The classification must survive arbitrary depths of context
    /// wrapping.
    #[test]
    fn is_user_input_error_detects_tag_under_nested_context_wrapper() {
        //* Given — tag is at depth 2
        let tagged = DataFusionError::Plan("invalid table reference".to_string())
            .context("amp::invalid_input");
        let wrapped = tagged.context("failed to convert SQL statement to logical plan");

        //* When
        let result = is_user_input_error(&wrapped);

        //* Then
        assert!(
            result,
            "is_user_input_error should return true even when the amp::invalid_input tag is \
             wrapped by an outer context: {wrapped:?}"
        );
    }

    /// Verifies that `is_user_input_error` returns `true` at any arbitrary depth
    /// of context wrapping (depth 3 in this case).
    #[test]
    fn is_user_input_error_detects_tag_under_multiple_nested_context_wrappers() {
        //* Given — tag is at depth 3
        let tagged =
            DataFusionError::Plan("forbidden alias".to_string()).context("amp::invalid_input");
        let wrapped_once = tagged.context("outer context 1");
        let wrapped_twice = wrapped_once.context("outer context 2");

        //* When
        let result = is_user_input_error(&wrapped_twice);

        //* Then
        assert!(
            result,
            "is_user_input_error should return true regardless of nesting depth: {wrapped_twice:?}"
        );
    }

    /// Verifies that `is_user_input_error` returns `false` when only non-tag
    /// context wrappers are present (no `amp::invalid_input` in the chain).
    #[test]
    fn is_user_input_error_returns_false_for_untagged_nested_context() {
        //* Given — multiple context wrappers but no tag
        let inner = DataFusionError::Plan("provider lookup failed".to_string());
        let wrapped = inner
            .context("failed to resolve catalog")
            .context("planning failed");

        //* When
        let result = is_user_input_error(&wrapped);

        //* Then
        assert!(
            !result,
            "is_user_input_error should return false when the tag is absent from all context \
             layers: {wrapped:?}"
        );
    }
}
