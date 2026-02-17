use std::{
    collections::BTreeMap,
    pin::Pin,
    sync::{Arc, LazyLock},
    task::{Context, Poll},
};

use arrow::{array::ArrayRef, compute::concat_batches, datatypes::SchemaRef};
use datafusion::{
    self,
    arrow::array::RecordBatch,
    catalog::MemorySchemaProvider,
    error::DataFusionError,
    execution::{
        RecordBatchStream, SendableRecordBatchStream, SessionStateBuilder, config::SessionConfig,
        context::SessionContext, memory_pool::human_readable_size, runtime_env::RuntimeEnv,
    },
    logical_expr::LogicalPlan,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{ExecutionPlan, displayable, stream::RecordBatchStreamAdapter},
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner as _},
    sql::parser,
};
use datafusion_tracing::{
    InstrumentationOptions, instrument_with_info_spans, pretty_format_compact_batch,
};
use datasets_common::network_id::NetworkId;
use futures::{Stream, TryStreamExt, stream};
use regex::Regex;
use tracing::field;

use crate::{
    BlockNum, BlockRange, arrow,
    catalog::physical::{Catalog, CatalogSnapshot, TableSnapshot},
    context::common::{
        ReadOnlyCheckError, SqlToPlanError, builtin_udfs, read_only_check, sql_to_plan,
    },
    memory_pool::{MemoryPoolKind, TieredMemoryPool, make_memory_pool},
    plan_visitors::{extract_table_references_from_plan, forbid_duplicate_field_names},
    query_env::QueryEnv,
    sql::{TableReference, TableReferenceConversionError},
};

/// A context for executing queries against a catalog.
#[derive(Clone)]
pub struct QueryContext {
    pub env: QueryEnv,
    session_config: SessionConfig,
    catalog: CatalogSnapshot,
    /// Per-query memory pool (if per-query limits are enabled)
    tiered_memory_pool: Arc<TieredMemoryPool>,
}

impl QueryContext {
    /// Creates a query context from a physical catalog.
    pub async fn for_catalog(
        env: QueryEnv,
        catalog: Catalog,
        ignore_canonical_segments: bool,
    ) -> Result<Self, CreateContextError> {
        // Create a catalog snapshot with canonical chain locked in
        let catalog_snapshot =
            CatalogSnapshot::from_catalog(catalog, ignore_canonical_segments, env.store.clone())
                .await
                .map_err(CreateContextError::CatalogSnapshot)?;

        let tiered_memory_pool: Arc<TieredMemoryPool> = {
            let per_query_bytes = env.query_max_mem_mb * 1024 * 1024;
            let child_pool = make_memory_pool(MemoryPoolKind::Greedy, per_query_bytes);

            Arc::new(TieredMemoryPool::new(
                env.global_memory_pool.clone(),
                child_pool,
            ))
        };

        Ok(Self {
            session_config: env.session_config.clone(),
            env,
            catalog: catalog_snapshot,
            tiered_memory_pool,
        })
    }

    /// Returns the catalog snapshot backing this query context.
    pub fn catalog(&self) -> &CatalogSnapshot {
        &self.catalog
    }

    /// Converts a parsed SQL statement into a logical plan against the physical catalog.
    pub async fn plan_sql(&self, query: parser::Statement) -> Result<LogicalPlan, PlanSqlError> {
        let ctx = new_session_ctx(
            self.session_config.clone(),
            &self.tiered_memory_pool,
            &self.env,
            &self.catalog,
        )
        .map_err(PlanSqlError::RegisterTable)?;

        let plan = sql_to_plan(&ctx, query)
            .await
            .map_err(PlanSqlError::SqlToPlan)?;
        Ok(plan)
    }

    /// Executes a logical plan and returns a streaming result set.
    pub async fn execute_plan(
        &self,
        plan: LogicalPlan,
        logical_optimize: bool,
    ) -> Result<SendableRecordBatchStream, ExecutePlanError> {
        let ctx = new_session_ctx(
            self.session_config.clone(),
            &self.tiered_memory_pool,
            &self.env,
            &self.catalog,
        )
        .map_err(ExecutePlanError::RegisterTable)?;

        let result = execute_plan(&ctx, plan, logical_optimize)
            .await
            .map_err(ExecutePlanError::Execute)?;

        Ok(PeakMemoryStream::wrap(
            result,
            self.tiered_memory_pool.clone(),
        ))
    }

    /// This will load the result set entirely in memory, so it should be used with caution.
    pub async fn execute_and_concat(
        &self,
        plan: LogicalPlan,
    ) -> Result<RecordBatch, ExecuteAndConcatError> {
        let schema = plan.schema().inner().clone();
        let ctx = new_session_ctx(
            self.session_config.clone(),
            &self.tiered_memory_pool,
            &self.env,
            &self.catalog,
        )
        .map_err(ExecuteAndConcatError::RegisterTable)?;

        let batch_stream = execute_plan(&ctx, plan, true)
            .await
            .map_err(ExecuteAndConcatError::Execute)?
            .try_collect::<Vec<_>>()
            .await
            .map_err(ExecuteAndConcatError::CollectResults)?;

        Ok(concat_batches(&schema, &batch_stream).unwrap())
    }

    /// Looks up a table snapshot by reference. Fails if the table is not in the catalog.
    pub fn get_table(
        &self,
        table_ref: &TableReference,
    ) -> Result<Arc<TableSnapshot>, TableNotFoundError> {
        self.catalog
            .table_snapshots()
            .iter()
            .find(|snapshot| snapshot.physical_table().table_ref() == *table_ref)
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

/// Failed to create a `QueryContext` from a catalog
///
/// This error covers failures during `QueryContext::for_catalog()`.
#[derive(Debug, thiserror::Error)]
pub enum CreateContextError {
    /// Failed to create a catalog snapshot from the physical catalog
    ///
    /// This occurs when creating snapshots of physical tables, typically
    /// due to segment metadata fetch failures or canonical chain computation errors.
    #[error("failed to create catalog snapshot")]
    CatalogSnapshot(#[source] crate::catalog::physical::FromCatalogError),
}

/// Failed to plan a SQL query in the query context
///
/// This error covers failures during `QueryContext::plan_sql()`.
#[derive(Debug, thiserror::Error)]
pub enum PlanSqlError {
    /// Failed to create a query session context
    ///
    /// This occurs when building a `SessionContext` for query execution fails,
    /// typically due to a table registration error during context setup.
    #[error("failed to create query session context")]
    RegisterTable(#[source] RegisterTableError),

    /// Failed to convert SQL to a logical plan
    ///
    /// This occurs during SQL-to-logical-plan conversion, including
    /// statement parsing, alias validation, and read-only enforcement.
    #[error("failed to convert SQL to logical plan: {0}")]
    SqlToPlan(#[source] SqlToPlanError),
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
}

/// Failed to execute a plan via `QueryContext::execute_plan`
///
/// This error wraps session context creation and inner execution errors.
#[derive(Debug, thiserror::Error)]
pub enum ExecutePlanError {
    /// Failed to create a query session context
    #[error("failed to create query session context")]
    RegisterTable(#[source] RegisterTableError),

    /// Failed during plan execution
    #[error("failed to execute plan")]
    Execute(#[source] ExecuteError),
}

/// Failed to execute a plan and concatenate results
///
/// This error covers `QueryContext::execute_and_concat()`.
#[derive(Debug, thiserror::Error)]
pub enum ExecuteAndConcatError {
    /// Failed to create a query session context
    #[error("failed to create query session context")]
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

/// Because `DatasetContext` is read-only, planning and execution can be done on ephemeral
/// sessions created by this function, and they will behave the same as if they had been run
/// against a persistent `SessionContext`
fn new_session_ctx(
    config: SessionConfig,
    tiered_memory_pool: &Arc<TieredMemoryPool>,
    env: &QueryEnv,
    catalog: &CatalogSnapshot,
) -> Result<SessionContext, RegisterTableError> {
    // Build per-query ctx
    let ctx = {
        let runtime_env = Arc::new(RuntimeEnv {
            memory_pool: tiered_memory_pool.clone(),
            disk_manager: env.disk_manager.clone(),
            cache_manager: env.cache_manager.clone(),
            object_store_registry: env.object_store_registry.clone(),
        });

        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime_env)
            .with_default_features()
            .with_physical_optimizer_rule(create_instrumentation_rule())
            .build();

        SessionContext::new_with_state(state)
    };

    // Register the builtin UDFs
    for udf in builtin_udfs() {
        ctx.register_udf(udf);
    }

    // Register catalog tables and UDFs
    for table in catalog.table_snapshots() {
        // The catalog schema needs to be explicitly created or table creation will fail.
        let schema_name = table.physical_table().sql_table_ref_schema();
        if ctx
            .catalog(&ctx.catalog_names()[0])
            .unwrap()
            .schema(schema_name)
            .is_none()
        {
            let schema = Arc::new(MemorySchemaProvider::new());
            ctx.catalog(&ctx.catalog_names()[0])
                .unwrap()
                .register_schema(schema_name, schema)
                .unwrap();
        }

        let table_ref = table.physical_table().table_ref().clone();

        // This may overwrite a previously registered store, but that should not make a difference.
        // The only segment of the `table.url()` that matters here is the schema and bucket name.
        ctx.register_object_store(
            table.physical_table().url(),
            env.store.as_datafusion_object_store().clone(),
        );

        ctx.register_table(table_ref, table.clone())
            .map_err(RegisterTableError)?;
    }
    for udf in catalog.udfs() {
        ctx.register_udf(udf.clone());
    }

    Ok(ctx)
}

/// Failed to register a dataset table with the query session context
///
/// This occurs when DataFusion rejects a table registration during query
/// session creation, typically because a table with the same name already
/// exists or the table metadata is invalid.
#[derive(Debug, thiserror::Error)]
#[error("Failed to register dataset table with query session context")]
pub struct RegisterTableError(#[source] DataFusionError);

/// `logical_optimize` controls whether logical optimizations should be applied to `plan`.
#[tracing::instrument(skip_all, err)]
async fn execute_plan(
    ctx: &SessionContext,
    mut plan: LogicalPlan,
    logical_optimize: bool,
) -> Result<SendableRecordBatchStream, ExecuteError> {
    use datafusion::physical_plan::execute_stream;

    read_only_check(&plan).map_err(ExecuteError::ReadOnlyCheck)?;
    tracing::debug!("logical plan: {}", plan.to_string().replace('\n', "\\n"));

    if logical_optimize {
        plan = ctx
            .state()
            .optimize(&plan)
            .map_err(ExecuteError::Optimize)?;
    }

    let is_explain = matches!(plan, LogicalPlan::Explain(_) | LogicalPlan::Analyze(_));

    let planner = DefaultPhysicalPlanner::default();
    let physical_plan = planner
        .create_physical_plan(&plan, &ctx.state())
        .await
        .map_err(ExecuteError::CreatePhysicalPlan)?;

    forbid_duplicate_field_names(&physical_plan, &plan)
        .map_err(ExecuteError::DuplicateFieldNames)?;

    tracing::debug!("physical plan: {}", print_physical_plan(&*physical_plan));

    match is_explain {
        false => execute_stream(physical_plan, ctx.task_ctx()).map_err(ExecuteError::ExecuteStream),
        true => execute_explain(physical_plan, ctx).await,
    }
}

// We do special handling for `Explain` plans to ensure that the output is sanitized from full paths.
async fn execute_explain(
    physical_plan: Arc<dyn ExecutionPlan>,
    ctx: &SessionContext,
) -> Result<SendableRecordBatchStream, ExecuteError> {
    use datafusion::physical_plan::execution_plan;

    let schema = physical_plan.schema().clone();
    let output = execution_plan::collect(physical_plan, ctx.task_ctx())
        .await
        .map_err(ExecuteError::CollectExplainResults)?;

    let concatenated = concat_batches(&schema, &output).unwrap();
    let sanitized = sanitize_explain(&concatenated);

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

// Sanitize the explain output by removing full paths and and keeping only the filenames.
fn sanitize_explain(batch: &RecordBatch) -> RecordBatch {
    use arrow::array::StringArray;

    let plan_idx = batch.schema().index_of("plan").unwrap();
    let plan_column = batch
        .column(plan_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    let transformed: StringArray = plan_column
        .iter()
        .map(|value| value.map(sanitize_parquet_paths))
        .collect();

    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns[plan_idx] = Arc::new(transformed);

    RecordBatch::try_new(batch.schema(), columns).unwrap()
}

/// Prints the physical plan to a single line, for logging.
fn print_physical_plan(plan: &dyn ExecutionPlan) -> String {
    let plan_str = displayable(plan)
        .indent(false)
        .to_string()
        .replace('\n', "\\n");
    sanitize_parquet_paths(&plan_str)
}

/// A stream wrapper that logs peak memory usage when dropped.
///
/// Because `execute_plan` returns a lazy `SendableRecordBatchStream`, memory is only
/// allocated when the stream is consumed. This wrapper defers the peak memory log to
/// when the stream is dropped (i.e., after consumption or cancellation).
struct PeakMemoryStream {
    inner: SendableRecordBatchStream,
    pool: Arc<TieredMemoryPool>,
}

impl PeakMemoryStream {
    fn wrap(
        inner: SendableRecordBatchStream,
        pool: Arc<TieredMemoryPool>,
    ) -> SendableRecordBatchStream {
        Box::pin(Self { inner, pool })
    }
}

impl Drop for PeakMemoryStream {
    fn drop(&mut self) {
        tracing::debug!(
            peak_memory_mb = human_readable_size(self.pool.peak_reserved()),
            "Query memory usage"
        );
    }
}

impl Stream for PeakMemoryStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl RecordBatchStream for PeakMemoryStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
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
