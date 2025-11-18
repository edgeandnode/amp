use std::{
    ops::RangeInclusive,
    sync::{Arc, LazyLock},
};

use arrow::{array::ArrayRef, compute::concat_batches};
use axum::response::IntoResponse;
use datafusion::{
    self,
    arrow::array::RecordBatch,
    catalog::MemorySchemaProvider,
    error::DataFusionError,
    execution::{
        SendableRecordBatchStream, SessionStateBuilder,
        config::SessionConfig,
        context::{SQLOptions, SessionContext},
        runtime_env::RuntimeEnv,
    },
    logical_expr::{AggregateUDF, LogicalPlan, ScalarUDF},
    parquet::file::metadata::ParquetMetaData,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{ExecutionPlan, displayable, stream::RecordBatchStreamAdapter},
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner as _},
    scalar::ScalarValue,
    sql::parser,
};
use datafusion_tracing::{
    InstrumentationOptions, instrument_with_info_spans, pretty_format_compact_batch,
};
use foyer::Cache;
use futures::{TryStreamExt, stream};
use js_runtime::isolate_pool::IsolatePool;
use metadata_db::FileId;
use regex::Regex;
use thiserror::Error;
use tracing::{debug, field, instrument};

use crate::{
    BlockNum, BoxError, arrow, block_range_intersection,
    catalog::physical::{Catalog, CatalogSnapshot, TableSnapshot},
    evm::udfs::{
        EvmDecodeLog, EvmDecodeParams, EvmDecodeType, EvmEncodeParams, EvmEncodeType, EvmTopic,
    },
    plan_visitors::{
        extract_table_references_from_plan, forbid_duplicate_field_names,
        forbid_underscore_prefixed_aliases,
    },
    sql::TableReference,
    utils::error_with_causes,
};

pub fn default_catalog_name() -> ScalarValue {
    ScalarValue::Utf8(Some("amp".to_string()))
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("invalid plan: {0}")]
    InvalidPlan(DataFusionError),

    #[error("planning error: {0}")]
    PlanningError(DataFusionError),

    #[error("query execution error: {0}")]
    ExecutionError(DataFusionError),

    /// Signals a problem with the dataset configuration.
    #[error("dataset error: {0}")]
    DatasetError(BoxError),

    #[error("meta table error: {0}")]
    MetaTableError(DataFusionError),

    #[error("SQL parse error")]
    SqlParseError(#[source] crate::sql::ParseSqlError),

    #[error("DataFusion configuration error: {0}")]
    ConfigError(DataFusionError),

    #[error("table not found: {0}")]
    TableNotFoundError(TableReference),
}

impl IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        let err = error_with_causes(&self);
        let status = match self {
            Error::SqlParseError(_) => axum::http::StatusCode::BAD_REQUEST,
            Error::InvalidPlan(_) => axum::http::StatusCode::BAD_REQUEST,
            Error::TableNotFoundError(_) => axum::http::StatusCode::NOT_FOUND,
            Error::DatasetError(_) => axum::http::StatusCode::BAD_REQUEST,
            Error::ConfigError(_) => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Error::PlanningError(_) => axum::http::StatusCode::BAD_REQUEST,
            Error::ExecutionError(_) | Error::MetaTableError(_) => {
                axum::http::StatusCode::INTERNAL_SERVER_ERROR
            }
        };
        let body = serde_json::json!({
            "error_code": match self {
                Error::SqlParseError(_) => "SQL_PARSE_ERROR",
                Error::InvalidPlan(_) => "INVALID_PLAN",
                Error::DatasetError(_) => "DATASET_ERROR",
                Error::ConfigError(_) => "CONFIG_ERROR",
                Error::PlanningError(_) => "PLANNING_ERROR",
                Error::ExecutionError(_) => "EXECUTION_ERROR",
                Error::MetaTableError(_) => "META_TABLE_ERROR",
                Error::TableNotFoundError(_) => "TABLE_NOT_FOUND_ERROR",
            },
            "error_message": err,
        });
        (status, axum::Json(body)).into_response()
    }
}

/// Handle to the environment resources used by the query engine.
#[derive(Clone, Debug)]
pub struct QueryEnv {
    pub df_env: Arc<RuntimeEnv>,
    pub isolate_pool: IsolatePool,
    pub parquet_footer_cache: Cache<FileId, Arc<ParquetMetaData>>,
}

/// A context for executing queries against a catalog.
#[derive(Clone)]
pub struct QueryContext {
    pub env: QueryEnv,
    session_config: SessionConfig,
    catalog: CatalogSnapshot,
}

impl QueryContext {
    pub async fn for_catalog(
        catalog: Catalog,
        env: QueryEnv,
        ignore_canonical_segments: bool,
    ) -> Result<Self, Error> {
        // This contains various tuning options for the query engine.
        // Using `from_env` allows tinkering without re-compiling.
        let mut session_config = SessionConfig::from_env().map_err(Error::ConfigError)?.set(
            "datafusion.catalog.default_catalog",
            &default_catalog_name(),
        );

        let opts = session_config.options_mut();

        // Rationale for DataFusion settings:
        //
        // `prefer_existing_sort` takes advantage of our files being time-partitioned and each file
        // having the rows written in sorted order.
        //
        // `pushdown_filters` should be helpful for very selective queries, which is something we
        // want to optimize for.

        // Set `prefer_existing_sort` by default.
        if std::env::var_os("DATAFUSION_OPTIMIZER_PREFER_EXISTING_SORT").is_none() {
            opts.optimizer.prefer_existing_sort = true;
        }

        // Set `parquet.pushdown_filters` by default.
        //
        // See https://github.com/apache/datafusion/issues/3463 for upstream default tracking.
        if std::env::var_os("DATAFUSION_EXECUTION_PARQUET_PUSHDOWN_FILTERS").is_none() {
            opts.execution.parquet.pushdown_filters = true;
        }

        // Create a catalog snapshot with canonical chain locked in
        let catalog_snapshot = CatalogSnapshot::from_catalog(
            catalog,
            ignore_canonical_segments,
            env.parquet_footer_cache.clone(),
        )
        .await
        .map_err(Error::DatasetError)?;

        Ok(Self {
            env,
            session_config,
            catalog: catalog_snapshot,
        })
    }

    pub fn catalog(&self) -> &CatalogSnapshot {
        &self.catalog
    }

    pub async fn plan_sql(&self, query: parser::Statement) -> Result<LogicalPlan, Error> {
        let ctx = self.datafusion_ctx()?;
        let plan = sql_to_plan(&ctx, query).await?;
        Ok(plan)
    }

    /// Because `DatasetContext` is read-only, planning and execution can be done on ephemeral
    /// sessions created by this function, and they will behave the same as if they had been run
    /// against a persistent `SessionContext`
    fn datafusion_ctx(&self) -> Result<SessionContext, Error> {
        let state = SessionStateBuilder::new()
            .with_config(self.session_config.clone())
            .with_runtime_env(self.env.df_env.clone())
            .with_default_features()
            .with_physical_optimizer_rule(create_instrumentation_rule())
            .build();
        let ctx = SessionContext::new_with_state(state);

        for table in self.catalog.table_snapshots() {
            register_table(&ctx, table.clone()).map_err(|e| Error::DatasetError(e.into()))?;
        }

        self.register_udfs(&ctx);

        Ok(ctx)
    }

    fn register_udfs(&self, ctx: &SessionContext) {
        for udf in udfs() {
            ctx.register_udf(udf);
        }
        for udaf in udafs() {
            ctx.register_udaf(udaf);
        }
        for udf in self.catalog.udfs() {
            ctx.register_udf(udf.clone());
        }
    }

    pub async fn execute_plan(
        &self,
        plan: LogicalPlan,
        logical_optimize: bool,
    ) -> Result<SendableRecordBatchStream, Error> {
        let ctx = self.datafusion_ctx()?;
        execute_plan(&ctx, plan, logical_optimize).await
    }

    /// This will load the result set entirely in memory, so it should be used with caution.
    pub async fn execute_and_concat(&self, plan: LogicalPlan) -> Result<RecordBatch, Error> {
        let schema = plan.schema().inner().clone();
        let ctx = self.datafusion_ctx()?;
        let batch_stream = execute_plan(&ctx, plan, true)
            .await?
            .try_collect::<Vec<_>>()
            .await
            .map_err(Error::ExecutionError)?;
        Ok(concat_batches(&schema, &batch_stream).unwrap())
    }

    /// Returns table or `TableNotFoundError`
    pub fn get_table(&self, table_ref: &TableReference) -> Result<Arc<TableSnapshot>, Error> {
        self.catalog
            .table_snapshots()
            .iter()
            .find(|snapshot| snapshot.physical_table().table().table_ref() == table_ref)
            .cloned()
            .ok_or_else(|| Error::TableNotFoundError(table_ref.clone()))
    }

    /// Get the blocks that have been synced for all tables in the plan.
    #[instrument(skip_all, err)]
    pub async fn synced_blocks_for_plan(
        &self,
        plan: &LogicalPlan,
    ) -> Result<Option<RangeInclusive<BlockNum>>, BoxError> {
        let mut range: Option<RangeInclusive<BlockNum>> = None;
        for df_table_ref in extract_table_references_from_plan(plan)? {
            let table_ref: TableReference<String> = df_table_ref.try_into()?;
            range = match (range, self.get_synced_range_for_table(&table_ref)?) {
                (None, range) | (range, None) => range,
                (Some(a), Some(b)) => block_range_intersection(a, b),
            };
        }
        Ok(range)
    }

    /// Get the most recent block that has been synced for all tables in the plan.
    #[instrument(skip_all, err)]
    pub async fn max_end_block(&self, plan: &LogicalPlan) -> Result<Option<BlockNum>, BoxError> {
        let range = self.synced_blocks_for_plan(plan).await?;
        Ok(range.map(|r| *r.end()))
    }

    /// Helper method to get ranges for a specific table.
    fn get_synced_range_for_table(
        &self,
        table: &TableReference,
    ) -> Result<Option<RangeInclusive<BlockNum>>, BoxError> {
        let table_snapshot = self.get_table(table)?;
        Ok(table_snapshot.synced_range())
    }
}

#[instrument(skip_all, err)]
pub async fn sql_to_plan(
    ctx: &SessionContext,
    query: parser::Statement,
) -> Result<LogicalPlan, Error> {
    let plan = ctx
        .state()
        .statement_to_plan(query)
        .await
        .map_err(Error::PlanningError)?;
    verify_plan(&plan)?;
    Ok(plan)
}

pub fn verify_plan(plan: &LogicalPlan) -> Result<(), Error> {
    forbid_underscore_prefixed_aliases(plan).map_err(Error::InvalidPlan)?;
    read_only_check(plan)
}

fn read_only_check(plan: &LogicalPlan) -> Result<(), Error> {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
        .verify_plan(plan)
        .map_err(Error::InvalidPlan)
}

fn register_table(ctx: &SessionContext, table: Arc<TableSnapshot>) -> Result<(), DataFusionError> {
    // The catalog schema needs to be explicitly created or table creation will fail.
    create_catalog_schema(ctx, table.physical_table().catalog_schema().to_string());

    let table_ref = table.physical_table().table_ref().clone();

    // This may overwrite a previously registered store, but that should not make a difference.
    // The only segment of the `table.url()` that matters here is the schema and bucket name.
    ctx.register_object_store(
        table.physical_table().url(),
        table.physical_table().object_store(),
    );

    ctx.register_table(table_ref, table)?;

    Ok(())
}

pub fn create_catalog_schema(ctx: &SessionContext, schema_name: String) {
    // We only have use catalog, the default one.
    let catalog = ctx.catalog(&ctx.catalog_names()[0]).unwrap();
    if catalog.schema(&schema_name).is_none() {
        let schema = Arc::new(MemorySchemaProvider::new());
        catalog.register_schema(&schema_name, schema).unwrap();
    }
}

pub fn udfs() -> Vec<ScalarUDF> {
    vec![
        EvmDecodeLog::new().into(),
        EvmDecodeLog::new().with_deprecated_name().into(),
        EvmTopic::new().into(),
        EvmEncodeParams::new().into(),
        EvmDecodeParams::new().into(),
        EvmEncodeType::new().into(),
        EvmDecodeType::new().into(),
    ]
}

pub fn udafs() -> Vec<AggregateUDF> {
    vec![]
}

/// `logical_optimize` controls whether logical optimizations should be applied to `plan`.
#[instrument(skip_all, err)]
async fn execute_plan(
    ctx: &SessionContext,
    mut plan: LogicalPlan,
    logical_optimize: bool,
) -> Result<SendableRecordBatchStream, Error> {
    use datafusion::physical_plan::execute_stream;

    read_only_check(&plan)?;
    debug!("logical plan: {}", plan.to_string().replace('\n', "\\n"));

    if logical_optimize {
        plan = ctx.state().optimize(&plan).map_err(Error::PlanningError)?;
    }

    let is_explain = matches!(plan, LogicalPlan::Explain(_) | LogicalPlan::Analyze(_));

    let planner = DefaultPhysicalPlanner::default();
    let physical_plan = planner
        .create_physical_plan(&plan, &ctx.state())
        .await
        .map_err(Error::PlanningError)?;

    forbid_duplicate_field_names(&physical_plan, &plan).map_err(Error::PlanningError)?;

    debug!("physical plan: {}", print_physical_plan(&*physical_plan));

    match is_explain {
        false => execute_stream(physical_plan, ctx.task_ctx()).map_err(Error::PlanningError),
        true => execute_explain(physical_plan, ctx).await,
    }
}

// We do special handling for `Explain` plans to ensure that the output is sanitized from full paths.
async fn execute_explain(
    physical_plan: Arc<dyn ExecutionPlan>,
    ctx: &SessionContext,
) -> Result<SendableRecordBatchStream, Error> {
    use datafusion::physical_plan::execution_plan;

    let schema = physical_plan.schema().clone();
    let output = execution_plan::collect(physical_plan, ctx.task_ctx())
        .await
        .map_err(Error::ExecutionError)?;

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
