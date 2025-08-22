use std::{ops::RangeInclusive, sync::Arc};

use arrow::{array::ArrayRef, compute::concat_batches};
use async_trait::async_trait;
use axum::response::IntoResponse;
use bincode::{Decode, Encode, config};
use bytes::Bytes;
use datafusion::{
    self,
    arrow::{
        array::RecordBatch,
        datatypes::{DataType, Field, Fields, SchemaRef},
    },
    catalog::{MemorySchemaProvider, Session, TableProvider},
    common::{
        DFSchema, DFSchemaRef, not_impl_err,
        tree_node::{TreeNode as _, TreeNodeRecursion},
    },
    datasource::{DefaultTableSource, TableType},
    error::DataFusionError,
    execution::{
        SendableRecordBatchStream, SessionStateBuilder,
        config::SessionConfig,
        context::{SQLOptions, SessionContext},
        runtime_env::RuntimeEnv,
    },
    logical_expr::{AggregateUDF, Extension, LogicalPlan, ScalarUDF, TableScan},
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{ExecutionPlan, displayable, stream::RecordBatchStreamAdapter},
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner as _},
    prelude::Expr,
    sql::{TableReference, parser},
};
use datafusion_proto::{
    bytes::{
        logical_plan_from_bytes_with_extension_codec, logical_plan_to_bytes_with_extension_codec,
    },
    logical_plan::LogicalExtensionCodec,
};
use datafusion_tracing::{
    InstrumentationOptions, instrument_with_info_spans, pretty_format_compact_batch,
};
use futures::{FutureExt as _, TryStreamExt, stream};
use js_runtime::isolate_pool::IsolatePool;
use metadata_db::TableId;
use regex::Regex;
use thiserror::Error;
use tracing::{debug, field, instrument};

use crate::{
    BlockNum, BoxError, LogicalCatalog, ResolvedTable, SPECIAL_BLOCK_NUM, arrow, attestation,
    block_range_intersection,
    catalog::physical::{Catalog, CatalogSnapshot, TableSnapshot},
    evm::udfs::{
        EvmDecodeLog, EvmDecodeParams, EvmDecodeType, EvmEncodeParams, EvmEncodeType, EvmTopic,
    },
    plan_visitors::{
        constrain_by_block_num, extract_table_references_from_plan,
        forbid_underscore_prefixed_aliases, is_incremental, order_by_block_num,
        propagate_block_num, unproject_special_block_num_column,
    },
    stream_helpers::is_streaming,
};

#[derive(Error, Debug)]
pub enum Error {
    #[error("invalid plan: {0}")]
    InvalidPlan(DataFusionError),

    #[error("plan encoding error: {0}")]
    PlanEncodingError(DataFusionError),

    #[error("plan decoding error: {0}")]
    PlanDecodingError(DataFusionError),

    #[error("planning error: {0}")]
    PlanningError(DataFusionError),

    #[error("query execution error: {0}")]
    ExecutionError(DataFusionError),

    /// Signals a problem with the dataset configuration.
    #[error("dataset error: {0}")]
    DatasetError(BoxError),

    #[error("meta table error: {0}")]
    MetaTableError(DataFusionError),

    #[error("SQL parse error: {0}")]
    SqlParseError(BoxError),

    #[error("DataFusion configuration error: {0}")]
    ConfigError(DataFusionError),

    #[error("table not found: {0}")]
    TableNotFoundError(TableReference),
}

impl IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        let err = self.to_string();
        let status = match self {
            Error::SqlParseError(_) => axum::http::StatusCode::BAD_REQUEST,
            Error::InvalidPlan(_) => axum::http::StatusCode::BAD_REQUEST,
            Error::TableNotFoundError(_) => axum::http::StatusCode::NOT_FOUND,
            Error::PlanEncodingError(_) | Error::PlanDecodingError(_) => {
                axum::http::StatusCode::INTERNAL_SERVER_ERROR
            }
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
                Error::PlanEncodingError(_) => "PLAN_ENCODING_ERROR",
                Error::PlanDecodingError(_) => "PLAN_DECODING_ERROR",
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

/// A context for planning SQL queries.
pub struct PlanningContext {
    session_config: SessionConfig,
    catalog: LogicalCatalog,
}

// Serialized plan with additional options
#[derive(Encode, Decode)]
pub struct RemotePlan {
    pub serialized_plan: Vec<u8>,
    pub is_streaming: bool,
    pub table_refs: Vec<String>,
    pub function_refs: Vec<String>,
}

pub fn remote_plan_from_bytes(bytes: &Bytes) -> Result<RemotePlan, Error> {
    let (remote_plan, _) = bincode::decode_from_slice(bytes, config::standard()).map_err(|e| {
        Error::PlanDecodingError(DataFusionError::Plan(format!(
            "Failed to serialize remote plan: {}",
            e
        )))
    })?;
    Ok(remote_plan)
}

impl PlanningContext {
    pub fn new(catalog: LogicalCatalog) -> Self {
        Self {
            session_config: SessionConfig::from_env().unwrap(),
            catalog,
        }
    }

    /// Infers the output schema of the query by planning it against empty tables.
    pub async fn sql_output_schema(&self, query: parser::Statement) -> Result<DFSchemaRef, Error> {
        let ctx = self.datafusion_ctx()?;
        let plan = sql_to_plan(&ctx, query).await?;
        Ok(plan.schema().clone())
    }

    /// This will plan the query against empty tables, and then serialize that plan using
    /// datafusion-proto. This is useful for sending the plan to a remote server for execution.
    ///
    /// Returns the serialized plan and its output schema.
    pub async fn sql_to_remote_plan(
        &self,
        query: parser::Statement,
    ) -> Result<(Bytes, DFSchemaRef), Error> {
        let is_streaming = is_streaming(&query);
        let ctx = self.datafusion_ctx()?;
        let plan = sql_to_plan(&ctx, query).await?;
        let schema = plan.schema().clone();
        let serialized_plan =
            logical_plan_to_bytes_with_extension_codec(&plan, &TableProviderCodec)
                .map_err(Error::PlanEncodingError)?;
        let LogicalCatalog { tables, udfs } = &self.catalog;
        let table_refs = tables.iter().map(|t| t.table_ref().to_string()).collect();
        let remote_plan = RemotePlan {
            serialized_plan: serialized_plan.to_vec(),
            is_streaming,
            table_refs,
            function_refs: udfs.iter().map(|f| f.name().to_string()).collect(),
        };
        let serialized_plan = Bytes::from(
            bincode::encode_to_vec(&remote_plan, config::standard()).map_err(|e| {
                Error::PlanEncodingError(DataFusionError::Plan(format!(
                    "Failed to serialize remote plan: {}",
                    e
                )))
            })?,
        );

        Ok((serialized_plan, schema))
    }

    fn datafusion_ctx(&self) -> Result<SessionContext, Error> {
        let state = SessionStateBuilder::new()
            .with_config(self.session_config.clone())
            .with_runtime_env(Default::default())
            .with_default_features()
            .build();
        let ctx = SessionContext::new_with_state(state);
        for table in &self.catalog.tables {
            // The catalog schema needs to be explicitly created or table creation will fail.
            create_catalog_schema(&ctx, table.catalog_schema().to_string());
            let planning_table = PlanningTable(table.clone());
            ctx.register_table(table.table_ref().clone(), Arc::new(planning_table))
                .map_err(|e| Error::DatasetError(e.into()))?;
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
        for udf in self.catalog.udfs.iter() {
            ctx.register_udf(udf.clone());
        }
    }

    pub fn catalog(&self) -> &[ResolvedTable] {
        &self.catalog.tables
    }

    pub async fn optimize_plan(
        &self,
        plan: &DetachedLogicalPlan,
    ) -> Result<DetachedLogicalPlan, Error> {
        self.datafusion_ctx()?
            .state()
            .optimize(&plan.0)
            .map_err(Error::PlanningError)
            .map(DetachedLogicalPlan)
    }

    pub async fn plan_sql(&self, query: parser::Statement) -> Result<DetachedLogicalPlan, Error> {
        let ctx = self.datafusion_ctx()?;
        let plan = sql_to_plan(&ctx, query).await?;
        Ok(DetachedLogicalPlan(plan))
    }

    pub async fn plan_from_bytes(&self, bytes: &[u8]) -> Result<DetachedLogicalPlan, Error> {
        let ctx = self.datafusion_ctx()?;
        let plan = logical_plan_from_bytes_with_extension_codec(bytes, &ctx, &TableProviderCodec)
            .map_err(Error::PlanDecodingError)?;
        verify_plan(&plan)?;
        Ok(DetachedLogicalPlan(plan))
    }
}

#[derive(Clone, Debug)]
struct PlanningTable(ResolvedTable);

#[async_trait]
impl TableProvider for PlanningTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.0.schema().clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        unreachable!("PlanningTable should never be scanned")
    }
}
/// A plan that has `PlanningTable` for its `TableProvider`s. It cannot be executed before being
/// first "attached" to a `QueryContext`.
#[derive(Debug, Clone)]
pub struct DetachedLogicalPlan(LogicalPlan);

impl DetachedLogicalPlan {
    pub fn attach_to(self, ctx: &QueryContext) -> Result<LogicalPlan, Error> {
        use datafusion::common::tree_node::Transformed;

        Ok(self
            .0
            .transform(|mut node| match &mut node {
                // Insert the clauses in non-view table scans
                LogicalPlan::TableScan(TableScan {
                    table_name, source, ..
                }) if source.table_type() == TableType::Base
                    && source.get_logical_plan().is_none() =>
                {
                    let provider = ctx
                        .get_table(table_name)
                        .map_err(|e| DataFusionError::External(e.into()))?;
                    *source = Arc::new(DefaultTableSource::new(provider));
                    Ok(Transformed::yes(node))
                }
                _ => Ok(Transformed::no(node)),
            })
            .map_err(Error::PlanningError)?
            .data)
    }

    pub fn is_incremental(&self) -> Result<bool, BoxError> {
        is_incremental(&self.0)
    }

    pub fn schema(&self) -> DFSchemaRef {
        self.0.schema().clone()
    }

    pub fn propagate_block_num(self) -> Result<Self, DataFusionError> {
        Ok(Self(propagate_block_num(self.0)?))
    }

    pub fn order_by_block_num(self) -> Self {
        Self(order_by_block_num(self.0))
    }

    pub fn unproject_special_block_num_column(self) -> Result<Self, DataFusionError> {
        Ok(Self(unproject_special_block_num_column(self.0)?))
    }

    pub fn apply<'n, F>(&self, f: F) -> Result<TreeNodeRecursion, DataFusionError>
    where
        F: FnMut(&LogicalPlan) -> Result<TreeNodeRecursion, DataFusionError>,
    {
        self.0.apply(f)
    }
}

/// Handle to the environment resources used by the query engine.
#[derive(Clone, Debug)]
pub struct QueryEnv {
    pub df_env: Arc<RuntimeEnv>,
    pub isolate_pool: IsolatePool,
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
        let mut session_config = SessionConfig::from_env().map_err(Error::ConfigError)?;

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
        let catalog_snapshot = CatalogSnapshot::from_catalog(catalog, ignore_canonical_segments)
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

    /// Security: This function can receive arbitrary SQL, it will check and restrict the `query`.
    #[instrument(skip_all, err)]
    pub async fn execute_sql(&self, query: &str) -> Result<SendableRecordBatchStream, Error> {
        debug!("query: {}", query);

        let statement = parse_sql(query)?;
        let ctx = self.datafusion_ctx()?;
        let plan = sql_to_plan(&ctx, statement).await?;

        execute_plan(&ctx, plan, true).await
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
        let table_id = TableId {
            dataset: table_ref.schema().unwrap(),
            dataset_version: None,
            table: table_ref.table(),
        };
        self.catalog
            .table_snapshots()
            .iter()
            .find(|snapshot| snapshot.physical_table().table_id() == table_id)
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
        for table in extract_table_references_from_plan(&plan)? {
            range = match (range, self.get_synced_range_for_table(&table)?) {
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

    /// This will:
    /// - Validate that dependencies have synced the required block range.
    /// - Inject block range constraints into the plan.
    /// - Execute the plan.
    ///
    /// This assumes that the `_block_num` column has already been propagated and is therefore
    ///  present in the schema of `plan`.
    #[instrument(skip_all, err)]
    pub async fn execute_plan_for_range(
        &self,
        plan: LogicalPlan,
        start: BlockNum,
        end: BlockNum,
        preserve_block_num: bool,
        logical_optimize: bool,
    ) -> Result<SendableRecordBatchStream, BoxError> {
        let tables = extract_table_references_from_plan(&plan)?;

        // Validate dependency block ranges
        {
            for table in tables {
                let range = self.get_synced_range_for_table(&table)?;
                let synced = range.map(|r| end <= *r.end()).unwrap_or(false);
                if !synced {
                    return Err(format!(
                    "tried to query up to block {end} of table {table} but it has not been synced"
                )
                    .into());
                }
            }
        }

        let plan = {
            let mut plan = constrain_by_block_num(plan, start, end)?;
            if !preserve_block_num {
                plan = unproject_special_block_num_column(plan)?
            }
            plan
        };
        Ok(self.execute_plan(plan, logical_optimize).await?)
    }
}

#[instrument(skip_all, err)]
async fn sql_to_plan(ctx: &SessionContext, query: parser::Statement) -> Result<LogicalPlan, Error> {
    let plan = ctx
        .state()
        .statement_to_plan(query)
        .await
        .map_err(Error::PlanningError)?;
    verify_plan(&plan)?;
    Ok(plan)
}

fn verify_plan(plan: &LogicalPlan) -> Result<(), Error> {
    forbid_underscore_prefixed_aliases(&plan).map_err(Error::InvalidPlan)?;
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

fn create_catalog_schema(ctx: &SessionContext, schema_name: String) {
    // We only have use catalog, the default one.
    let catalog = ctx.catalog(&ctx.catalog_names()[0]).unwrap();
    if catalog.schema(&schema_name).is_none() {
        let schema = Arc::new(MemorySchemaProvider::new());
        catalog.register_schema(&schema_name, schema).unwrap();
    }
}

fn udfs() -> Vec<ScalarUDF> {
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

fn udafs() -> Vec<AggregateUDF> {
    vec![attestation::AttestationHasherUDF::new().into()]
}

pub fn parse_sql(sql: &str) -> Result<parser::Statement, Error> {
    let mut statements =
        parser::DFParser::parse_sql(sql).map_err(|e| Error::SqlParseError(e.into()))?;
    if statements.len() != 1 {
        return Err(Error::SqlParseError(
            format!(
                "a single SQL statement is expected, found {}",
                statements.len()
            )
            .into(),
        ));
    }
    let statement = statements.pop_back().unwrap();
    Ok(statement)
}

/// `logical_optimize` controls whether logical optimizations should be applied to `plan`.
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

// Sanitize the explain output by removing full paths and and keeping only the filenames.
fn sanitize_explain(batch: &RecordBatch) -> RecordBatch {
    use arrow::array::StringArray;

    let plan_idx = batch.schema().index_of("plan").unwrap();
    let plan_column = batch
        .column(plan_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    // Match full paths to .parquet files and capture just the filename
    // This handles paths with forward or backward slashes
    let regex = Regex::new(r"(?:[^\s\[,]+[/\\])?([^\s\[,/\\]+\.parquet)").unwrap();

    let transformed: StringArray = plan_column
        .iter()
        .map(|value| value.map(|v| regex.replace_all(v, "$1").into_owned()))
        .collect();

    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns[plan_idx] = Arc::new(transformed);

    RecordBatch::try_new(batch.schema(), columns).unwrap()
}

pub fn prepend_special_block_num_field(schema: &DFSchema) -> Arc<DFSchema> {
    let mut new_schema = DFSchema::from_unqualified_fields(
        Fields::from(vec![Field::new(SPECIAL_BLOCK_NUM, DataType::UInt64, false)]),
        Default::default(),
    )
    .unwrap();
    new_schema.merge(schema);
    new_schema.into()
}

/// Prints the physical plan to a single line, for logging.
fn print_physical_plan(plan: &dyn ExecutionPlan) -> String {
    displayable(plan)
        .indent(false)
        .to_string()
        .replace('\n', "\\n")
}

#[derive(Debug)]
pub struct TableProviderCodec;

impl LogicalExtensionCodec for TableProviderCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[LogicalPlan],
        _ctx: &SessionContext,
    ) -> Result<Extension, DataFusionError> {
        not_impl_err!("No extension codec provided")
    }

    fn try_encode(&self, _node: &Extension, _buf: &mut Vec<u8>) -> Result<(), DataFusionError> {
        not_impl_err!("No extension codec provided")
    }

    fn try_decode_table_provider(
        &self,
        _buf: &[u8],
        table_ref: &TableReference,
        _schema: SchemaRef,
        ctx: &SessionContext,
    ) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        // Unwrap: We use the default catalog and schema providers which are not async.
        ctx.table_provider(table_ref.clone())
            .now_or_never()
            .unwrap()
    }

    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
        _node: Arc<dyn TableProvider>,
        _buf: &mut Vec<u8>,
    ) -> Result<(), DataFusionError> {
        // No-op, the only thing we need for table scans is the table name.
        Ok(())
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
