use std::{collections::BTreeMap, iter, sync::Arc};

use arrow::{array::RecordBatch, compute::concat_batches, datatypes::SchemaRef};
use async_udf::physical_optimizer::AsyncFuncRule;
use bincode::{config, Decode, Encode};
use bytes::Bytes;
use datafusion::{
    arrow::datatypes::{DataType, Field, Fields, Schema},
    catalog::MemorySchemaProvider,
    common::{
        not_impl_err, plan_err,
        tree_node::{
            Transformed, TransformedResult, TreeNode as _, TreeNodeRecursion, TreeNodeRewriter,
        },
        Column, DFSchema, DFSchemaRef,
    },
    datasource::{DefaultTableSource, MemTable, TableProvider, TableType},
    error::DataFusionError,
    execution::{
        config::SessionConfig,
        context::{SQLOptions, SessionContext},
        runtime_env::RuntimeEnv,
        SendableRecordBatchStream, SessionStateBuilder,
    },
    logical_expr::{
        AggregateUDF, Extension, LogicalPlan, LogicalPlanBuilder, Projection, ScalarUDF, TableScan,
    },
    physical_plan::{displayable, ExecutionPlan},
    prelude::{col, Expr},
    sql::{parser, TableReference},
};
use datafusion_proto::{
    bytes::{
        logical_plan_from_bytes_with_extension_codec, logical_plan_to_bytes_with_extension_codec,
    },
    logical_plan::LogicalExtensionCodec,
};
use futures::TryStreamExt;
use js_runtime::isolate_pool::IsolatePool;
use metadata_db::TableId;
use thiserror::Error;
use tracing::{debug, instrument};

use crate::{
    arrow, attestation,
    catalog::physical::{Catalog, PhysicalTable},
    evm::udfs::{
        EvmDecodeLog, EvmDecodeParams, EvmDecodeType, EvmEncodeParams, EvmEncodeType, EvmTopic,
    },
    internal,
    stream_helpers::is_streaming,
    BoxError, LogicalCatalog, ResolvedTable, BLOCK_NUM, SPECIAL_BLOCK_NUM,
};

#[derive(Error, Debug)]
pub enum Error {
    #[error("invalid plan: {0}")]
    InvalidPlan(DataFusionError),

    #[error("plan encoding error: {0}")]
    PlanEncodingError(DataFusionError),

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
}

pub fn remote_plan_from_bytes(bytes: &Bytes) -> Result<RemotePlan, Error> {
    let (remote_plan, _) = bincode::decode_from_slice(bytes, config::standard()).map_err(|e| {
        Error::PlanEncodingError(DataFusionError::Plan(format!(
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
    pub async fn sql_output_schema(
        &self,
        query: parser::Statement,
        additional_fields: &[Field],
    ) -> Result<DFSchemaRef, Error> {
        let ctx = self.datafusion_ctx(additional_fields).await?;
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
        let ctx = self
            .datafusion_ctx(&[Field::new(SPECIAL_BLOCK_NUM, DataType::UInt64, false)])
            .await?;
        let plan = sql_to_plan(&ctx, query).await?;
        let schema = plan.schema().clone();
        let serialized_plan = logical_plan_to_bytes_with_extension_codec(&plan, &EmptyTableCodec)
            .map_err(Error::PlanEncodingError)?;
        let remote_plan = RemotePlan {
            serialized_plan: serialized_plan.to_vec(),
            is_streaming,
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

    async fn datafusion_ctx(&self, additional_fields: &[Field]) -> Result<SessionContext, Error> {
        let state = SessionStateBuilder::new()
            .with_config(self.session_config.clone())
            .with_runtime_env(Default::default())
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(AsyncFuncRule))
            .build();
        let ctx = SessionContext::new_with_state(state);
        create_empty_tables(&ctx, self.catalog.tables.iter(), additional_fields).await?;
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
}

/// Handle to the environment resources used by the query engine.
#[derive(Clone, Debug)]
pub struct QueryEnv {
    pub df_env: Arc<RuntimeEnv>,
    pub isolate_pool: IsolatePool,
}

/// A context for executing queries against a catalog.
pub struct QueryContext {
    env: QueryEnv,
    session_config: SessionConfig,
    catalog: Catalog,
}

impl QueryContext {
    pub fn for_catalog(catalog: Catalog, env: QueryEnv) -> Result<Self, Error> {
        // This contains various tuning options for the query engine.
        // Using `from_env` allows tinkering without re-compiling.
        let mut session_config = SessionConfig::from_env().map_err(Error::ConfigError)?;

        let opts = session_config.options_mut();

        // Rationale for DataFusion settings:
        //
        // `collect_statistics`, `prefer_existing_sort` and `split_file_groups_by_statistics` all
        // work together to take advantage of our files being time-partitioned and each file having
        // the rows written in sorted order.
        //
        // `pushdown_filters` should be helpful for very selective queries, which is something we
        // want to optimize for.

        // Set `prefer_existing_sort` by default.
        if std::env::var_os("DATAFUSION_OPTIMIZER_PREFER_EXISTING_SORT").is_none() {
            opts.optimizer.prefer_existing_sort = true;
        }

        // Set `split_file_groups_by_statistics` by default.
        //
        // See https://github.com/apache/datafusion/issues/10336 for upstream default tracking.
        if std::env::var_os("DATAFUSION_EXECUTION_SPLIT_FILE_GROUPS_BY_STATISTICS").is_none() {
            opts.execution.split_file_groups_by_statistics = false;
        }

        // Set `parquet.pushdown_filters` by default.
        //
        // See https://github.com/apache/datafusion/issues/3463 for upstream default tracking.
        if std::env::var_os("DATAFUSION_EXECUTION_PARQUET_PUSHDOWN_FILTERS").is_none() {
            opts.execution.parquet.pushdown_filters = false;
        }

        if std::env::var_os("DATAFUSION_EXECUTION_COLLECT_STATISTICS").is_none() {
            // Set `collect_statistics` by default, so DataFusion eagerly reads and caches the
            // Parquet metadata statistics used for various optimizations.
            //
            // This is also a requirement for `split_file_groups_by_statistics` to work.
            opts.execution.collect_statistics = true;
        }

        Ok(Self {
            env,
            session_config,
            catalog,
        })
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub async fn plan_sql(&self, query: parser::Statement) -> Result<LogicalPlan, Error> {
        let ctx = self.datafusion_ctx().await?;
        let plan = sql_to_plan(&ctx, query).await?;
        Ok(plan)
    }

    /// Security: This function can receive arbitrary SQL, it will check and restrict the `query`.
    #[instrument(skip_all, err)]
    pub async fn execute_sql(&self, query: &str) -> Result<SendableRecordBatchStream, Error> {
        debug!("query: {}", query);

        let statement = parse_sql(query)?;
        let ctx = self.datafusion_ctx().await?;
        let plan = sql_to_plan(&ctx, statement).await?;

        execute_plan(&ctx, plan).await
    }

    /// This will deserialize the plan with empty tables in the `TableScan` nodes.
    pub async fn plan_from_bytes(&self, bytes: &[u8]) -> Result<LogicalPlan, Error> {
        let ctx = self.datafusion_ctx().await?;
        let plan = logical_plan_from_bytes_with_extension_codec(bytes, &ctx, &EmptyTableCodec)
            .map_err(Error::PlanEncodingError)?;
        verify_plan(&plan)?;
        Ok(plan)
    }

    /// A remote plan has empty tables, so this will attach the actual table providers from our
    /// catalog to the plan before executing it.
    pub async fn prepare_remote_plan(&self, plan: LogicalPlan) -> Result<LogicalPlan, Error> {
        let ctx = self.datafusion_ctx().await?;
        let all_tables = all_tables(&ctx).await;
        let mut rewriter = AttachTablesToPlan {
            providers: all_tables,
        };
        let plan = plan
            .rewrite(&mut rewriter)
            .map_err(Error::PlanningError)?
            .data;

        Ok(plan)
    }

    /// Because `DatasetContext` is read-only, planning and execution can be done on ephemeral
    /// sessions created by this function, and they will behave the same as if they had been run
    /// against a persistent `SessionContext`
    async fn datafusion_ctx(&self) -> Result<SessionContext, Error> {
        let state = SessionStateBuilder::new()
            .with_config(self.session_config.clone())
            .with_runtime_env(self.env.df_env.clone())
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(AsyncFuncRule))
            .build();
        let ctx = SessionContext::new_with_state(state);

        for table in self.catalog.tables() {
            create_physical_table(&ctx, table.clone())
                .await
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
        for udf in self.catalog.udfs() {
            ctx.register_udf(udf.clone());
        }
    }

    pub async fn execute_plan(
        &self,
        plan: LogicalPlan,
    ) -> Result<SendableRecordBatchStream, Error> {
        let ctx = self.datafusion_ctx().await?;
        execute_plan(&ctx, plan).await
    }

    /// This will load the result set entirely in memory, so it should be used with caution.
    pub async fn execute_and_concat(&self, plan: LogicalPlan) -> Result<RecordBatch, Error> {
        let schema = plan.schema().inner().clone();
        let ctx = self.datafusion_ctx().await?;
        let batch_stream = execute_plan(&ctx, plan)
            .await?
            .try_collect::<Vec<_>>()
            .await
            .map_err(Error::ExecutionError)?;
        Ok(concat_batches(&schema, &batch_stream).unwrap())
    }

    pub fn get_table(&self, table_ref: &TableReference) -> Option<Arc<PhysicalTable>> {
        let table_id = TableId {
            dataset: table_ref.schema().unwrap(),
            dataset_version: None,
            table: table_ref.table(),
        };

        self.catalog
            .tables()
            .iter()
            .find(|table| table.table_id() == table_id)
            .cloned()
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
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
        .verify_plan(plan)
        .map_err(Error::InvalidPlan)
}

async fn create_empty_tables(
    ctx: &SessionContext,
    tables: impl Iterator<Item = &ResolvedTable>,
    additional_fields: &[Field],
) -> Result<(), Error> {
    for table in tables {
        // The catalog schema needs to be explicitly created or table creation will fail.
        create_catalog_schema(ctx, table.catalog_schema().to_string());

        // Unwrap: Table is empty.
        let schema = Schema::try_merge([
            Schema::new(additional_fields.to_vec()),
            table.schema().as_ref().clone(),
        ])
        .unwrap();
        let mem_table = MemTable::try_new(schema.into(), vec![vec![]]).unwrap();
        ctx.register_table(table.table_ref().clone(), Arc::new(mem_table))
            .map_err(|e| Error::DatasetError(e.into()))?;
    }
    Ok(())
}

async fn create_physical_table(
    ctx: &SessionContext,
    provider: Arc<PhysicalTable>,
) -> Result<(), DataFusionError> {
    // The catalog schema needs to be explicitly created or table creation will fail.
    create_catalog_schema(ctx, provider.catalog_schema().to_string());

    let table_ref = provider.table_ref().clone();

    // This may overwrite a previously registered store, but that should not make a difference.
    // The only segment of the `table.url()` that matters here is the schema and bucket name.
    ctx.register_object_store(provider.url(), provider.object_store());

    ctx.register_table(table_ref, provider)?;

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

async fn execute_plan(
    ctx: &SessionContext,
    plan: LogicalPlan,
) -> Result<SendableRecordBatchStream, Error> {
    use datafusion::physical_plan::execute_stream;

    verify_plan(&plan)?;
    debug!("logical plan: {}", plan.to_string().replace('\n', "\\n"));

    let physical_plan = ctx
        .state()
        .create_physical_plan(&plan)
        .await
        .map_err(Error::PlanningError)?;
    debug!("physical plan: {}", print_physical_plan(&*physical_plan));

    execute_stream(physical_plan, ctx.task_ctx()).map_err(Error::PlanningError)
}

/// Aliases with a name starting with `_` are always forbidden, since underscore-prefixed
/// names are reserved for special columns.
pub fn forbid_underscore_prefixed_aliases(plan: &LogicalPlan) -> Result<(), DataFusionError> {
    plan.apply(|node| {
        match node {
            LogicalPlan::Projection(projection) => {
                for expr in projection.expr.iter() {
                    if let Expr::Alias(alias) = expr {
                        if alias.name.starts_with('_') {
                            return plan_err!(
                                "projection contains a column alias starting with '_': '{}'. Underscore-prefixed names are reserved. Please rename your column",
                                alias.name
                            );
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

/// Propagate the `SPECIAL_BLOCK_NUM` column through the logical plan.
pub fn propagate_block_num(plan: LogicalPlan) -> Result<LogicalPlan, DataFusionError> {
    plan.transform(|node| {
        match node {
            LogicalPlan::Projection(mut projection) => {
                // If the projection already selects the `SPECIAL_BLOCK_NUM` column directly, we don't need to
                // add that column, but we do have to ensure that the `SPECIAL_BLOCK_NUM` is not
                // qualified with a table name (since it does not necessarily belong to a table).
                for expr in projection.expr.iter_mut() {
                    if let Expr::Column(c) = expr {
                        if c.name() == SPECIAL_BLOCK_NUM {
                            // Unqualify the column.
                            c.relation = None;
                            return Ok(Transformed::yes(LogicalPlan::Projection(projection)));
                        }
                    }
                }

                // Prepend `SPECIAL_BLOCK_NUM` column to the projection.
                projection.expr.insert(0, col(SPECIAL_BLOCK_NUM));
                projection.schema = prepend_special_block_num_field(&projection.schema);
                Ok(Transformed::yes(LogicalPlan::Projection(projection)))
            }
            LogicalPlan::TableScan(table_scan) => {
                if table_scan
                    .source
                    .schema()
                    .fields()
                    .iter()
                    .any(|f| f.name() == SPECIAL_BLOCK_NUM)
                {
                    // If the table already has a `SPECIAL_BLOCK_NUM` column, we don't need to add it.
                    Ok(Transformed::no(LogicalPlan::TableScan(table_scan)))
                } else {
                    // Add a projection on top of the TableScan to select the `BLOCK_NUM` column
                    // as `SPECIAL_BLOCK_NUM`.
                    let schema = prepend_special_block_num_field(&table_scan.projected_schema);
                    let col_exprs = iter::once(col(BLOCK_NUM).alias(SPECIAL_BLOCK_NUM)).chain(
                        table_scan.projected_schema.fields().iter().map(|f| {
                            Expr::Column(Column {
                                relation: Some(table_scan.table_name.clone()),
                                name: f.name().to_string(),
                                spans: Default::default(),
                            })
                        }),
                    );
                    let projection = Projection::try_new_with_schema(
                        col_exprs.collect(),
                        Arc::new(LogicalPlan::TableScan(table_scan)),
                        schema,
                    )?;
                    Ok(Transformed::yes(LogicalPlan::Projection(projection)))
                }
            }
            LogicalPlan::Union(mut union) => {
                // Add the `SPECIAL_BLOCK_NUM` column to the union schema.
                union.schema = prepend_special_block_num_field(&union.schema);
                Ok(Transformed::yes(LogicalPlan::Union(union)))
            }
            _ => Ok(Transformed::no(node)),
        }
    })
    .data()
}

/// If the original query does not select the `SPECIAL_BLOCK_NUM` column, this will
/// project the `SPECIAL_BLOCK_NUM` out of the plan. (Essentially, add a projection
/// on top of the query which selects all columns except `SPECIAL_BLOCK_NUM`.)
pub fn unproject_special_block_num_column(
    plan: LogicalPlan,
    original_schema: Arc<DFSchema>,
) -> Result<LogicalPlan, DataFusionError> {
    if original_schema
        .fields()
        .iter()
        .any(|f| f.name() == SPECIAL_BLOCK_NUM)
    {
        // If the original schema already contains the `SPECIAL_BLOCK_NUM` column, we don't need to
        // project it out.
        return Ok(plan);
    }

    let exprs = original_schema
        .fields()
        .iter()
        .map(|f| col(f.name()))
        .collect();
    let projection = Projection::try_new_with_schema(exprs, Arc::new(plan), original_schema)
        .map_err(|e| {
            internal!(
                "error while removing {} from projection: {}",
                SPECIAL_BLOCK_NUM,
                e
            )
        })?;
    Ok(LogicalPlan::Projection(projection))
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

/// Replaces placeholder table providers coming from a deserialized plan with the actual providers
/// registered in a local session context.
struct AttachTablesToPlan {
    providers: BTreeMap<TableReference, Arc<dyn TableProvider + 'static>>,
}

impl TreeNodeRewriter for AttachTablesToPlan {
    type Node = LogicalPlan;
    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        match node {
            // Look for table scans that are not view references, and replace their provider with the
            // one registered in the local context.
            LogicalPlan::TableScan(TableScan {
                table_name, source, ..
            }) if source.table_type() == TableType::Base && source.get_logical_plan().is_none() => {
                let provider = self.providers.get(&table_name).ok_or_else(|| {
                    DataFusionError::External(
                        format!("table {table_name} is not registered in the context").into(),
                    )
                })?;
                let new_source = Arc::new(DefaultTableSource::new(provider.clone()));
                Ok(Transformed::yes(
                    LogicalPlanBuilder::scan(table_name, new_source, None)?.build()?,
                ))
            }
            _ => Ok(Transformed::no(node)),
        }
    }
}

async fn all_tables(ctx: &SessionContext) -> BTreeMap<TableReference, Arc<dyn TableProvider>> {
    let mut tables = BTreeMap::new();

    // Unwrap: We always have a single catalog, the default one.
    let catalog = ctx.catalog(&ctx.catalog_names()[0]).unwrap();
    for catalog_schema in catalog.schema_names() {
        // Unwrap: This was taken from `schema_names`.
        let schema_provider = catalog.schema(&catalog_schema).unwrap();
        for table_name in schema_provider.table_names() {
            // Unwrap: This was taken from `table_names`.
            let table_provider = schema_provider.table(&table_name).await.unwrap().unwrap();
            let table_ref = TableReference::partial(catalog_schema.clone(), table_name);
            tables.insert(table_ref, table_provider);
        }
    }

    tables
}

#[derive(Debug)]
pub struct EmptyTableCodec;

impl LogicalExtensionCodec for EmptyTableCodec {
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
        _table_ref: &TableReference,
        schema: SchemaRef,
        _ctx: &SessionContext,
    ) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        let table = MemTable::try_new(schema, vec![vec![]])?;
        Ok(Arc::new(table))
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
