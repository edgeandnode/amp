use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::util::pretty::pretty_format_batches;
use async_udf::physical_optimizer::AsyncFuncRule;
use bincode::{config, Decode, Encode};
use bytes::Bytes;
use datafusion::common::tree_node::{Transformed, TreeNode as _, TreeNodeRewriter};
use datafusion::common::{not_impl_err, DFSchema};
use datafusion::datasource::{DefaultTableSource, MemTable, TableProvider, TableType};
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{
    AggregateUDF, CreateCatalogSchema, Extension, LogicalPlanBuilder, ScalarUDF, SortExpr,
    TableScan,
};
use datafusion::physical_plan::{displayable, ExecutionPlan};
use datafusion::sql::parser;
use datafusion::{
    common::{Constraints, DFSchemaRef, ToDFSchema as _},
    error::DataFusionError,
    execution::{
        config::SessionConfig,
        context::{SQLOptions, SessionContext},
        runtime_env::RuntimeEnv,
        SendableRecordBatchStream,
    },
    logical_expr::{CreateExternalTable, DdlStatement, LogicalPlan},
    sql::TableReference,
};
use datafusion_proto::bytes::{
    logical_plan_from_bytes_with_extension_codec, logical_plan_to_bytes_with_extension_codec,
};
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use futures::{StreamExt as _, TryStreamExt};
use thiserror::Error;
use tracing::{debug, instrument};
use url::Url;

use crate::catalog::physical::{Catalog, PhysicalTable};
use crate::evm::udfs::{EvmDecode, EvmDecodeFunctionData, EvmTopic};
use crate::{arrow, attestation, BoxError, Table};
use crate::stream_helpers::is_streaming;

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

#[derive(Clone, Debug)]
pub struct ResolvedTables {
    pub tables: Vec<ResolvedTable>,
    /// UDFs specific to the datasets corresponding to the resolved tables.
    pub udfs: Vec<ScalarUDF>,
}

#[derive(Clone, Debug)]
pub struct ResolvedTable {
    pub catalog_schema: String,
    pub table: Table,
    pub table_ref: TableReference,
}

impl ResolvedTable {
    pub fn new(catalog_schema: String, table: Table) -> Self {
        Self {
            catalog_schema: catalog_schema.clone(),
            table: table.clone(),
            table_ref: TableReference::partial(catalog_schema, table.name),
        }
    }
}

/// A context for planning SQL queries.
pub struct PlanningContext {
    session_config: SessionConfig,
    catalog: ResolvedTables,
}

// Serialized plan with additional options
#[derive(Encode, Decode)]
pub struct RemotePlan {
    pub serialized_plan: Vec<u8>,
    pub is_streaming: bool,
}

pub fn remote_plan_from_bytes(bytes: &Bytes) -> Result<RemotePlan, Error> {
    let (remote_plan, _) = bincode::decode_from_slice::<RemotePlan, _>(bytes, config::standard())
        .map_err(|e| Error::PlanEncodingError(
            DataFusionError::Plan(format!("Failed to serialize remote plan: {}", e))
        ))?;
    Ok(remote_plan)
}

impl PlanningContext {
    pub fn new(catalog: ResolvedTables) -> Self {
        Self {
            session_config: SessionConfig::from_env().unwrap(),
            catalog,
        }
    }

    /// Infers the output schema of the query by planning it against empty tables.
    pub async fn sql_output_schema(&self, query: parser::Statement) -> Result<DFSchemaRef, Error> {
        let ctx = self.datafusion_ctx().await?;
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
        let ctx = self.datafusion_ctx().await?;
        let plan = sql_to_plan(&ctx, query).await?;
        let schema = plan.schema().clone();
        let serialized_plan = logical_plan_to_bytes_with_extension_codec(&plan, &EmptyTableCodec)
            .map_err(Error::PlanEncodingError)?;
        let remote_plan = RemotePlan {
            serialized_plan: serialized_plan.to_vec(),
            is_streaming,
        };
        let serialized_plan = Bytes::from(
            bincode::encode_to_vec(remote_plan, config::standard())
                .map_err(|e| Error::PlanEncodingError(
                    DataFusionError::Plan(format!("Failed to serialize remote plan: {}", e))
                ))?
        );

        Ok((serialized_plan, schema))
    }

    async fn datafusion_ctx(&self) -> Result<SessionContext, Error> {
        let state = SessionStateBuilder::new()
            .with_config(self.session_config.clone())
            .with_runtime_env(Default::default())
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(AsyncFuncRule))
            .build();
        let ctx = SessionContext::new_with_state(state);
        create_empty_tables(&ctx, self.catalog.tables.iter()).await?;
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

/// A context for executing queries against a catalog.
pub struct QueryContext {
    env: Arc<RuntimeEnv>,
    session_config: SessionConfig,
    catalog: Catalog,
}

impl QueryContext {
    pub fn for_catalog(catalog: Catalog, env: Arc<RuntimeEnv>) -> Result<Self, Error> {
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
            opts.execution.split_file_groups_by_statistics = true;
        }

        // Set `parquet.pushdown_filters` by default.
        //
        // See https://github.com/apache/datafusion/issues/3463 for upstream default tracking.
        if std::env::var_os("DATAFUSION_EXECUTION_PARQUET_PUSHDOWN_FILTERS").is_none() {
            opts.execution.parquet.pushdown_filters = true;
        }

        if std::env::var_os("DATAFUSION_EXECUTION_COLLECT_STATISTICS").is_none() {
            // Set `collect_statistics` by default, so DataFusion eagerly reads and caches the
            // Parquet metadata statistics used for various optimizations.
            //
            // This is also a requirement for `split_file_groups_by_statistics` to work.
            opts.execution.collect_statistics = true;
        }

        let this = Self {
            env,
            session_config,
            catalog,
        };

        Ok(this)
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

    pub async fn rewrite_remote_plan(&self, plan: LogicalPlan) -> Result<LogicalPlan, Error> {
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

    /// A remote plan has empty tables, so this will attach the actual table providers from our
    /// catalog to the plan before executing it.
    pub async fn execute_remote_plan(
        &self,
        plan: LogicalPlan,
    ) -> Result<SendableRecordBatchStream, Error> {
        let ctx = self.datafusion_ctx().await?;
        let all_tables = all_tables(&ctx).await;
        let mut rewriter = AttachTablesToPlan {
            providers: all_tables,
        };
        let plan = plan
            .rewrite(&mut rewriter)
            .map_err(Error::PlanningError)?
            .data;

        execute_plan(&ctx, plan).await
    }

    /// Because `DatasetContext` is read-only, planning and execution can be done on ephemeral
    /// sessions created by this function, and they will behave the same as if they had been run
    /// against a persistent `SessionContext`
    async fn datafusion_ctx(&self) -> Result<SessionContext, Error> {
        let state = SessionStateBuilder::new()
            .with_config(self.session_config.clone())
            .with_runtime_env(self.env.clone())
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(AsyncFuncRule))
            .build();
        let ctx = SessionContext::new_with_state(state);

        for ds in self.catalog.datasets() {
            create_physical_tables(&ctx, ds.tables()).await?;
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

    pub async fn print_schema(&self) -> Result<Vec<(PhysicalTable, String)>, Error> {
        let mut output = vec![];
        for table in self.catalog.all_tables() {
            let mut record_stream = self
                .execute_sql(format!("describe {}", table.table_ref()).as_str())
                .await?;
            let Some(Ok(batch)) = record_stream.next().await else {
                return Err(Error::DatasetError(
                    format!("no schema for table `{}`", table.table_ref(),).into(),
                ));
            };
            let pretty_schema = pretty_format_batches(&[batch])
                .map_err(|e| Error::DatasetError(e.into()))?
                .to_string();

            // For readability, simplify somme common type names, using whitespace to keep the character width.
            let pretty_schema = pretty_schema.replace(
                r#"Timestamp(Nanosecond, Some("+00:00"))"#,
                r#"Timestamp                            "#,
            );
            let pretty_schema = pretty_schema.replace("Decimal128(38, 0)", "UInt126          ");
            let pretty_schema = pretty_schema.replace("FixedSizeBinary(32)", "Binary32           ");
            let pretty_schema = pretty_schema.replace("FixedSizeBinary(20)", "Binary20           ");

            output.push((table.clone(), pretty_schema));
        }
        Ok(output)
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
}

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
) -> Result<(), Error> {
    for table in tables {
        // The catalog schema needs to be explicitly created or table creation will fail.
        create_catalog_schema(ctx, table.catalog_schema.to_string())
            .await
            .map_err(|e| Error::DatasetError(e.into()))?;

        // Unwrap: Table is empty.
        let mem_table =
            MemTable::try_new(table.table.schema.as_ref().clone().into(), vec![vec![]]).unwrap();
        ctx.register_table(table.table_ref.clone(), Arc::new(mem_table))
            .map_err(|e| Error::DatasetError(e.into()))?;
    }
    Ok(())
}

async fn create_physical_tables(
    ctx: &SessionContext,
    tables: impl Iterator<Item = &PhysicalTable>,
) -> Result<(), Error> {
    for table in tables {
        // The catalog schema needs to be explicitly created or table creation will fail.
        create_catalog_schema(ctx, table.catalog_schema().to_string())
            .await
            .map_err(|e| Error::DatasetError(e.into()))?;

        create_physical_table(ctx, table)
            .await
            .map_err(|e| Error::DatasetError(e.into()))?;
    }
    Ok(())
}

async fn create_physical_table(
    ctx: &SessionContext,
    table: &PhysicalTable,
) -> Result<(), DataFusionError> {
    let table_ref = table.table_ref().clone();
    let schema = table.schema().to_dfschema_ref()?;

    // This may overwrite a previously registered store, but that should not make a difference.
    // The only segment of the `table.url()` that matters here is the schema and bucket name.
    ctx.register_object_store(table.url(), table.object_store());

    let cmd = create_external_table_cmd(table_ref, schema, &table.url(), table.order_exprs());
    ctx.execute_logical_plan(cmd).await?;

    Ok(())
}

fn create_external_table_cmd(
    name: TableReference,
    schema: DFSchemaRef,
    url: &Url,
    order_exprs: Vec<Vec<SortExpr>>,
) -> LogicalPlan {
    let command = CreateExternalTable {
        file_type: "PARQUET".to_string(),

        name,
        schema,
        location: url.to_string(),
        order_exprs,

        // Up to our preference, but maybe it's more robust for the caller to check duplicates.
        if_not_exists: false,

        // Things we don't currently use.
        table_partition_cols: vec![],
        options: Default::default(),
        constraints: Constraints::empty(),
        column_defaults: Default::default(),
        definition: None,
        unbounded: false,
        temporary: false,
    };

    LogicalPlan::Ddl(DdlStatement::CreateExternalTable(command))
}

async fn create_catalog_schema(ctx: &SessionContext, schema_name: String) -> Result<(), Error> {
    let command = DdlStatement::CreateCatalogSchema(CreateCatalogSchema {
        schema_name,

        // We call this for every table so we may create the schema multiple times.
        if_not_exists: true,

        // This 'schema' has no meaning.
        schema: Arc::new(DFSchema::empty()),
    });

    ctx.execute_logical_plan(LogicalPlan::Ddl(command))
        .await
        .map_err(|e| Error::DatasetError(e.into()))?;

    Ok(())
}

fn udfs() -> Vec<ScalarUDF> {
    vec![
        EvmDecode::new().into(),
        EvmTopic::new().into(),
        EvmDecodeFunctionData::evm_decode_params().into(),
        EvmDecodeFunctionData::evm_decode_results().into(),
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
