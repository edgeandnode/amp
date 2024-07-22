use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::util::pretty::pretty_format_batches;
use bytes::Bytes;
use datafusion::common::tree_node::{Transformed, TreeNode as _, TreeNodeRewriter};
use datafusion::common::{not_impl_err, DFSchema};
use datafusion::datasource::{DefaultTableSource, MemTable, TableProvider, TableType};
use datafusion::logical_expr::{
    CreateCatalogSchema, DmlStatement, Expr, Extension, LogicalPlanBuilder, ScalarUDF, TableScan,
    WriteOp,
};
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
use futures::StreamExt as _;
use thiserror::Error;
use url::Url;

use crate::catalog::physical::{Catalog, PhysicalTable};
use crate::evm::udfs::{EvmDecode, EvmTopic};
use crate::{arrow, BoxError, Store};

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

pub struct QueryContext {
    env: Arc<RuntimeEnv>,
    session_config: SessionConfig,
    catalog: Catalog,
}

impl QueryContext {
    pub fn empty(env: Arc<RuntimeEnv>) -> Result<Self, Error> {
        Self::for_catalog(Catalog::empty(), env)
    }

    pub fn for_catalog(catalog: Catalog, env: Arc<RuntimeEnv>) -> Result<Self, Error> {
        // This contains various tuning options for the query engine.
        // Using `from_env` allows tinkering without re-compiling.
        let mut session_config = SessionConfig::from_env().map_err(Error::ConfigError)?;

        let opts = session_config.options_mut();
        if std::env::var_os("DATAFUSION_OPTIMIZER_PREFER_EXISTING_SORT").is_none() {
            // Set `prefer_existing_sort` by default.
            opts.optimizer.prefer_existing_sort = true;
        }

        if std::env::var_os("DATAFUSION_EXECUTION_SPLIT_FILE_GROUPS_BY_STATISTICS").is_none() {
            // Set `split_file_groups_by_statistics` by default.
            //
            // Without this `prefer_existing_sort` will not be used with multiple files.
            // See https://github.com/apache/datafusion/issues/10336.
            opts.execution.split_file_groups_by_statistics = true;
        }

        if std::env::var_os("DATAFUSION_EXECUTION_COLLECT_STATISTICS").is_none() {
            // Set `collect_statistics` by default, so DataFusion eagerly reads and caches the
            // Parquet metadata statistics used for various optimizations.
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

    /// Infers the output schema of the query by planning it against empty tables
    pub async fn sql_output_schema(&self, query: parser::Statement) -> Result<DFSchemaRef, Error> {
        let ctx = self.datafusion_ctx_inner(false).await?;
        let plan = sql_to_plan(&ctx, query).await?;
        Ok(plan.schema().clone())
    }

    pub async fn plan_sql(&self, query: parser::Statement) -> Result<LogicalPlan, Error> {
        let ctx = self.datafusion_ctx().await?;
        let plan = sql_to_plan(&ctx, query).await?;
        Ok(plan)
    }

    /// Security: This function can receive arbitrary SQL, it will check and restrict the `query`.
    pub async fn execute_sql(&self, query: &str) -> Result<SendableRecordBatchStream, Error> {
        let statement = parse_sql(query)?;
        let ctx = self.datafusion_ctx().await?;
        let plan = sql_to_plan(&ctx, statement).await?;
        execute_plan(&ctx, plan).await
    }

    /// This will plan the query against empty tables, and then serialize that plan using
    /// datafusion-proto. This is useful for sending the plan to a remote server for execution.
    ///
    /// Returns the serialized plan and its output schema.
    pub async fn sql_to_remote_plan(
        &self,
        query: parser::Statement,
    ) -> Result<(Bytes, DFSchemaRef), Error> {
        let ctx = self.datafusion_ctx_inner(false).await?;
        let plan = sql_to_plan(&ctx, query).await?;
        let schema = plan.schema().clone();
        let serialized_plan = logical_plan_to_bytes_with_extension_codec(&plan, &EmptyTableCodec)
            .map_err(Error::PlanEncodingError)?;
        Ok((serialized_plan, schema))
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
        self.datafusion_ctx_inner(true).await
    }

    /// If `external_tables` is `false`, This will create empty in-memory tables, so the context will
    /// be able to plan queries but will be useless for executing them.
    async fn datafusion_ctx_inner(&self, external_tables: bool) -> Result<SessionContext, Error> {
        let ctx = SessionContext::new_with_config_rt(self.session_config.clone(), self.env.clone());

        for ds in self.catalog.datasets() {
            let ds_name = ds.name().to_string();
            create_dataset_tables(&ctx, ds_name, ds.data_store(), ds.tables(), external_tables)
                .await?;
        }

        for udf in udfs() {
            ctx.register_udf(udf);
        }

        Ok(ctx)
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

    async fn meta_datafusion_ctx(&self) -> Result<SessionContext, Error> {
        let ctx = SessionContext::new_with_config_rt(self.session_config.clone(), self.env.clone());
        for ds in self.catalog.datasets() {
            let ds_name = ds.name().to_string();
            create_dataset_tables(&ctx, ds_name, ds.data_store(), ds.meta_tables(), true).await?;
        }
        Ok(ctx)
    }

    /// Meta table queries are trusted and are expected to have small result sizes.
    pub async fn meta_execute_sql(&self, query: &str) -> Result<RecordBatch, Error> {
        let ctx = self.meta_datafusion_ctx().await?;
        let df = ctx.sql(query).await.map_err(Error::MetaTableError)?;
        let schema = SchemaRef::new(df.schema().into());
        let batches = df.collect().await.map_err(Error::MetaTableError)?;
        let batch = concat_batches(&schema, &batches).unwrap();
        Ok(batch)
    }

    pub async fn meta_execute_plan(&self, plan: LogicalPlan) -> Result<RecordBatch, Error> {
        let ctx = self.meta_datafusion_ctx().await?;
        let df = ctx
            .execute_logical_plan(plan)
            .await
            .map_err(Error::MetaTableError)?;
        let schema = SchemaRef::new(df.schema().into());
        let batches = df.collect().await.map_err(Error::MetaTableError)?;
        let batch = concat_batches(&schema, &batches).unwrap();
        Ok(batch)
    }

    pub async fn execute_plan(
        &self,
        plan: LogicalPlan,
    ) -> Result<SendableRecordBatchStream, Error> {
        let ctx = self.datafusion_ctx().await?;
        execute_plan(&ctx, plan).await
    }

    /// Insert rows into a metadata table.
    pub async fn meta_insert_into(
        &self,
        table_ref: TableReference,
        batch: RecordBatch,
    ) -> Result<(), Error> {
        let schema = batch.schema();
        let values = {
            // Unwrap: The schema is the batch schema.
            let mem_table = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
            let table_source = Arc::new(DefaultTableSource::new(Arc::new(mem_table)));

            // Unwrap: The scan is trivial.
            let table_scan =
                TableScan::try_new("temp_insert_input", table_source, None, vec![], None).unwrap();
            Arc::new(LogicalPlan::TableScan(table_scan))
        };

        // Unwrap: Not really fallible.
        let df_schema = schema.to_dfschema_ref().unwrap();
        let insert = DmlStatement::new(table_ref, df_schema, WriteOp::InsertInto, values);

        // Execute plan against meta ctx
        self.meta_execute_plan(LogicalPlan::Dml(insert)).await?;

        Ok(())
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

async fn create_dataset_tables(
    ctx: &SessionContext,
    dataset_name: String,
    dataset_store: Arc<Store>,
    tables: impl Iterator<Item = &PhysicalTable>,
    external_tables: bool,
) -> Result<(), Error> {
    // This may overwrite a previously registered store, but that should not make a difference.
    dataset_store.register_in(ctx);

    // The catalog schema needs to be explicitly created or table creation will fail.
    let create_schema_cmd = create_catalog_schema_cmd(dataset_name);
    ctx.execute_logical_plan(create_schema_cmd)
        .await
        .map_err(|e| Error::DatasetError(e.into()))?;
    for table in tables {
        create_table(ctx, table, external_tables)
            .await
            .map_err(|e| Error::DatasetError(e.into()))?;
    }
    Ok(())
}

async fn create_table(
    ctx: &SessionContext,
    table: &PhysicalTable,
    external: bool,
) -> Result<(), DataFusionError> {
    let table_ref = table.table_ref().clone();
    let schema = table.schema().to_dfschema_ref()?;
    if external {
        let cmd = create_external_table_cmd(table_ref, schema, &table.url(), table.order_exprs());
        ctx.execute_logical_plan(cmd).await?;
    } else {
        // Unwrap: Table is empty.
        let mem_table = MemTable::try_new(schema.as_ref().clone().into(), vec![vec![]]).unwrap();
        ctx.register_table(table_ref, Arc::new(mem_table))?;
    };
    Ok(())
}

fn create_external_table_cmd(
    name: TableReference,
    schema: DFSchemaRef,
    url: &Url,
    order_exprs: Vec<Vec<Expr>>,
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
    };

    LogicalPlan::Ddl(DdlStatement::CreateExternalTable(command))
}

fn create_catalog_schema_cmd(schema_name: String) -> LogicalPlan {
    let command = DdlStatement::CreateCatalogSchema(CreateCatalogSchema {
        schema_name,

        // Better to explicitly check duplicates.
        if_not_exists: false,

        // This 'schema' has no meaning.
        schema: Arc::new(DFSchema::empty()),
    });

    LogicalPlan::Ddl(command)
}

fn udfs() -> Vec<ScalarUDF> {
    vec![EvmDecode::new().into(), EvmTopic::new().into()]
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
    let physical_plan = ctx
        .state()
        .create_physical_plan(&plan)
        .await
        .map_err(Error::PlanningError)?;

    execute_stream(physical_plan, ctx.task_ctx()).map_err(Error::PlanningError)
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
