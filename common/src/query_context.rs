use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::util::pretty::pretty_format_batches;
use datafusion::logical_expr::{Expr, ScalarUDF};
use datafusion::sql::parser;
use datafusion::{
    common::{parsers::CompressionTypeVariant, Constraints, DFSchemaRef, ToDFSchema as _},
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
use futures::StreamExt as _;
use object_store::ObjectStore;
use thiserror::Error;
use url::Url;

use crate::catalog::physical::{Catalog, PhysicalTable};
use crate::evm::udfs::{EvmDecode, EvmTopic};
use crate::Table;
use crate::{arrow, BoxError};

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
}

pub struct QueryContext {
    env: Arc<RuntimeEnv>,
    session_config: SessionConfig,
    catalog: Catalog,
}

impl QueryContext {
    pub async fn for_catalog(catalog: Catalog, env: Arc<RuntimeEnv>) -> Result<Self, BoxError> {
        env.register_object_store(catalog.url(), catalog.object_store());

        // This contains various tuning options for the query engine.
        // Using `from_env` allows tinkering without re-compiling.
        let mut session_config = SessionConfig::from_env()?;

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

        // Do a 'dry run' to ensure the dataset is correctly configured.
        this.datafusion_ctx().await?;

        Ok(this)
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// Security: This function can receive arbitrary SQL, it will check and restrict the `query`.
    pub async fn execute_sql(&self, query: &str) -> Result<SendableRecordBatchStream, Error> {
        let statement = parse_sql(query).map_err(Error::SqlParseError)?;
        self.execute_plan(self.sql_to_plan(statement).await?).await
    }

    pub async fn execute_plan(
        &self,
        plan: LogicalPlan,
    ) -> Result<SendableRecordBatchStream, Error> {
        use datafusion::physical_plan::execute_stream;
        verify_plan(&plan)?;
        let ctx = self.datafusion_ctx().await?;
        let physical_plan = ctx
            .state()
            .create_physical_plan(&plan)
            .await
            .map_err(Error::PlanningError)?;

        execute_stream(physical_plan, ctx.task_ctx()).map_err(Error::PlanningError)
    }

    pub async fn sql_to_plan(&self, query: parser::Statement) -> Result<LogicalPlan, Error> {
        let ctx = self.datafusion_ctx().await?;
        let plan = ctx
            .state()
            .statement_to_plan(query)
            .await
            .map_err(Error::PlanningError)?;
        verify_plan(&plan)?;
        Ok(plan)
    }

    pub async fn plan_from_bytes(&self, bytes: &[u8]) -> Result<LogicalPlan, Error> {
        let ctx = self.datafusion_ctx().await?;
        let plan = datafusion_proto::bytes::logical_plan_from_bytes(bytes, &ctx)
            .map_err(Error::PlanEncodingError)?;
        verify_plan(&plan)?;
        Ok(plan)
    }

    // Because `DatasetContext` is read-only, planning and execution can be done on ephemeral
    // sessions created by this function, and they will behave the same as if they had been run
    // against a persistent `SessionContext`
    async fn datafusion_ctx(&self) -> Result<SessionContext, Error> {
        let ctx = SessionContext::new_with_config_rt(self.session_config.clone(), self.env.clone());
        for table in self.catalog.tables() {
            create_external_table(&ctx, table)
                .await
                .map_err(|e| Error::DatasetError(e.into()))?;
        }
        for udf in udfs() {
            ctx.register_udf(udf);
        }
        Ok(ctx)
    }

    pub fn tables(&self) -> impl Iterator<Item = &Table> {
        self.catalog.tables().map(|t| &t.table)
    }

    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        // Unwrap: This was registered in the constructor.
        let url = self.catalog.url();
        self.env.object_store_registry.get_store(url).unwrap()
    }

    pub async fn print_schema(&self) -> Result<Vec<(Table, String)>, Error> {
        let mut output = vec![];
        for table in self.tables() {
            let mut record_stream = self
                .execute_sql(format!("describe {}", table.name).as_str())
                .await?;
            let Some(Ok(batch)) = record_stream.next().await else {
                return Err(Error::DatasetError(
                    format!("no schema for table `{}`", table.name,).into(),
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
        for table in self.catalog.meta_tables() {
            create_external_table(&ctx, table)
                .await
                .map_err(|e| Error::DatasetError(e.into()))?;
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

        // Note: This is a workaround. Replace with:
        // `let batch = concat_batches(&schema, &batches).unwrap();`
        // After https://github.com/apache/datafusion/pull/10394 is merged and released.
        let batch = if batches.is_empty() {
            RecordBatch::new_empty(schema)
        } else {
            concat_batches(&batches[0].schema(), &batches).unwrap()
        };
        Ok(batch)
    }
}

fn verify_plan(plan: &LogicalPlan) -> Result<(), Error> {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
        .verify_plan(plan)
        .map_err(Error::InvalidPlan)
}

async fn create_external_table(
    ctx: &SessionContext,
    table: &PhysicalTable,
) -> Result<(), DataFusionError> {
    // We will eventually want namespacing, for referencing many datasets in a single query.
    let table_reference = TableReference::bare(table.name().to_string());
    let schema = table.table.schema.clone().to_dfschema_ref()?;
    let command = create_external_table_cmd(
        table_reference,
        schema,
        &table.url,
        table.table.order_exprs(),
    );
    ctx.execute_logical_plan(command).await?;
    Ok(())
}

fn create_external_table_cmd(
    name: TableReference,
    schema: DFSchemaRef,
    url: &Url,
    order_exprs: Vec<Vec<Expr>>,
) -> LogicalPlan {
    let command = CreateExternalTable {
        // Assume parquet, which has native compression.
        file_type: "PARQUET".to_string(),
        file_compression_type: CompressionTypeVariant::UNCOMPRESSED,

        name,
        schema,
        location: url.to_string(),
        order_exprs,

        // Up to our preference, but maybe it's more robust for the caller to check existence.
        if_not_exists: false,

        // Wen streaming?
        unbounded: false,

        // Things we don't currently use.
        table_partition_cols: vec![],
        options: Default::default(),
        constraints: Constraints::empty(),
        column_defaults: Default::default(),
        definition: None,

        // CSV specific
        has_header: false,
        delimiter: ',',
    };

    LogicalPlan::Ddl(DdlStatement::CreateExternalTable(command))
}

fn udfs() -> Vec<ScalarUDF> {
    vec![EvmDecode::new().into(), EvmTopic::new().into()]
}

pub fn parse_sql(sql: &str) -> Result<parser::Statement, BoxError> {
    let mut statements = parser::DFParser::parse_sql(sql)?;
    if statements.len() != 1 {
        return Err(format!(
            "a single SQL statement is expected, found {}",
            statements.len()
        )
        .into());
    }
    let statement = statements.pop_back().unwrap();
    Ok(statement)
}
