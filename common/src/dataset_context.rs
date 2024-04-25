use std::{collections::HashMap, sync::Arc};

use anyhow::Context as _;
use datafusion::{
    common::{
        parsers::CompressionTypeVariant, Constraints, DFSchemaRef, OwnedTableReference,
        ToDFSchema as _,
    },
    error::DataFusionError,
    execution::{
        config::SessionConfig,
        context::{SQLOptions, SessionContext},
        disk_manager::DiskManagerConfig,
        runtime_env::{RuntimeConfig, RuntimeEnv},
        SendableRecordBatchStream,
    },
    logical_expr::{col, CreateExternalTable, DdlStatement, LogicalPlan},
    sql::TableReference,
};
use futures::StreamExt as _;
use object_store::{gcp::GoogleCloudStorageBuilder, local::LocalFileSystem, ObjectStore};
use thiserror::Error;
use url::Url;

use crate::{DataSet, Table, BLOCK_NUM};

#[derive(Error, Debug)]
pub enum Error {
    #[error("invalid plan: {0}")]
    InvalidPlan(DataFusionError),

    #[error("plan encoding error: {0}")]
    PlanEncodingError(DataFusionError),

    #[error("planning error: {0}")]
    PlanningError(DataFusionError),

    #[error("unsupported SQL: {0}")]
    UnsupportedSql(DataFusionError),

    /// Signals a problem with the dataset configuration.
    #[error("dataset error: {0}")]
    DatasetError(DataFusionError),
}

pub struct DatasetContext {
    env: Arc<RuntimeEnv>,
    dataset: DataSet,
    session_config: SessionConfig,

    // Physical location of the data.
    data_url: Url,

    // URLs for each table in the dataset, in a format understood by the object store.
    table_urls: HashMap<Table, Url>,
}

impl DatasetContext {
    /// Connects a dataset definition to exisiting data.
    ///
    /// Examples of valid formats for `data_location`:
    /// - Filesystem path: `relative/path/to/data/`
    /// - GCS: `gs://bucket-name/`
    pub async fn new(dataset: DataSet, data_location: String) -> Result<Self, anyhow::Error> {
        let (data_url, object_store) = infer_object_store(data_location)?;
        Self::with_object_store(dataset, data_url, object_store).await
    }

    pub async fn with_object_store(
        dataset: DataSet,
        data_url: Url,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self, anyhow::Error> {
        let env = RuntimeEnv::new(runtime_config())?;
        env.register_object_store(&data_url, object_store);

        // This contains various tuning options for the query engine.
        // Using `from_env` allows to tinker at runtime.
        let session_config = SessionConfig::from_env()?;

        let table_urls = {
            let mut table_urls = HashMap::new();
            for table in dataset.tables() {
                let url = if data_url.scheme() == "file" {
                    Url::from_directory_path(&format!("/{}/", &table.name)).unwrap()
                } else {
                    data_url.join(&format!("{}/", &table.name))?
                };
                table_urls.insert(table.clone(), url);
            }
            table_urls
        };

        let this = Self {
            env: Arc::new(env),
            dataset,
            session_config,
            data_url,
            table_urls,
        };

        // Do a 'dry run' to ensure the dataset is correctly configured.
        this.datafusion_ctx().await?;

        Ok(this)
    }

    /// Security: This function can receive arbitrary SQL, it will check and restrict the `query`.
    pub async fn execute_sql(&self, query: &str) -> Result<SendableRecordBatchStream, Error> {
        self.execute_plan(self.sql_to_plan(query).await?).await
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

    pub async fn sql_to_plan(&self, query: &str) -> Result<LogicalPlan, Error> {
        let ctx = self.datafusion_ctx().await?;
        let plan = ctx
            .state()
            .create_logical_plan(query)
            .await
            .map_err(Error::UnsupportedSql)?;
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

    pub async fn print_schema(&self) -> Result<Vec<(Table, String)>, Error> {
        use datafusion::arrow::util::pretty::pretty_format_batches;

        let mut output = vec![];
        for table in self.tables() {
            let mut record_stream = self
                .execute_sql(format!("describe {}", table.name).as_str())
                .await?;
            let Some(Ok(batch)) = record_stream.next().await else {
                return Err(Error::DatasetError(DataFusionError::Execution(format!(
                    "no schema for table `{}`",
                    table.name,
                ))));
            };
            let pretty_schema = pretty_format_batches(&[batch])
                .map_err(|e| Error::DatasetError(e.into()))?
                .to_string();

            // For readability, simplify somme common type names, using whitespace to keep the character width.
            let pretty_schema = pretty_schema.replace(
                r#"Timestamp(Nanosecond, Some("+00:00"))"#,
                r#"Timestamp                            "#,
            );
            let pretty_schema = pretty_schema.replace("Decimal128(38, 0)", "UInt128          ");

            output.push((table.clone(), pretty_schema));
        }
        Ok(output)
    }

    // Because `DatasetContext` is read-only, planning and execution can be done on ephemeral
    // sessions created by this function, and they will behave the same as if they had been run
    // against a persistent `SessionContext`
    async fn datafusion_ctx(&self) -> Result<SessionContext, Error> {
        let ctx = SessionContext::new_with_config_rt(self.session_config.clone(), self.env.clone());
        self.create_external_tables(&ctx)
            .await
            .map_err(Error::DatasetError)?;
        Ok(ctx)
    }

    async fn create_external_tables(&self, ctx: &SessionContext) -> Result<(), DataFusionError> {
        for (table, url) in &self.table_urls {
            // We will eventually wante namespacing, for referencing many datasets in a single query.
            let table_reference = TableReference::bare(table.name.clone());

            let schema = table.schema.as_ref().clone().to_dfschema_ref()?;

            let command = create_external_table(table_reference, schema, &url);
            ctx.execute_logical_plan(command).await?;
        }
        Ok(())
    }

    pub fn tables(&self) -> &[Table] {
        self.dataset.tables()
    }

    // Should never error unless there was a bug in the constructor.
    pub fn object_store(&self) -> Result<Arc<dyn ObjectStore>, DataFusionError> {
        self.env.object_store_registry.get_store(&self.data_url)
    }
}

fn runtime_config() -> RuntimeConfig {
    // TODO: Experiment with spill to disk and memory limits.
    // For now, spill to disk is disabled and memory is unbounded.
    let disk_manager = DiskManagerConfig::Disabled;
    let memory_pool = None;

    RuntimeConfig {
        disk_manager,
        memory_pool,
        ..Default::default()
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

pub fn infer_object_store(
    mut data_location: String,
) -> Result<(Url, Arc<dyn ObjectStore>), anyhow::Error> {
    // Make sure there is a trailing slash so it's recognized as a directory.
    if !data_location.ends_with('/') {
        data_location.push('/');
    }

    if data_location.starts_with("gs://") {
        let bucket = {
            let segment = data_location.trim_start_matches("gs://").split('/').next();
            segment.context("invalid GCS url")?
        };

        let store = Arc::new(
            GoogleCloudStorageBuilder::from_env()
                .with_bucket_name(bucket)
                .build()?,
        );
        Ok((Url::parse(&data_location)?, store))
    } else {
        let store = Arc::new(LocalFileSystem::new_with_prefix(&data_location)?);
        let path = format!("/{}", data_location);
        let url = Url::from_directory_path(path).unwrap();
        Ok((url, store))
    }
}

fn create_external_table(name: OwnedTableReference, schema: DFSchemaRef, url: &Url) -> LogicalPlan {
    let command = CreateExternalTable {
        // Assume parquet, which has native compression.
        file_type: "PARQUET".to_string(),
        file_compression_type: CompressionTypeVariant::UNCOMPRESSED,

        name,
        schema,
        location: url.to_string(),

        // TODO:
        // - Make this less hardcoded to handle non-blockchain data.
        // - Have a consistency check that the data really is sorted.
        // - Add other sorted columns that may be relevant such as `ordinal`.
        // - Do we want to address and leverage https://github.com/apache/arrow-datafusion/issues/4177?
        order_exprs: vec![
            vec![col(BLOCK_NUM).sort(true, false)],
            vec![col("timestamp").sort(true, false)],
        ],

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
