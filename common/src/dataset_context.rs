use std::sync::Arc;

use crate::arrow;
use crate::config::Config;
use anyhow::anyhow;
use anyhow::Context as _;
use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::util::pretty::pretty_format_batches;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::logical_expr::Expr;
use datafusion::{
    common::{parsers::CompressionTypeVariant, Constraints, DFSchemaRef, ToDFSchema as _},
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
use object_store::{
    aws::AmazonS3Builder, gcp::GoogleCloudStorageBuilder, local::LocalFileSystem, ObjectStore,
};
use thiserror::Error;
use url::Url;

use crate::meta_tables;
use crate::{Dataset, Table, BLOCK_NUM};

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

    #[error("query execution error: {0}")]
    ExecutionError(DataFusionError),

    /// Signals a problem with the dataset configuration.
    #[error("dataset error: {0:#}")]
    DatasetError(anyhow::Error),

    #[error("meta table error: {0}")]
    MetaTableError(DataFusionError),
}

struct TableUrl {
    table: Table,
    url: Url,

    // Physical ordering of the table at this location.
    order_exprs: Vec<Vec<Expr>>,
}

impl TableUrl {
    fn resolve(
        base: &Url,
        table: &Table,
        order_exprs: Vec<Vec<Expr>>,
    ) -> Result<TableUrl, anyhow::Error> {
        let url = if base.scheme() == "file" {
            Url::from_directory_path(&format!("/{}/", &table.name)).unwrap()
        } else {
            base.join(&format!("{}/", &table.name))?
        };
        Ok(TableUrl {
            table: table.clone(),
            url,
            order_exprs,
        })
    }
}

struct DatasetLocation {
    dataset: Dataset,

    // Physical location of the data.
    data_url: Url,

    // URLs for each table in the dataset, in a format understood by the object store.
    table_urls: Vec<TableUrl>,

    // Metadata tables. These are not part of a dataset and not queryable.
    meta_table_urls: Vec<TableUrl>,
}

pub struct DatasetContext {
    env: Arc<RuntimeEnv>,
    session_config: SessionConfig,
    dataset_location: DatasetLocation,
}

impl DatasetContext {
    /// Connects a dataset definition to exisiting data.
    ///
    /// Examples of valid formats for `data_location`:
    /// - Filesystem path: `relative/path/to/data/`
    /// - GCS: `gs://bucket-name/`
    /// - S3: `s3://bucket-name/`
    pub async fn new(dataset: Dataset, config: &Config) -> Result<Self, anyhow::Error> {
        let (data_url, object_store) = infer_object_store(config.data_location.clone())?;
        let meta = meta_tables::tables();
        Self::with_object_store(config, dataset, meta, data_url, object_store).await
    }

    pub async fn with_object_store(
        config: &Config,
        dataset: Dataset,
        meta: Vec<Table>,
        data_url: Url,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self, anyhow::Error> {
        let env = RuntimeEnv::new(runtime_config(config))?;
        env.register_object_store(&data_url, object_store);

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

        // Leveraging `order_exprs` can optimize away sorting for many query plans.
        // TODO:
        // - Make this less hardcoded to handle non-blockchain data.
        // - Have a consistency check that the data really is sorted.
        // - Do we want to address and leverage https://github.com/apache/arrow-datafusion/issues/4177?
        let order_exprs = vec![
            vec![col(BLOCK_NUM).sort(true, false)],
            vec![col("timestamp").sort(true, false)],
        ];
        let table_urls = dataset
            .tables()
            .iter()
            .map(|table| TableUrl::resolve(&data_url, table, order_exprs.clone()))
            .collect::<Result<Vec<_>, _>>()?;

        let meta_table_urls = meta
            .iter()
            .map(|table| TableUrl::resolve(&data_url, table, vec![]))
            .collect::<Result<Vec<_>, _>>()?;

        let dataset_location = DatasetLocation {
            dataset,
            data_url,
            table_urls,
            meta_table_urls,
        };

        let this = Self {
            env: Arc::new(env),
            dataset_location,
            session_config,
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

    // Because `DatasetContext` is read-only, planning and execution can be done on ephemeral
    // sessions created by this function, and they will behave the same as if they had been run
    // against a persistent `SessionContext`
    async fn datafusion_ctx(&self) -> Result<SessionContext, Error> {
        let mut ctx =
            SessionContext::new_with_config_rt(self.session_config.clone(), self.env.clone());
        create_external_tables(&ctx, &self.dataset_location.table_urls)
            .await
            .map_err(|e| Error::DatasetError(e.into()))?;
        self.dataset_location.dataset.register(&mut ctx);
        Ok(ctx)
    }

    pub fn tables(&self) -> &[Table] {
        self.dataset().tables()
    }

    // Should never error unless there was a bug in the constructor.
    pub fn object_store(&self) -> Result<Arc<dyn ObjectStore>, DataFusionError> {
        self.env
            .object_store_registry
            .get_store(&self.dataset_location.data_url)
    }

    pub fn dataset(&self) -> &Dataset {
        &self.dataset_location.dataset
    }

    pub async fn print_schema(&self) -> Result<Vec<(Table, String)>, Error> {
        let mut output = vec![];
        for table in self.tables() {
            let mut record_stream = self
                .execute_sql(format!("describe {}", table.name).as_str())
                .await?;
            let Some(Ok(batch)) = record_stream.next().await else {
                return Err(Error::DatasetError(anyhow!(
                    "no schema for table `{}`",
                    table.name,
                )));
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

    async fn meta_datafusion_ctx(&self) -> Result<SessionContext, Error> {
        let ctx = SessionContext::new_with_config_rt(self.session_config.clone(), self.env.clone());
        create_external_tables(&ctx, &self.dataset_location.meta_table_urls)
            .await
            .map_err(|e| Error::DatasetError(e.into()))?;
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

fn runtime_config(config: &Config) -> RuntimeConfig {
    use datafusion::execution::cache::{
        cache_manager::CacheManagerConfig, cache_unit::DefaultFileStatisticsCache,
    };

    let disk_manager = if config.spill_location.is_empty() {
        DiskManagerConfig::Disabled
    } else {
        DiskManagerConfig::NewSpecified(config.spill_location.clone())
    };
    let memory_pool: Option<Arc<dyn MemoryPool>> = if config.max_mem > 0 {
        Some(Arc::new(FairSpillPool::new(config.max_mem)))
    } else {
        None
    };
    let cache_manager = CacheManagerConfig {
        // Caches parquet file statistics. Seems like a good thing.
        table_files_statistics_cache: Some(Arc::new(DefaultFileStatisticsCache::default())),
        // Seems it might lead to staleness in the ListingTable, better not.
        list_files_cache: None,
    };

    RuntimeConfig {
        disk_manager,
        memory_pool,
        cache_manager,
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
    } else if data_location.starts_with("s3://") {
        let bucket = {
            let segment = data_location.trim_start_matches("s3://").split('/').next();
            segment.context("invalid S3 url")?
        };

        let store = Arc::new(
            AmazonS3Builder::from_env()
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

async fn create_external_tables(
    ctx: &SessionContext,
    table_urls: &[TableUrl],
) -> Result<(), DataFusionError> {
    for table_url in table_urls {
        // We will eventually wante namespacing, for referencing many datasets in a single query.
        let table_reference = TableReference::bare(table_url.table.name.clone());
        let schema = table_url.table.schema.clone().to_dfschema_ref()?;
        let command = create_external_table(
            table_reference,
            schema,
            &table_url.url,
            table_url.order_exprs.clone(),
        );
        ctx.execute_logical_plan(command).await?;
    }
    Ok(())
}

fn create_external_table(
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
