use std::{
    io::ErrorKind,
    path::PathBuf,
    process::{ExitStatus, Stdio},
    str::FromStr as _,
    sync::Arc,
    time::Duration,
};

use arrow_flight::{
    flight_service_client::FlightServiceClient, sql::client::FlightSqlServiceClient,
};
use common::{
    arrow::{
        self,
        array::{BinaryArray, FixedSizeBinaryArray, RecordBatch, StringArray},
        datatypes::DataType,
        json::writer::JsonArray,
    },
    catalog::physical::{Catalog, PhysicalTable},
    config::{Addrs, Config},
    metadata::block_ranges_by_table,
    multirange::MultiRange,
    parquet::basic::{Compression, ZstdLevel},
    query_context::parse_sql,
    BoxError, QueryContext,
};
use dataset_store::DatasetStore;
use dump::{dump_tables, parquet_opts, Ctx as DumpCtx};
use figment::{
    providers::{Format as _, Json},
    Figment,
};
use fs_err as fs;
use futures::{stream::TryStreamExt, StreamExt as _};
use metadata_db::temp::TempMetadataDb;
use metadata_db::{MetadataDb, KEEP_TEMP_DIRS};
use nozzle::{dump_cmd::datasets_and_dependencies, server::BoundAddrs};
use object_store::path::Path;
use pretty_assertions::assert_str_eq;
use serde::{Deserialize, Deserializer};
use tokio::time;
use tracing::{debug, info, instrument, warn};

/// Assume the `cargo test` command is run either from the workspace root or from the crate root.
const TEST_CONFIG_BASE_DIRS: [&str; 2] = ["tests/config", "config"];

pub async fn load_test_config(config_override: Option<Figment>) -> Result<Arc<Config>, BoxError> {
    let mut path = None;
    for dir in TEST_CONFIG_BASE_DIRS.iter() {
        let p = format!("{}/config.toml", dir);
        if matches!(
            fs::metadata(&p).map_err(|e| e.kind()),
            Err(ErrorKind::NotFound)
        ) {
            continue;
        }
        path = Some(p);
        break;
    }

    let path = path.expect(
        "Couldn't find a test config file, `cargo test` must be run from the workspace root or the tests crate root"
    );
    Ok(Arc::new(
        Config::load(path, false, config_override, dynamic_addrs()).await?,
    ))
}

pub async fn bless(
    test_env: &TestEnv,
    dataset_name: &str,
    start: u64,
    end: u64,
) -> Result<(), BoxError> {
    let config = test_env.config.clone();
    let deps = {
        let ds = dataset_name.to_string();
        let mut ds_and_deps = datasets_and_dependencies(&test_env.dataset_store, vec![ds]).await?;
        assert_eq!(ds_and_deps.pop(), Some(dataset_name.to_string()));
        ds_and_deps
    };
    for dep in deps {
        restore_blessed_dataset(&dep, &test_env.metadata_db).await?;
    }

    dump(config, dataset_name, vec![], start, end, 1, true).await?;
    Ok(())
}

pub struct TestEnv {
    pub config: Arc<Config>,
    pub metadata_db: Arc<MetadataDb>,
    dataset_store: Arc<DatasetStore>,

    // Drop guard
    _temp_db: TempMetadataDb,
    _temp_dir: Option<tempfile::TempDir>,
}

impl TestEnv {
    /// Create a new test environment with a temp metadata database and data directory.
    pub async fn temp() -> Result<Self, BoxError> {
        Self::new(true).await
    }

    /// Create a new test environment with a temp metadata database, but the blessed data directory.
    pub async fn blessed() -> Result<Self, BoxError> {
        Self::new(false).await
    }
    pub async fn new(temp: bool) -> Result<Self, BoxError> {
        let db = TempMetadataDb::new(*KEEP_TEMP_DIRS).await;
        let figment = Figment::from(Json::string(&format!(
            r#"{{ "metadata_db_url": "{}" }}"#,
            db.url(),
        )));

        let (temp_dir, figment) = if temp {
            let temp_dir = tempfile::Builder::new()
                .disable_cleanup(*KEEP_TEMP_DIRS)
                .tempdir()?;
            let data_path = temp_dir.path();
            info!("Temporary data dir {}", data_path.display());
            let figment = figment.merge(Figment::from(Json::string(&format!(
                r#"{{ "data_dir": "{}" }}"#,
                data_path.display(),
            ))));
            (Some(temp_dir), figment)
        } else {
            (None, figment)
        };

        let config = load_test_config(Some(figment)).await?;
        let metadata_db: Arc<MetadataDb> = config.metadata_db().await?.into();
        let dataset_store = DatasetStore::new(config.clone(), metadata_db.clone());
        Ok(Self {
            config: config.clone(),
            metadata_db,
            dataset_store,
            _temp_db: db,
            _temp_dir: temp_dir,
        })
    }
}

pub struct SnapshotContext {
    pub(crate) ctx: QueryContext,
}

impl SnapshotContext {
    pub async fn blessed(env: &TestEnv, dataset: &str) -> Result<Self, BoxError> {
        let metadata_db = &env.metadata_db;
        let tables = restore_blessed_dataset(dataset, metadata_db).await?;
        let catalog = Catalog::new(tables, vec![]);
        let ctx: QueryContext = QueryContext::for_catalog(catalog, env.config.make_query_env()?)?;
        Ok(Self { ctx })
    }

    /// Dump the dataset to a temporary data directory.
    pub async fn temp_dump(
        test_env: &TestEnv,
        dataset_name: &str,
        dependencies: Vec<&str>,
        start: u64,
        end: u64,
        n_jobs: u16,
    ) -> Result<SnapshotContext, BoxError> {
        let catalog = dump(
            test_env.config.clone(),
            dataset_name,
            dependencies,
            start,
            end,
            n_jobs,
            true,
        )
        .await?;
        let ctx = QueryContext::for_catalog(catalog, test_env.config.make_query_env()?)?;

        Ok(SnapshotContext { ctx })
    }

    async fn check_block_range_eq(&self, blessed: &SnapshotContext) -> Result<(), BoxError> {
        let blessed_block_ranges = block_ranges_by_table(&blessed.ctx).await?;

        for table in self.ctx.catalog().tables() {
            let table_name = table.table_name().to_string();
            let ranges = table.ranges().await?;
            let expected_range = MultiRange::from_ranges(ranges)?;
            let actual_range = &blessed_block_ranges[&table_name];
            let table_qualified = table.table_ref().to_string();
            assert_eq!(
                expected_range, *actual_range,
                "for table {table_qualified}, \
                 the test expected data ranges to be exactly {expected_range}, but dataset has data ranges {actual_range}"
            );
        }

        Ok(())
    }

    /// Typically used to check a fresh snapshot against a blessed one.
    pub async fn assert_eq(&self, other: &SnapshotContext) -> Result<(), BoxError> {
        self.check_block_range_eq(other).await?;

        for table in self.ctx.catalog().tables() {
            let query = parse_sql(&format!(
                "select * from {} order by block_num",
                table.table_ref()
            ))?;
            let self_all_rows: RecordBatch = self
                .ctx
                .execute_and_concat(self.ctx.plan_sql(query.clone()).await?)
                .await?;
            let other_all_rows: RecordBatch = other
                .ctx
                .execute_and_concat(other.ctx.plan_sql(query.clone()).await?)
                .await?;

            assert_batch_eq(&self_all_rows, &other_all_rows);
        }

        Ok(())
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct DumpTestDatasetCommand {
    #[serde(rename = "dataset")]
    pub(crate) dataset_name: String,
    #[serde(default)]
    pub(crate) dependencies: Vec<String>,
    pub(crate) start: u64,
    pub(crate) end: u64,
    #[serde(rename = "nJobs")]
    pub(crate) n_jobs: u16,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct StreamingExecutionOptions {
    #[serde(rename = "maxDuration", deserialize_with = "deserialize_duration")]
    pub(crate) max_duration: Duration,
    #[serde(rename = "atLeastRows")]
    pub(crate) at_least_rows: usize,
}

fn deserialize_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;

    // Parse strings like "10s" into Duration
    humantime::parse_duration(&s)
        .map_err(|e| serde::de::Error::custom(format!("invalid duration: {}", e)))
}

#[instrument(skip_all)]
/// Clears the dataset directory if specified, before dumping.
async fn dump(
    config: Arc<Config>,
    dataset_name: &str,
    dependencies: Vec<&str>,
    start: u64,
    end: u64,
    n_jobs: u16,
    clear: bool,
) -> Result<Catalog, BoxError> {
    let metadata_db: Arc<MetadataDb> = config.metadata_db().await?.into();
    let dataset_store = DatasetStore::new(config.clone(), metadata_db);
    let mut tables = Vec::new();
    // First dump dependencies, then main dataset
    for dataset_name in dependencies {
        tables.push(
            dump_test_dataset(
                dataset_name,
                &config,
                &dataset_store,
                start,
                end,
                n_jobs,
                clear,
            )
            .await?,
        );
    }

    tables.push(
        dump_test_dataset(
            dataset_name,
            &config,
            &dataset_store,
            start,
            end,
            n_jobs,
            clear,
        )
        .await?,
    );

    let mut catalog = Catalog::empty();
    for table in tables.into_iter().flatten() {
        catalog.add_table(table);
    }
    Ok(catalog)
}

pub async fn check_blocks(
    test_env: &TestEnv,
    dataset_name: &str,
    start: u64,
    end: u64,
) -> Result<(), BoxError> {
    let env = test_env.config.make_query_env()?;

    dump_check::dump_check(
        dataset_name,
        &test_env.dataset_store,
        test_env.metadata_db.clone(),
        &env,
        1000,
        1,
        start,
        end,
    )
    .await
}

async fn clear_dataset(config: &Config, dataset_name: &str) -> Result<(), BoxError> {
    let store = config.data_store.prefixed_store();
    let path = Path::parse(dataset_name).unwrap();
    let path_stream = store.list(Some(&path)).map_ok(|o| o.location).boxed();
    store
        .delete_stream(path_stream)
        .try_collect::<Vec<_>>()
        .await?;
    Ok(())
}

#[instrument(skip_all)]
async fn dump_test_dataset(
    dataset_name: &str,
    config: &Arc<Config>,
    dataset_store: &Arc<DatasetStore>,
    start: u64,
    end: u64,
    n_jobs: u16,
    clear: bool,
) -> Result<Vec<PhysicalTable>, BoxError> {
    let partition_size = 1024 * 1024; // 100 kB
    let input_batch_block_size = 100_000;
    let compression = Compression::ZSTD(ZstdLevel::try_new(1).unwrap());

    // Disable bloom filters, as they bloat the test files and are not tested themselves.
    let parquet_opts = parquet_opts(compression, false);

    if clear {
        clear_dataset(config, dataset_name).await?;
    }

    let metadata_db: Arc<MetadataDb> = config.metadata_db().await?.into();
    let data_store = config.data_store.clone();
    let tables = {
        let dataset = dataset_store.load_dataset(dataset_name).await?.dataset;
        let mut tables = Vec::new();
        for table in Arc::new(dataset.clone()).resolved_tables() {
            let physical_table =
                match PhysicalTable::get_active(&table, metadata_db.clone()).await? {
                    Some(physical_table) if !clear => physical_table,
                    _ => {
                        PhysicalTable::next_revision(&table, &data_store, metadata_db.clone(), true)
                            .await?
                    }
                };

            tables.push(physical_table);
        }
        tables
    };

    dump_tables(
        DumpCtx {
            config: config.clone(),
            metadata_db: metadata_db.clone(),
            dataset_store: dataset_store.clone(),
            data_store: data_store.clone(),
        },
        &tables,
        n_jobs,
        partition_size,
        input_batch_block_size,
        &parquet_opts,
        (start as i64, Some(end as i64)),
    )
    .await?;

    Ok(tables)
}

pub async fn check_provider_file(filename: &str) {
    if TEST_CONFIG_BASE_DIRS.iter().all(|dir| {
        matches!(
            fs::metadata(format!("{dir}/providers/{filename}")).map_err(|e| e.kind()),
            Err(ErrorKind::NotFound)
        )
    }) {
        panic!(
            "Provider file '{filename}' does not exist. To run this test, copy 'COPY_ME_{filename}' as '{filename}', \
             filling in the required endpoints and credentials.",
        );
    }
}

pub fn assert_batch_eq(left: &RecordBatch, right: &RecordBatch) {
    use pretty_assertions::assert_str_eq;

    if left != right {
        let left = record_batch_to_json(left.clone());
        let right = record_batch_to_json(right.clone());
        assert_str_eq!(left, right);
    }
}

fn convert_binary_to_hex_strings(mut record_batch: RecordBatch) -> RecordBatch {
    let mut new_columns = Vec::with_capacity(record_batch.num_columns());
    let schema = record_batch.schema();
    let num_columns = record_batch.num_columns();

    for column_index in 0..num_columns {
        let column = record_batch.remove_column(0);
        let field = schema.field(column_index);

        let values: Box<dyn Iterator<Item = Option<&[u8]>>> = match field.data_type() {
            DataType::Binary => Box::new(
                column
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .into_iter(),
            ),
            DataType::FixedSizeBinary(_) => Box::new(Box::new(
                column
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap()
                    .into_iter(),
            )),
            _ => {
                // Not a binary column, so just add it back
                new_columns.push(column);
                continue;
            }
        };

        let string_array = values.map(|v| v.map(hex::encode)).collect::<StringArray>();
        new_columns.push(Arc::new(string_array));
    }

    RecordBatch::try_new(schema, new_columns).unwrap()
}

pub fn record_batch_to_json(record_batch: RecordBatch) -> String {
    // JSON does not support binary data, so encode any binary fields as hex strings.
    let record_batch = convert_binary_to_hex_strings(record_batch);

    let buffer = vec![];
    let mut writer = arrow::json::WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, JsonArray>(buffer);
    writer.write(&record_batch).unwrap();
    writer.finish().unwrap();

    String::from_utf8(writer.into_inner()).unwrap()
}

fn dynamic_addrs() -> Addrs {
    Addrs {
        flight_addr: ([0, 0, 0, 0], 0).into(),
        jsonl_addr: ([0, 0, 0, 0], 0).into(),
        registry_service_addr: ([0, 0, 0, 0], 0).into(),
        admin_api_addr: ([0, 0, 0, 0], 0).into(),
    }
}

/// Start a nozzle server, execute the given query, convert the result to JSONL, shut down the
/// server and return the JSONL string in binary format.
pub async fn run_query_on_fresh_server(
    test_env: &TestEnv,
    test_name: &str,
    query: &str,
    initial_dumps: Vec<DumpTestDatasetCommand>,
    dumps_on_running_server: Vec<DumpTestDatasetCommand>,
    streaming_options: Option<&StreamingExecutionOptions>,
) -> Result<serde_json::Value, BoxError> {
    check_provider_file("rpc_eth_mainnet.toml").await;
    check_provider_file("firehose_eth_mainnet.toml").await;

    let metadata_db = test_env.metadata_db.clone();

    restore_blessed_dataset("eth_firehose", &metadata_db).await?;
    restore_blessed_dataset("eth_rpc", &metadata_db).await?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
    let (bound, server) = nozzle::server::run(
        test_env.config.clone(),
        metadata_db.clone(),
        false,
        false,
        shutdown_rx,
    )
    .await?;
    tokio::spawn(async move {
        server.await.unwrap();
    });

    for initial_dump in initial_dumps {
        let dependencies: Vec<&str> = initial_dump
            .dependencies
            .iter()
            .map(|s| s.as_str())
            .collect();
        let _ = dump(
            test_env.config.clone(),
            &initial_dump.dataset_name,
            dependencies,
            initial_dump.start,
            initial_dump.end,
            initial_dump.n_jobs,
            true,
        )
        .await;
    }

    let client = FlightServiceClient::connect(format!("grpc://{}", bound.flight_addr)).await?;
    let mut client = FlightSqlServiceClient::new_from_inner(client);

    // Execute the SQL query and collect the results.
    let mut info = client.execute(query.to_string(), None).await?;
    let mut batches = client
        .do_get(info.endpoint[0].ticket.take().unwrap())
        .await?;
    let mut buf: Vec<u8> = Default::default();
    let mut writer = arrow::json::writer::ArrayWriter::new(&mut buf);
    let mut rows_returned: usize = 0;

    // Dumps on running server
    let config = test_env.config.clone();
    tokio::spawn(async move {
        for dump_command in dumps_on_running_server {
            let dependencies: Vec<&str> = dump_command
                .dependencies
                .iter()
                .map(|s| s.as_str())
                .collect();
            let _ = dump(
                config.clone(),
                &dump_command.dataset_name,
                dependencies,
                dump_command.start,
                dump_command.end,
                dump_command.n_jobs,
                false,
            )
            .await;
        }
    });

    if let Some(streaming_options) = streaming_options {
        loop {
            match time::timeout(streaming_options.max_duration, batches.next()).await {
                Ok(Some(batch)) => {
                    let batch = batch?;
                    writer.write(&batch)?;

                    // Stop streaming if we have enough rows taken
                    rows_returned += batch.num_rows();
                    if rows_returned >= streaming_options.at_least_rows {
                        break;
                    }
                }
                Ok(None) => {
                    break;
                }
                Err(_) => {
                    warn!(
                        "({test_name}) Streaming timed out after {:?}, stopping...",
                        streaming_options.max_duration
                    );
                    break;
                }
            }
        }
    } else {
        while let Some(batch) = batches.next().await {
            let batch = batch?;
            writer.write(&batch)?;
        }
    }

    shutdown_tx.send(()).unwrap();

    writer.finish()?;
    serde_json::from_slice(&buf).map_err(Into::into)
}

pub fn load_sql_tests(file_name: &str) -> Result<Vec<SqlTest>, BoxError> {
    let crate_path = env!("CARGO_MANIFEST_DIR");
    let path = format!("{crate_path}/specs/{file_name}");
    let content =
        fs::read(&path).map_err(|e| BoxError::from(format!("Failed to read {file_name}: {e}")))?;
    serde_yaml::from_slice(&content)
        .map_err(|e| BoxError::from(format!("Failed to parse {file_name}: {e}")))
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct SqlTest {
    /// Test name.
    pub name: String,
    /// SQL query to execute.
    pub query: String,
    /// JSON-encoded results.
    #[serde(flatten)]
    pub result: SqlTestResult,
    #[serde(default, rename = "streamingOptions")]
    pub streaming_options: Option<StreamingExecutionOptions>,
    #[serde(default, rename = "initialDumps")]
    pub initial_dumps: Vec<DumpTestDatasetCommand>,
    #[serde(default, rename = "dumpsOnRunningServer")]
    pub dumps_on_running_server: Vec<DumpTestDatasetCommand>,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(untagged)]
pub enum SqlTestResult {
    Success { results: String },
    Failure { failure: String },
}

impl SqlTest {
    pub fn assert_result_eq(self, results: Result<serde_json::Value, String>) {
        match self.result {
            SqlTestResult::Success {
                results: expected_results,
            } => {
                let expected_results: serde_json::Value = serde_json::from_str(&expected_results)
                    .map_err(|e| {
                        format!(
                            "Failed to parse expected results for test \"{}\": {e:?}",
                            self.name,
                        )
                    })
                    .unwrap();
                let results = results.unwrap();
                assert_str_eq!(
                    results.to_string(),
                    expected_results.to_string(),
                    "SQL test \"{}\" failed: SQL query \"{}\" did not return the expected results, see sql-tests.yaml",
                    self.name, self.query,
                );
            }
            SqlTestResult::Failure { failure } => {
                let failure = failure.trim();
                let results = results.unwrap_err();
                if !results.to_string().contains(&failure) {
                    panic!(
                        "SQL test \"{}\" failed: SQL query \"{}\" did not return the expected error, got \"{}\", expected \"{}\"",
                        self.name, self.query, results, failure,
                    );
                }
            }
        }
    }
}

pub struct DatasetPackage {
    pub name: String,

    // Relative to crate root
    pub path: PathBuf,
}

impl DatasetPackage {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            path: PathBuf::from_str(&format!("datasets/{}", name)).unwrap(),
        }
    }

    #[instrument(skip_all, err)]
    pub async fn pnpm_install(&self) -> Result<(), BoxError> {
        let install_path = self.path.parent().unwrap();
        debug!(
            "Running pnpm install on `{}`",
            install_path.to_string_lossy()
        );

        let status = tokio::process::Command::new("pnpm")
            .args(&["install"])
            .current_dir(install_path)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .await?;

        if status != ExitStatus::default() {
            return Err(BoxError::from(format!(
                "Failed to install dataset {}: pnpm install failed with exit code {status}",
                self.name,
            )));
        }

        Ok(())
    }

    pub async fn build(&self, bound_addrs: BoundAddrs) -> Result<(), BoxError> {
        let status = tokio::process::Command::new("pnpm")
            .args(&["nozzl", "build"])
            .env(
                "NOZZLE_REGISTRY_URL",
                &format!("http://{}", bound_addrs.registry_service_addr),
            )
            .env(
                "NOZZLE_ADMIN_URL",
                &format!("http://{}", bound_addrs.admin_api_addr),
            )
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .current_dir(&self.path)
            .status()
            .await?;

        if status != ExitStatus::default() {
            return Err(BoxError::from(format!(
                "Failed to build dataset {}: pnpm build failed with exit code {status}",
                self.name,
            )));
        }

        Ok(())
    }

    #[instrument(skip_all, err)]
    pub async fn deploy(&self, bound_addrs: BoundAddrs) -> Result<(), BoxError> {
        let status = tokio::process::Command::new("pnpm")
            .args(&["nozzl", "deploy"])
            .env(
                "NOZZLE_REGISTRY_URL",
                &format!("http://{}", bound_addrs.registry_service_addr),
            )
            .env(
                "NOZZLE_ADMIN_URL",
                &format!("http://{}", bound_addrs.admin_api_addr),
            )
            .current_dir(&self.path)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .await?;

        if status != ExitStatus::default() {
            return Err(BoxError::from(format!(
                "Failed to deploy dataset {}: pnpm deploy failed with exit code {status}",
                self.name,
            )));
        }

        Ok(())
    }
}

async fn restore_blessed_dataset(
    dataset: &str,
    metadata_db: &Arc<MetadataDb>,
) -> Result<Vec<PhysicalTable>, BoxError> {
    let config = load_test_config(None).await?;
    let dataset_store = DatasetStore::new(config.clone(), metadata_db.clone());
    let dataset = dataset_store.load_dataset(dataset).await?.dataset;
    let dataset_name = dataset.name.clone();
    let data_store = config.data_store.clone();
    let mut tables = Vec::new();
    for table in Arc::new(dataset).resolved_tables() {
        tables.push(
            PhysicalTable::restore_latest_revision(&table, data_store.clone(), metadata_db.clone())
                .await?
                .expect(
                    format!(
                        "Failed to restore blessed table {dataset_name}.{}. This is likely due to \
                        the dataset or table being deleted. \n\
                        Bless the dataset again with by running \
                        `cargo run -p tests -- bless {dataset_name} <start_block> <end_block>`",
                        table.name()
                    )
                    .as_str(),
                ),
        );
    }
    Ok(tables)
}
