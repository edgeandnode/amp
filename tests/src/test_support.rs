use std::{
    io::ErrorKind,
    process::{ExitStatus, Stdio},
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
    catalog::physical::{Catalog, PhysicalDataset, PhysicalTable},
    config::{Addrs, Config, FigmentJson},
    metadata::block_ranges_by_table,
    multirange::MultiRange,
    parquet::basic::{Compression, ZstdLevel},
    query_context::parse_sql,
    BoxError, QueryContext,
};
use dataset_store::DatasetStore;
use dump::{dump_dataset, parquet_opts};
use figment::providers::Format as _;
use fs_err as fs;
use futures::{stream::TryStreamExt, StreamExt as _};
use metadata_db::{MetadataDb, KEEP_TEMP_DIRS};
use nozzle::server::BoundAddrs;
use object_store::path::Path;
use pretty_assertions::assert_str_eq;
use serde::{Deserialize, Deserializer};
use tempfile::TempDir;
use tokio::time;
use tracing::{info, instrument};

/// Assume the `cargo test` command is run either from the workspace root or from the crate root.
const TEST_CONFIG_BASE_DIRS: [&str; 2] = ["tests/config", "config"];

pub async fn load_test_config(
    literal_override: Option<FigmentJson>,
) -> Result<Arc<Config>, BoxError> {
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
        Config::load(path, false, literal_override, dynamic_addrs()).await?,
    ))
}

pub async fn bless(dataset_name: &str, start: u64, end: u64) -> Result<(), BoxError> {
    let config = load_test_config(None).await?;
    dump(config, dataset_name, vec![], start, end, 1, true).await?;
    Ok(())
}

pub struct SnapshotContext {
    pub(crate) ctx: QueryContext,

    /// For a dataset dumped to a temporary directory. The directory is deleted on drop.
    _temp_dir: Option<TempDir>,
}

impl SnapshotContext {
    pub async fn blessed(dataset: &str) -> Result<Self, BoxError> {
        let config = load_test_config(None).await?;
        let metadata_db: Arc<MetadataDb> = config.metadata_db().await?.into();
        let dataset_store = DatasetStore::new(config.clone(), metadata_db.clone());
        let dataset = dataset_store.load_dataset(dataset).await?.dataset;
        let dataset_name = &dataset.name;
        let data_store = config.data_store.clone();
        let mut tables = Vec::new();
        for table in dataset.tables() {
            tables.push(
                PhysicalTable::restore_latset_revision(
                    table,
                    data_store.clone(),
                    dataset_name,
                    metadata_db.clone(),
                )
                .await?
                .expect(
                    format!(
                        "Failed to restore blessed table {dataset_name}.{}. This is likely due to \
                        the dataset or table being deleted. \n\
                        Bless the dataset again with by running \
                        `cargo run -p tests -- bless {dataset_name} <start_block> <end_block>`",
                        table.name
                    )
                    .as_str(),
                ),
            );
        }
        let dataset: PhysicalDataset = PhysicalDataset::new(dataset, tables);
        let catalog = Catalog::new(vec![dataset]);
        let ctx: QueryContext = QueryContext::for_catalog(catalog, config.make_query_env()?)?;
        Ok(Self {
            ctx,
            _temp_dir: None,
        })
    }

    /// Dump the dataset to a temporary data directory.
    pub async fn temp_dump(
        dataset_name: &str,
        dependencies: Vec<&str>,
        start: u64,
        end: u64,
        n_jobs: u16,
        keep_temp_dir: bool,
    ) -> Result<SnapshotContext, BoxError> {
        use figment::providers::Json;

        let temp_dir = tempfile::Builder::new()
            .disable_cleanup(keep_temp_dir)
            .tempdir()?;
        let path = temp_dir.path();
        info!("Dumping dataset to {}", path.display());

        let config_override = Some(Json::string(&format!(
            r#"{{ "data_dir": "{}" }}"#,
            path.display()
        )));

        let config = load_test_config(config_override).await?;
        let catalog = dump(
            config.clone(),
            dataset_name,
            dependencies,
            start,
            end,
            n_jobs,
            true,
        )
        .await?;
        let ctx = QueryContext::for_catalog(catalog, config.make_query_env()?)?;

        Ok(SnapshotContext {
            ctx,
            _temp_dir: Some(temp_dir),
        })
    }

    async fn check_block_range_eq(&self, blessed: &SnapshotContext) -> Result<(), BoxError> {
        let blessed_block_ranges = block_ranges_by_table(&blessed.ctx).await?;

        for table in self.ctx.catalog().all_tables() {
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

        for table in self.ctx.catalog().all_tables() {
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
    let mut datasets = Vec::new();
    // First dump dependencies, then main dataset
    for dataset_name in dependencies {
        datasets.push(
            dump_test_dataset(
                dataset_name,
                &*config,
                &dataset_store,
                start,
                end,
                n_jobs,
                clear,
            )
            .await?,
        );
    }

    datasets.push(
        dump_test_dataset(
            dataset_name,
            &*config,
            &dataset_store,
            start,
            end,
            n_jobs,
            clear,
        )
        .await?,
    );
    let catalog = Catalog::new(datasets);
    Ok(catalog)
}

pub async fn check_blocks(dataset_name: &str, start: u64, end: u64) -> Result<(), BoxError> {
    let config = load_test_config(None).await?;
    let metadata_db: Arc<MetadataDb> = config.metadata_db().await?.into();
    let dataset_store = DatasetStore::new(config.clone(), metadata_db.clone());
    let env = config.make_query_env()?;

    dump_check::dump_check(
        dataset_name,
        &dataset_store,
        metadata_db,
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
    config: &Config,
    dataset_store: &Arc<DatasetStore>,
    start: u64,
    end: u64,
    n_jobs: u16,
    clear: bool,
) -> Result<PhysicalDataset, BoxError> {
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
    let dataset = {
        let dataset = dataset_store.load_dataset(dataset_name).await?.dataset;
        let mut tables = Vec::new();
        for table in dataset.tables() {
            let physical_table = match PhysicalTable::get_or_restore_active_revision(
                table,
                dataset_name,
                data_store.clone(),
                metadata_db.clone(),
            )
            .await?
            {
                Some(physical_table) if !clear => physical_table,
                _ => {
                    PhysicalTable::next_revision(
                        table,
                        &data_store,
                        dataset_name,
                        metadata_db.clone(),
                    )
                    .await?
                }
            };

            tables.push(physical_table);
        }
        PhysicalDataset::new(dataset, tables)
    };

    dump_dataset(
        &dataset,
        dataset_store,
        config,
        n_jobs,
        partition_size,
        input_batch_block_size,
        &parquet_opts,
        start as i64,
        Some(end as i64),
    )
    .await?;

    Ok(dataset)
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

fn record_batch_to_json(record_batch: RecordBatch) -> String {
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
    query: &str,
    initial_dumps: Vec<DumpTestDatasetCommand>,
    dumps_on_running_server: Vec<DumpTestDatasetCommand>,
    streaming_options: Option<&StreamingExecutionOptions>,
) -> Result<serde_json::Value, BoxError> {
    check_provider_file("rpc_eth_mainnet.toml").await;
    check_provider_file("firehose_eth_mainnet.toml").await;

    // Start the nozzle server.
    let config = if initial_dumps.is_empty() && dumps_on_running_server.is_empty() {
        load_test_config(None).await?
    } else {
        use figment::providers::Json;

        let temp_dir = tempfile::Builder::new()
            .disable_cleanup(*KEEP_TEMP_DIRS)
            .tempdir()?;
        let path = temp_dir.path();

        let config_override = Some(Json::string(&format!(
            r#"{{ "data_dir": "{}" }}"#,
            path.display()
        )));

        load_test_config(config_override).await?
    };
    let metadata_db = config.metadata_db().await?.into();
    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
    let (bound, server) =
        nozzle::server::run(config.clone(), metadata_db, false, false, shutdown_rx).await?;
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
            config.clone(),
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
    let path = format!("{crate_path}/{file_name}");
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
    pub path: String,
}

impl DatasetPackage {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            path: format!("datasets/{}", name),
        }
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
