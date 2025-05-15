use std::{io::ErrorKind, sync::Arc};

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
use metadata_db::MetadataDb;
use object_store::path::Path;
use tempfile::TempDir;
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
    redump(config, dataset_name, vec![], start, end, 1).await?;
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
        let ctx: QueryContext =
            QueryContext::for_catalog(catalog, Arc::new(config.make_runtime_env()?))?;
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

        let temp_dir = tempfile::Builder::new().keep(keep_temp_dir).tempdir()?;
        let path = temp_dir.path();
        info!("Dumping dataset to {}", path.display());

        let config_override = Some(Json::string(&format!(
            r#"{{ "data_dir": "{}" }}"#,
            path.display()
        )));

        let config = load_test_config(config_override).await?;
        let catalog = redump(
            config.clone(),
            dataset_name,
            dependencies,
            start,
            end,
            n_jobs,
        )
        .await?;
        let ctx = QueryContext::for_catalog(catalog, Arc::new(config.make_runtime_env()?))?;

        Ok(SnapshotContext {
            ctx,
            _temp_dir: Some(temp_dir),
        })
    }

    async fn check_scanned_range_eq(&self, blessed: &SnapshotContext) -> Result<(), BoxError> {
        use common::meta_tables::scanned_ranges::scanned_ranges_by_table;

        let blessed_scanned_ranges = scanned_ranges_by_table(&blessed.ctx).await?;

        for table in self.ctx.catalog().all_tables() {
            let table_name = table.table_name().to_string();
            let ranges = table.ranges().await?;
            let expected_range = MultiRange::from_ranges(ranges)?;
            let actual_range = &blessed_scanned_ranges[&table_name];
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
        self.check_scanned_range_eq(other).await?;

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

#[instrument(skip_all)]
/// Clears the dataset directory, if it exists, before dumping.
async fn redump(
    config: Arc<Config>,
    dataset_name: &str,
    dependencies: Vec<&str>,
    start: u64,
    end: u64,
    n_jobs: u16,
) -> Result<Catalog, BoxError> {
    let metadata_db: Arc<MetadataDb> = config.metadata_db().await?.into();
    let dataset_store = DatasetStore::new(config.clone(), metadata_db);
    let mut datasets = Vec::new();
    // First dump dependencies, then main dataset
    for dataset_name in dependencies {
        datasets.push(
            redump_dataset(dataset_name, &*config, &dataset_store, start, end, n_jobs).await?,
        );
    }

    datasets
        .push(redump_dataset(dataset_name, &*config, &dataset_store, start, end, n_jobs).await?);
    let catalog = Catalog::new(datasets);
    Ok(catalog)
}

pub async fn check_blocks(dataset_name: &str, start: u64, end: u64) -> Result<(), BoxError> {
    let config = load_test_config(None).await?;
    let metadata_db: Arc<MetadataDb> = config.metadata_db().await?.into();
    let dataset_store = DatasetStore::new(config.clone(), metadata_db.clone());
    let env = Arc::new(config.make_runtime_env()?);

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
async fn redump_dataset(
    dataset_name: &str,
    config: &Config,
    dataset_store: &Arc<DatasetStore>,
    start: u64,
    end: u64,
    n_jobs: u16,
) -> Result<PhysicalDataset, BoxError> {
    let partition_size = 1024 * 1024; // 100 kB
    let input_batch_block_size = 100_000;
    let compression = Compression::ZSTD(ZstdLevel::try_new(1).unwrap());

    // Disable bloom filters, as they bloat the test files and are not tested themselves.
    let parquet_opts = parquet_opts(compression, false);

    clear_dataset(config, dataset_name).await?;
    let metadata_db: Arc<MetadataDb> = config.metadata_db().await?.into();
    let data_store = config.data_store.clone();
    let dataset = {
        let dataset = dataset_store.load_dataset(dataset_name).await?.dataset;
        let mut tables = Vec::new();
        for table in dataset.tables() {
            let physical_table =
                PhysicalTable::next_revision(table, &data_store, dataset_name, metadata_db.clone())
                    .await?;
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
pub async fn run_query_on_fresh_server(query: &str) -> Result<serde_json::Value, BoxError> {
    check_provider_file("rpc_eth_mainnet.toml").await;

    // Start the nozzle server.
    let config = load_test_config(None).await.unwrap();
    let metadata_db = config.metadata_db().await?.into();
    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
    let (bound, server) =
        nozzle::server::run(config, metadata_db, false, false, shutdown_rx).await?;
    tokio::spawn(async move {
        server.await.unwrap();
    });

    let client = FlightServiceClient::connect(format!("grpc://{}", bound.flight_addr)).await?;
    let mut client = FlightSqlServiceClient::new_from_inner(client);

    // Execute the SQL query and collect the results.
    let mut info = client.execute(query.to_string(), None).await?;
    let mut batches = client
        .do_get(info.endpoint[0].ticket.take().unwrap())
        .await?;
    let mut buf: Vec<u8> = Default::default();
    let mut writer = arrow::json::writer::ArrayWriter::new(&mut buf);
    while let Some(batch) = batches.next().await {
        let batch = batch?;
        writer.write(&batch)?;
    }

    shutdown_tx.send(()).unwrap();

    writer.finish()?;
    serde_json::from_slice(&buf).map_err(Into::into)
}

pub fn load_sql_tests() -> Result<Vec<SqlTest>, BoxError> {
    let crate_path = env!("CARGO_MANIFEST_DIR");
    let path = format!("{crate_path}/sql-tests.yaml");
    let content = fs::read(&path)
        .map_err(|e| BoxError::from(format!("Failed to read sql-tests.yaml: {e}")))?;
    serde_yaml::from_slice(&content)
        .map_err(|e| BoxError::from(format!("Failed to parse sql-tests.yaml: {e}")))
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
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(untagged)]
pub enum SqlTestResult {
    Success { results: String },
    Failure { failure: String },
}
