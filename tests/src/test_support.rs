use std::{
    io::{ErrorKind, Write},
    sync::Arc,
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
    catalog::physical::{Catalog, PhysicalDataset},
    config::{Addrs, Config, FigmentJson},
    multirange::MultiRange,
    parquet::basic::{Compression, ZstdLevel},
    query_context::parse_sql,
    BoxError, QueryContext,
};
use dataset_store::DatasetStore;
use figment::providers::Format as _;
use futures::{stream::TryStreamExt, StreamExt as _};
use metadata_db::MetadataDb;
use object_store::path::Path;
use tracing::info;

use dump::{dump_dataset, parquet_opts};
use fs_err as fs;
use tempfile::TempDir;

/// Assume the `cargo test` command is run either from the workspace root or from the crate root.
const TEST_CONFIG_BASE_DIRS: [&str; 2] = ["tests/config", "config"];

pub fn load_test_config(literal_override: Option<FigmentJson>) -> Result<Arc<Config>, BoxError> {
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
    Ok(Arc::new(Config::load(
        path,
        false,
        literal_override,
        dynamic_addrs(),
    )?))
}

pub async fn bless(dataset_name: &str, start: u64, end: u64) -> Result<(), BoxError> {
    let config = load_test_config(None)?;
    redump(config, dataset_name, vec![], start, end, 1, None).await?;
    Ok(())
}

pub struct SnapshotContext {
    ctx: QueryContext,

    /// For a dataset dumped to a temporary directory. The directory is deleted on drop.
    _temp_dir: Option<TempDir>,
}

impl SnapshotContext {
    pub async fn blessed(dataset: &str) -> Result<Self, BoxError> {
        let config = load_test_config(None)?;
        let dataset_store = DatasetStore::new(config.clone(), None);
        let dataset = dataset_store.load_dataset(dataset).await?.dataset;
        let catalog = Catalog::for_dataset(dataset, config.data_store.clone(), None).await?;
        let ctx = QueryContext::for_catalog(catalog, Arc::new(config.make_runtime_env()?))?;
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
        metadata_db: Option<&MetadataDb>,
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

        let config = load_test_config(config_override)?;

        let physical_dataset = redump(
            config.clone(),
            dataset_name,
            dependencies.clone(),
            start,
            end,
            n_jobs,
            metadata_db,
        )
        .await?;
        let mut physical_datasets: Vec<PhysicalDataset> = vec![physical_dataset];
        let dataset_store = DatasetStore::new(config.clone(), None);
        for dep in dependencies {
            let dep = dataset_store.load_dataset(dep).await?.dataset;
            let dep = PhysicalDataset::from_dataset_at(dep, config.data_store.clone(), None, true)
                .await?;
            physical_datasets.push(dep);
        }

        let catalog = Catalog::new(physical_datasets);
        let ctx = QueryContext::for_catalog(catalog, Arc::new(config.make_runtime_env()?))?;

        Ok(SnapshotContext {
            ctx,
            _temp_dir: Some(temp_dir),
        })
    }

    async fn check_scanned_range_eq(
        &self,
        other: &SnapshotContext,
        metadata_db: Option<&MetadataDb>,
    ) -> Result<(), BoxError> {
        use common::meta_tables::scanned_ranges::{ranges_for_table, scanned_ranges_by_table};

        let other_scanned_ranges = scanned_ranges_by_table(&other.ctx, None).await?;

        for table in self.ctx.catalog().all_tables() {
            let table_name = table.table_name().to_string();
            let tbl = table.table_id();
            let ranges = ranges_for_table(&self.ctx, metadata_db, tbl).await?;
            let expected_range = MultiRange::from_ranges(ranges)?;
            let actual_range = &other_scanned_ranges[&table_name];
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
    pub async fn assert_eq(
        &self,
        other: &SnapshotContext,
        metadata_db: Option<&MetadataDb>,
    ) -> Result<(), BoxError> {
        self.check_scanned_range_eq(other, metadata_db).await?;

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

/// Clears the dataset directory, if it exists, before dumping.
async fn redump(
    config: Arc<Config>,
    dataset_name: &str,
    dependencies: Vec<&str>,
    start: u64,
    end: u64,
    n_jobs: u16,
    metadata_db: Option<&MetadataDb>,
) -> Result<PhysicalDataset, BoxError> {
    let dataset_store = DatasetStore::new(config.clone(), metadata_db.cloned());

    // First dump dependencies, then main dataset
    for dataset_name in dependencies {
        let _ = redump_dataset(
            dataset_name,
            &*config,
            &dataset_store,
            start,
            end,
            n_jobs,
            metadata_db,
        )
        .await?;
    }
    let dataset = redump_dataset(
        dataset_name,
        &*config,
        &dataset_store,
        start,
        end,
        n_jobs,
        metadata_db,
    )
    .await?;

    Ok(dataset)
}

pub async fn check_blocks(dataset_name: &str, start: u64, end: u64) -> Result<(), BoxError> {
    let config = load_test_config(None)?;
    let dataset_store = DatasetStore::new(config.clone(), None);
    let env = Arc::new(config.make_runtime_env()?);

    dump_check::dump_check(
        dataset_name,
        &dataset_store,
        &config,
        None,
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

async fn redump_dataset(
    dataset_name: &str,
    config: &Config,
    dataset_store: &Arc<DatasetStore>,
    start: u64,
    end: u64,
    n_jobs: u16,
    metadata_db: Option<&MetadataDb>,
) -> Result<PhysicalDataset, BoxError> {
    let partition_size = 1024 * 1024; // 100 kB
    let input_batch_block_size = 100_000;
    let compression = Compression::ZSTD(ZstdLevel::try_new(1).unwrap());

    // Disable bloom filters, as they bloat the test files and are not tested themselves.
    let parquet_opts = parquet_opts(compression, false);

    clear_dataset(config, dataset_name).await?;

    let dataset = {
        let dataset = dataset_store.load_dataset(dataset_name).await?.dataset;
        PhysicalDataset::from_dataset_at(dataset, config.data_store.clone(), metadata_db, false)
            .await?
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

pub const SQL_TEST_QUERIES: &[(&str, &str)] = &[
    ("SELECT 1", "select_1.json"),
    (
        "SELECT eth_rpc.eth_call(from, to, input, CAST(block_num as STRING))
         FROM eth_rpc.transactions
         WHERE tx_index = 2",
        "eth_call.json"
    ),
    (
        "SELECT evm_decode(l.topic1, l.topic2, l.topic3, l.data, 'Transfer(address indexed from, address indexed to, uint256 value)')
         FROM eth_rpc.logs l
         WHERE l.topic0 = evm_topic('Transfer(address indexed from, address indexed to, uint256 value)')
         AND l.topic3 IS NULL
         LIMIT 1",
        "evm_decode.json"
    ),
    ("SELECT evm_topic('Transfer(address indexed from, address indexed to, uint256 value)')", "evm_topic.json"),
    ("SELECT attestation_hash(input) FROM eth_rpc.transactions", "attestation_hash.json"),
];

pub async fn bless_sql_snapshots(filename: Option<String>) -> Result<(), BoxError> {
    let queries = SQL_TEST_QUERIES.iter().filter(|(_, f)| match filename {
        Some(ref filename) => f == filename,
        None => true,
    });
    for (query, filename) in queries {
        let jsonl = run_query_on_fresh_server(query).await?;
        let path = sql_snapshot_path(filename);
        let mut file = fs::File::create(&path)?;
        file.write_all(&jsonl)?;
    }
    Ok(())
}

pub fn sql_snapshot_path(filename: &str) -> String {
    let crate_path = env!("CARGO_MANIFEST_DIR");
    format!("{crate_path}/sql-snapshots/{filename}")
}

/// Start a nozzle server, execute the given query, convert the result to JSONL, shut down the
/// server and return the JSONL string in binary format.
pub async fn run_query_on_fresh_server(query: &str) -> Result<Vec<u8>, BoxError> {
    check_provider_file("rpc_eth_mainnet.toml").await;

    // Start the nozzle server.
    let config = load_test_config(None).unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
    let (bound, server) = nozzle::server::run(config, None, false, shutdown_rx).await?;
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
    let mut writer = arrow::json::writer::LineDelimitedWriter::new(&mut buf);
    while let Some(batch) = batches.next().await {
        let batch = batch?;
        writer.write(&batch)?;
    }

    shutdown_tx.send(()).unwrap();

    Ok(buf)
}
