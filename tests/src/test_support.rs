use std::{
    collections::BTreeMap,
    io::ErrorKind,
    path::PathBuf,
    process::{ExitStatus, Stdio},
    str::FromStr as _,
    sync::Arc,
};

use common::{
    BoxError, QueryContext,
    arrow::{
        self,
        array::{BinaryArray, FixedSizeBinaryArray, RecordBatch, StringArray},
        datatypes::DataType,
        json::writer::JsonArray,
    },
    catalog::physical::{Catalog, PhysicalTable},
    config::Config,
    metadata::segments::BlockRange,
    query_context::parse_sql,
};
use dataset_store::DatasetStore;
use dump::worker::Worker;
use figment::{
    Figment,
    providers::{Format as _, Json},
};
use fs_err as fs;
use futures::{StreamExt as _, stream::TryStreamExt};
use metadata_db::{KEEP_TEMP_DIRS, MetadataDb, WorkerNodeId, temp::TempMetadataDb};
use nozzle::{
    dump_cmd::{datasets_and_dependencies, dump},
    server::BoundAddrs,
};
use object_store::path::Path;
use pretty_assertions::assert_str_eq;
use tracing::{debug, info, instrument};

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
        Config::load(path, false, config_override, true).await?,
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

    clear_dataset(&test_env.config, dataset_name).await?;
    dump_dataset(&config, dataset_name, start, end, 1, None).await?;
    Ok(())
}

pub struct TestEnv {
    pub config: Arc<Config>,
    pub metadata_db: Arc<MetadataDb>,
    pub dataset_store: Arc<DatasetStore>,
    pub server_addrs: BoundAddrs,

    // Drop guard
    _temp_db: TempMetadataDb,
    _temp_dir: Option<tempfile::TempDir>,
}

impl TestEnv {
    /// Create a new test environment with a temp metadata database and data directory.
    pub async fn temp(test_name: &str) -> Result<Self, BoxError> {
        Self::new(test_name, true).await
    }

    /// Create a new test environment with a temp metadata database, but the blessed data directory.
    pub async fn blessed(test_name: &str) -> Result<Self, BoxError> {
        Self::new(test_name, false).await
    }

    pub async fn new(test_name: &str, temp: bool) -> Result<Self, BoxError> {
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

        let (bound, server) = nozzle::server::run(
            config.clone(),
            metadata_db.clone(),
            false,
            true,
            true,
            true,
            true,
        )
        .await?;
        tokio::spawn(server);

        let worker = Worker::new(
            config.clone(),
            metadata_db.clone(),
            WorkerNodeId::from_str(test_name).unwrap(),
        );
        tokio::spawn(worker.run());

        Ok(Self {
            config: config.clone(),
            metadata_db,
            dataset_store,
            server_addrs: bound,
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
        start: u64,
        end: u64,
        n_jobs: u16,
    ) -> Result<SnapshotContext, BoxError> {
        dump_dataset(&test_env.config, dataset_name, start, end, n_jobs, None).await?;
        let catalog = catalog_for_dataset(
            dataset_name,
            &test_env.dataset_store,
            test_env.metadata_db.clone(),
        )
        .await?;
        let ctx = QueryContext::for_catalog(catalog, test_env.config.make_query_env()?)?;

        Ok(SnapshotContext { ctx })
    }

    async fn check_block_range_eq(&self, blessed: &SnapshotContext) -> Result<(), BoxError> {
        let mut blessed_block_ranges: BTreeMap<String, Vec<BlockRange>> = Default::default();
        for table in blessed.ctx.catalog().tables() {
            blessed_block_ranges
                .insert(table.table_name().to_string(), table_ranges(&table).await?);
        }

        for table in self.ctx.catalog().tables() {
            let table_name = table.table_name();
            let mut expected_ranges = table_ranges(&table).await?;
            expected_ranges.sort_by_key(|r| *r.numbers.start());
            let actual_ranges = blessed_block_ranges.get_mut(table_name).unwrap();
            actual_ranges.sort_by_key(|r| *r.numbers.start());
            let table_qualified = table.table_ref().to_string();
            assert_eq!(
                &expected_ranges, actual_ranges,
                "for table {table_qualified}, \
                 the test expected data ranges to be exactly {expected_ranges:?}, but dataset has data ranges {actual_ranges:?}"
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

#[instrument(skip_all)]
pub(crate) async fn dump_dataset(
    config: &Arc<Config>,
    dataset_name: &str,
    start: u64,
    end: u64,
    n_jobs: u16,
    microbatch_max_interval: Option<u64>,
) -> Result<(), BoxError> {
    // dump the dataset
    let partition_size_mb = 100;
    let metadata_db: Arc<MetadataDb> = config.metadata_db().await?.into();

    dump(
        config.clone(),
        metadata_db.clone(),
        vec![dataset_name.to_string()],
        true,
        start as i64,
        Some(end.to_string()),
        n_jobs,
        partition_size_mb,
        false,
        None,
        microbatch_max_interval,
        None,
        false,
    )
    .await?;

    Ok(())
}

pub(crate) async fn catalog_for_dataset(
    dataset_name: &str,
    dataset_store: &Arc<DatasetStore>,
    metadata_db: Arc<MetadataDb>,
) -> Result<Catalog, BoxError> {
    let dataset = dataset_store.load_dataset(dataset_name).await?;
    let mut tables = Vec::new();
    for table in Arc::new(dataset.clone()).resolved_tables() {
        // Unwrap: we just dumped the dataset, so it must have an active physical table.
        let physical_table = PhysicalTable::get_active(&table, metadata_db.clone())
            .await?
            .unwrap();
        tables.push(physical_table.into());
    }
    Ok(Catalog::new(tables, vec![]))
}

pub async fn table_ranges(table: &PhysicalTable) -> Result<Vec<BlockRange>, BoxError> {
    let files = table.files().await?;
    Ok(files
        .into_iter()
        .map(|mut f| {
            assert!(f.parquet_meta.ranges.len() == 1);
            f.parquet_meta.ranges.remove(0)
        })
        .collect())
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

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(untagged)]
pub enum SqlTestResult {
    Success { results: String },
    Failure { failure: String },
}

impl SqlTestResult {
    #[cfg(test)]
    pub(crate) fn assert_eq(
        &self,
        actual_result: Result<serde_json::Value, BoxError>,
    ) -> Result<(), BoxError> {
        match self {
            SqlTestResult::Success {
                results: expected_json_str,
            } => {
                let expected: serde_json::Value = serde_json::from_str(expected_json_str)
                    .unwrap_or_else(|e| panic!("failed to parse expected JSON: {e}"));

                let actual = actual_result.expect(&format!("expected success, got error",));

                assert_str_eq!(
                    actual.to_string(),
                    expected.to_string(),
                    "Test returned unexpected results",
                );
            }

            SqlTestResult::Failure { failure } => {
                let expected_substring = failure.trim();

                let actual_error =
                    actual_result.expect_err(&format!("expected failure, got success"));

                let actual_error_str = actual_error.to_string();

                if !actual_error_str.contains(expected_substring) {
                    panic!(
                        "Expected substring: \"{}\"\nActual error: \"{}\"",
                        expected_substring, actual_error_str
                    );
                }
            }
        }
        Ok(())
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

pub async fn restore_blessed_dataset(
    dataset: &str,
    metadata_db: &Arc<MetadataDb>,
) -> Result<Vec<Arc<PhysicalTable>>, BoxError> {
    let config = load_test_config(None).await?;
    let dataset_store = DatasetStore::new(config.clone(), metadata_db.clone());
    let dataset = dataset_store.load_dataset(dataset).await?;
    let dataset_name = dataset.name.clone();
    let data_store = config.data_store.clone();
    let mut tables = Vec::new();
    for table in Arc::new(dataset).resolved_tables() {
        let physical_table =
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
                );
        tables.push(physical_table.into());
    }
    Ok(tables)
}
