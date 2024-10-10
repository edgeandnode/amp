use std::{io::ErrorKind, sync::Arc};

use common::{
    arrow::{
        self,
        array::{BinaryArray, FixedSizeBinaryArray, RecordBatch, StringArray},
        datatypes::DataType,
        json::writer::JsonArray,
    },
    catalog::physical::Catalog,
    config::Config,
    parquet::basic::{Compression, ZstdLevel},
    query_context::parse_sql,
    BoxError, Dataset, QueryContext,
};
use dataset_store::DatasetStore;
use figment::providers::Format as _;
use futures::{stream::TryStreamExt, StreamExt as _};
use log::info;
use object_store::path::Path;

use dump::{dump_dataset, parquet_opts};
use tempfile::TempDir;
use tokio::fs;

pub const TEST_CONFIG_PATH: &str = "config/config.toml";

pub async fn bless(dataset_name: &str, start: u64, end: u64) -> Result<(), BoxError> {
    let config = Arc::new(Config::load(TEST_CONFIG_PATH, false, None)?);
    dump(config, dataset_name, start, end).await
}

pub async fn assert_temp_eq_blessed(temp: &TempDatasetDump) -> Result<(), BoxError> {
    let blessed_ctx = {
        let config = Arc::new(Config::load(TEST_CONFIG_PATH, false, None)?);
        let dataset_store = DatasetStore::new(config.clone());
        let dataset = dataset_store.load_dataset(&temp.dataset.name).await?;
        let catalog = Catalog::for_dataset(&dataset, config.data_store.clone())?;
        QueryContext::for_catalog(catalog, Arc::new(config.make_runtime_env()?))?
    };

    let temp_ctx = {
        let catalog = Catalog::for_dataset(&temp.dataset, temp.config.data_store.clone())?;
        QueryContext::for_catalog(catalog, Arc::new(temp.config.make_runtime_env()?))?
    };

    for table in blessed_ctx.catalog().all_tables() {
        let query = parse_sql(&format!(
            "select * from {} order by block_num",
            table.table_ref()
        ))?;
        let blessed_all_rows: RecordBatch = blessed_ctx
            .execute_and_concat(blessed_ctx.plan_sql(query.clone()).await?)
            .await?;
        let temp_all_rows: RecordBatch = temp_ctx
            .execute_and_concat(temp_ctx.plan_sql(query.clone()).await?)
            .await?;

        assert_batch_eq(&temp_all_rows, &blessed_all_rows);
    }

    Ok(())
}

/// Context for a dataset dumped to a temporary directory. The directory is deleted on drop.
pub struct TempDatasetDump {
    pub config: Arc<Config>,
    pub _temp_dir: TempDir,
    pub dataset: Dataset,
}

// Dump the dataset to a temporary data directory.
pub async fn temp_dump(
    dataset_name: &str,
    start: u64,
    end: u64,
) -> Result<TempDatasetDump, BoxError> {
    use figment::providers::Json;

    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path();
    info!("Dumping dataset to {}", path.display());

    let config_override = Some(Json::string(&format!(
        r#"{{ "data_dir": "{}" }}"#,
        path.display()
    )));

    let config = Arc::new(Config::load(TEST_CONFIG_PATH, false, config_override)?);

    dump(config.clone(), dataset_name, start, end).await?;

    let dataset_store = DatasetStore::new(config.clone());
    let dataset = dataset_store.load_dataset(&dataset_name).await?;

    Ok(TempDatasetDump {
        config,
        _temp_dir: temp_dir,
        dataset,
    })
}

async fn dump(
    config: Arc<Config>,
    dataset_name: &str,
    start: u64,
    end: u64,
) -> Result<(), BoxError> {
    let dataset_store = DatasetStore::new(config.clone());
    let partition_size = 1024 * 1024; // 100 kB
    let compression = Compression::ZSTD(ZstdLevel::try_new(1).unwrap());

    // Disable bloom filters, as they take over 10 MB per file, too large for files that we'd be
    // willing to commit to git.
    let parquet_opts = parquet_opts(compression, false);
    let env = Arc::new(config.make_runtime_env()?);

    // Clear the data dir.
    clear_dataset(&config, dataset_name).await?;

    dump_dataset(
        dataset_name,
        &dataset_store,
        &config,
        &env,
        1,
        partition_size,
        &parquet_opts,
        start,
        Some(end),
    )
    .await?;
    Ok(())
}

pub async fn check_blocks(dataset_name: &str, start: u64, end: u64) -> Result<(), BoxError> {
    let config = Arc::new(Config::load(TEST_CONFIG_PATH, false, None)?);
    let dataset_store = DatasetStore::new(config.clone());
    let env = Arc::new(config.make_runtime_env()?);

    dump_check::dump_check(
        dataset_name,
        &dataset_store,
        &config,
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
    let path = format!("config/providers/{}", filename);
    if matches!(
        fs::metadata(&path).await.map_err(|e| e.kind()),
        Err(ErrorKind::NotFound)
    ) {
        panic!(
                "Provider file '{path}' does not exist. To run this test, copy 'COPY_ME_{filename}' as '{filename}', \
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
