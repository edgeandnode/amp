use std::{
    collections::HashSet,
    io::Write as _,
    path::{Path, PathBuf},
    sync::Arc,
};

use alloy::primitives::BlockNumber;
use anyhow::{anyhow, Context as _};
use common::{
    manifest::{Manifest, TableSchema},
    query_context::parse_sql,
};
use dataset_store::DatasetStore;
use indoc::formatdoc;

use crate::datasets_and_dependencies;

pub struct Nozzle {
    dir: PathBuf,
    datasets: HashSet<String>,
}

impl Nozzle {
    pub fn new(dir: PathBuf) -> anyhow::Result<Self> {
        let nozzle = Self {
            dir,
            datasets: Default::default(),
        };
        write_file(
            &nozzle.dir.join("config.toml"),
            formatdoc! {r#"
                data_dir = "data"
                dataset_defs_dir = "datasets"
                providers_dir = "providers"
                max_mem_mb = 2000
                spill_location = []
            "#}
            .as_bytes(),
        )?;

        let data_dir = nozzle.dir.join("data");
        match std::fs::remove_dir_all(&data_dir) {
            Ok(()) => (),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => (),
            Err(err) => return Err(err).context(anyhow!("delete dir {}", data_dir.display())),
        };
        std::fs::create_dir_all(&data_dir).context(anyhow!("create dir {}", data_dir.display()))?;

        Ok(nozzle)
    }

    pub fn add_rpc_dataset(&mut self, name: &str, url: &str) -> anyhow::Result<()> {
        write_file(
            &self.dir.join(format!("providers/{name}.toml")),
            formatdoc! {r#"
                    url = "{url}"
                "#}
            .as_bytes(),
        )?;
        write_file(
            &self.dir.join(format!("datasets/{name}.toml")),
            formatdoc! {r#"
                    name = "{name}"
                    network = "{name}"
                    kind = "evm-rpc"
                    provider = "{name}.toml"
                "#}
            .as_bytes(),
        )?;
        self.datasets.insert(name.into());
        Ok(())
    }

    pub fn add_manifest_dataset(&mut self, manifest: Manifest) -> anyhow::Result<()> {
        let dataset = manifest.name.clone();
        let mut object = serde_json::to_value(manifest).unwrap();
        object
            .as_object_mut()
            .unwrap()
            .insert("kind".into(), "manifest".into());
        write_file(
            &self.dir.join(format!("datasets/{}.json", dataset)),
            serde_json::to_string_pretty(&object).unwrap().as_bytes(),
        )?;
        self.datasets.insert(dataset);
        Ok(())
    }

    pub async fn schema(&self, sql: &str) -> anyhow::Result<TableSchema> {
        let query = parse_sql(sql)?;
        let dataset_store = DatasetStore::new(self.config(), None);
        let ctx = dataset_store.planning_ctx_for_sql(&query).await?;
        let schema = ctx.sql_output_schema(query).await?;
        Ok(schema.into())
    }

    pub fn service(&self) -> anyhow::Result<server::service::Service> {
        let config = self.config();
        // TODO: cache service creation, invalidate on config/dataset changes
        server::service::Service::new(config, None).map_err(|err| anyhow!(err))
    }

    pub async fn dump_datasets(&self, end_block: Option<BlockNumber>) -> anyhow::Result<()> {
        let config = self.config();
        let dataset_store = DatasetStore::new(config.clone(), None);
        let parquet_opts =
            dump::parquet_opts(common::parquet::basic::Compression::UNCOMPRESSED, true);
        let datasets =
            datasets_and_dependencies(&dataset_store, self.datasets.iter().cloned().collect())
                .await
                .map_err(|err| anyhow!(err))?;
        for dataset in datasets {
            dump::dump_dataset(
                &dataset,
                &dataset_store,
                &config,
                None,
                1,
                1,
                &parquet_opts,
                0,
                end_block,
            )
            .await
            .map_err(|err: common::BoxError| anyhow!(err))?;
        }
        Ok(())
    }

    fn config(&self) -> Arc<common::config::Config> {
        Arc::new(common::config::Config::load(self.dir.join("config.toml"), true, None).unwrap())
    }
}

fn write_file(path: &Path, data: &[u8]) -> anyhow::Result<()> {
    let dir = path
        .parent()
        .context(anyhow!("invalid path: {}", path.display()))?;
    std::fs::create_dir_all(dir).context(anyhow!("create dir {}", dir.display()))?;
    let mut file =
        std::fs::File::create(path).context(anyhow!("create file: {}", path.display()))?;
    file.write_all(data)
        .context(anyhow!("write to file: {}", path.display()))?;
    Ok(())
}
