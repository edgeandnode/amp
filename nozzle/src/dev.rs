use std::{
    collections::HashSet,
    io::Write as _,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use alloy::{primitives::BlockNumber, rpc::client::ReqwestClient as RpcClient};
use anyhow::{anyhow, Context as _};
use dataset_store::DatasetStore;
use indoc::formatdoc;
use tokio::sync::watch;

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

    pub fn add_rpc_dataset(&mut self, url: &str) -> anyhow::Result<()> {
        write_file(
            &self.dir.join("providers/anvil.toml"),
            formatdoc! {r#"
                    url = "{url}"
                "#}
            .as_bytes(),
        )?;
        write_file(
            &self.dir.join("datasets/anvil.toml"),
            formatdoc! {r#"
                    name = "anvil"
                    network = "local"
                    kind = "evm-rpc"
                    provider = "anvil.toml"
                "#}
            .as_bytes(),
        )?;
        self.datasets.insert("anvil".into());
        Ok(())
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

pub fn watch_chain_head(rpc: RpcClient) -> watch::Receiver<Option<BlockNumber>> {
    let (tx, rx) = watch::channel(None);
    tokio::spawn(async move {
        loop {
            match fetch_latest_block_number(&rpc).await {
                Ok(latest_block) => {
                    tx.send_if_modified(|value| {
                        if *value == Some(latest_block) {
                            return false;
                        }
                        *value = Some(latest_block);
                        true
                    });
                }
                Err(err) => {
                    log::error!("RPC error: {err}");
                }
            };
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
    rx
}

async fn fetch_latest_block_number(rpc: &RpcClient) -> anyhow::Result<BlockNumber> {
    #[derive(Debug, serde::Deserialize)]
    struct Response {
        number: String,
    }
    let response: Response = rpc
        .request("eth_getBlockByNumber", ("latest", false))
        .await?;
    u64::from_str_radix(response.number.trim_start_matches("0x"), 16)
        .context("failed to parse RPC response")
}
