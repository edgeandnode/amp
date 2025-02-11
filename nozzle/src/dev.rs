use std::{
    collections::HashSet,
    io::Write as _,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use alloy::{json_abi::JsonAbi, primitives::BlockNumber, rpc::client::ReqwestClient as RpcClient};
use anyhow::{anyhow, Context as _};
use common::manifest::{ArrowSchema, Manifest};
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

    pub fn add_rpc_dataset(&mut self, name: &str, url: &str) -> anyhow::Result<()> {
        write_file(
            &self.dir.join(format!("providers/{name}.toml")),
            formatdoc! {r#"
                    url = "{url}"
                "#}
            .as_bytes(),
        )?;
        write_file(
            &self.dir.join("datasets/{name}.toml"),
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

pub fn load_artifact_manifests(dir: &Path) -> anyhow::Result<Vec<Manifest>> {
    let mut manifests: Vec<Manifest> = Default::default();
    for entry in std::fs::read_dir(dir).context(anyhow!("read {}", dir.display()))? {
        let entry = entry.context(anyhow!("read {}", dir.display()))?;
        let metadata = entry
            .metadata()
            .context(anyhow!("read {}", entry.path().display()))?;
        if !(metadata.is_dir() && entry.file_name().to_string_lossy().ends_with(".sol")) {
            continue;
        }
        let dir = entry.path();
        for entry in std::fs::read_dir(&dir).context(anyhow!("read {}", dir.display()))? {
            let entry = entry.context(anyhow!("read {}", dir.display()))?;
            let metadata = entry
                .metadata()
                .context(anyhow!("read {}", entry.path().display()))?;
            if !(metadata.is_file() && entry.file_name().to_string_lossy().ends_with(".json")) {
                continue;
            }
            let dataset =
                camelcase_to_snakecase(&entry.file_name().to_string_lossy().replace(".json", ""));
            if manifests.iter().any(|m| m.name == dataset) {
                log::warn!(
                    "skipping duplicate dataset from path {}",
                    entry.path().display()
                );
                continue;
            }
            let abi = load_contract_abi(&entry.path())?;
            let events = match filter_abi_events(&abi) {
                Ok(events) => events,
                Err(err) => {
                    log::info!("skipping contract dataset '{dataset}': {err}");
                    continue;
                }
            };
            for event in &events {
                log::info!(
                    "adding table {}.{} for {}",
                    dataset,
                    camelcase_to_snakecase(&event.name),
                    event.full_signature(),
                );
            }
            // TODO: set the correct schema
            manifests.push(Manifest {
                name: dataset,
                version: semver::Version::new(0, 0, 0),
                dependencies: [(
                    "anvil".into(),
                    common::manifest::Dependency {
                        owner: "".into(),
                        name: "anvil".into(),
                        version: "*".parse().unwrap(),
                    },
                )]
                .into(),
                tables: events
                    .into_iter()
                    .map(|event| {
                        (
                            camelcase_to_snakecase(&event.name),
                            common::manifest::Table {
                                input: common::manifest::TableInput::View(common::manifest::View {
                                    sql: sql_for_event(&event.full_signature()),
                                }),
                                schema: common::manifest::TableSchema {
                                    arrow: ArrowSchema {
                                        fields: Default::default(),
                                    },
                                },
                            },
                        )
                    })
                    .collect(),
            });
        }
    }
    Ok(manifests)
}

fn camelcase_to_snakecase(name: &str) -> String {
    let mut result = String::new();
    let mut prev_char_was_upper = false;
    for (i, c) in name.chars().enumerate() {
        if c.is_uppercase() {
            if (i != 0) && !prev_char_was_upper {
                result.push('_');
            }
            result.push(c.to_ascii_lowercase());
            prev_char_was_upper = true;
        } else {
            result.push(c);
            prev_char_was_upper = false;
        }
    }
    result
}

fn sql_for_event(signature: &str) -> String {
    formatdoc! {r#"
        SELECT
            l.block_num,
            l.timestamp,
            l.address,
            evm_decode(l.topic1, l.topic2, l.topic3, l.data, '{signature}') AS event
        FROM
            anvil.logs l
        WHERE
            l.topic0 = evm_topic('{signature}')
    "#}
}

fn load_contract_abi(file: &Path) -> anyhow::Result<JsonAbi> {
    #[derive(serde::Deserialize)]
    struct ContractArtifact {
        abi: JsonAbi,
    }
    let file = std::fs::File::open(file).context(anyhow!("read {}", file.display()))?;
    let artifact: ContractArtifact =
        serde_json::from_reader(&file).context("parse contract artifact")?;
    Ok(artifact.abi)
}

fn filter_abi_events(abi: &JsonAbi) -> anyhow::Result<Vec<&alloy::json_abi::Event>> {
    // Nozzle dump seems to fail if any events in a dataset contain a type that cannot be converted
    // to an arrow data type.
    for event in abi.events() {
        for input_type in event.inputs.iter().map(|i| i.selector_type()) {
            anyhow::ensure!(
                !input_type.ends_with("[]"),
                "unsupported event: {}: unsupported type {}",
                event.full_signature().replace("event ", ""),
                input_type
            );
        }
    }
    anyhow::ensure!(!abi.events.is_empty(), "no events");
    Ok(abi.events().collect())
}

#[cfg(test)]
mod test {
    #[test]
    fn camelcase_to_snakecase() {
        let tests = [
            ("IERC721Enumerable", "ierc721_enumerable"),
            ("MockERC721", "mock_erc721"),
            ("stdStorageSafe", "std_storage_safe"),
            ("Vm", "vm"),
            ("VmSafe", "vm_safe"),
        ];
        for (input, expected) in tests {
            assert_eq!(&super::camelcase_to_snakecase(input), expected);
        }
    }
}
