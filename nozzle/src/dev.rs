use std::{
    io::Write as _,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{anyhow, Context as _};
use indoc::formatdoc;

pub struct Nozzle {
    dir: PathBuf,
}

impl Nozzle {
    pub fn new(dir: PathBuf) -> anyhow::Result<Self> {
        let nozzle = Self { dir };
        write_file(
            &nozzle.root_config(),
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

    pub fn add_rpc_dataset(&self, url: &str) -> anyhow::Result<()> {
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
        Ok(())
    }

    pub fn service(&self) -> anyhow::Result<server::service::Service> {
        let config =
            Arc::new(common::config::Config::load(self.root_config(), true, None).unwrap());
        // TODO: cache service creation, invalidate on config/dataset changes
        server::service::Service::new(config, None).map_err(|err| anyhow!(err))
    }

    fn root_config(&self) -> PathBuf {
        self.dir.join("config.toml")
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
