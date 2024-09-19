#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) mod test_support {
    use std::{io::ErrorKind, sync::Arc};

    use common::{
        config::Config,
        parquet::basic::{Compression, ZstdLevel},
        BoxError,
    };
    use dataset_store::DatasetStore;
    use futures::{stream::TryStreamExt, StreamExt as _};
    use object_store::path::Path;

    use dump::{dump_dataset, parquet_opts};
    use tokio::fs;

    pub async fn bless(dataset_name: &str, start: u64, end: u64) -> Result<(), BoxError> {
        check_provider_files().await;
        let config = Arc::new(Config::load("config/config.toml", false)?);
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

    #[cfg(test)]
    pub async fn check_blocks(dataset_name: &str, start: u64, end: u64) -> Result<(), BoxError> {
        let config = Arc::new(Config::load("config/config.toml", false)?);
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

    async fn check_provider_files() {
        let expected_providers = ["firehose_eth_mainnet.toml"];

        for provider in expected_providers {
            let path = format!("config/providers/{}", provider);
            if matches!(
                fs::metadata(&path).await.map_err(|e| e.kind()),
                Err(ErrorKind::NotFound)
            ) {
                panic!(
                "Provider file {path} does not exist. To run tests, copy `COPY_ME_{provider}` as `{provider}`, \
                 filling in the required endpoints and credentials.",
            );
            }
        }
    }
}
