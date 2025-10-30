mod adbc;
mod container;
mod driver;

#[cfg(test)]
mod tests;

use std::sync::Arc;

pub use adbc::DriverOpts;
use amp_client::SqlClient;
use common::{config::Config as CommonConfig, query_context::QueryEnv};
use dataset_store::{
    DatasetStore, manifests::DatasetManifestsStore, providers::ProviderConfigsStore,
};
use driver::{Drivers, serde_vendors};
use serde::{Deserialize, Serialize};

use crate::{Connection, Error, Result, Schema, SupportedVendor, arrow::Schema as ArrowSchema};

#[derive(Debug, Clone)]
pub struct Config {
    pub common: CommonConfig,
    pub drivers: Drivers,
}

impl Config {
    pub async fn load(
        config_path: impl Into<std::path::PathBuf>,
        ampdbc_config_path: impl Into<std::path::PathBuf>,
    ) -> Result<Self> {
        let common = CommonConfig::load(config_path, true, None, false).await?;
        let AmpdbcConfig { drivers } = AmpdbcConfig::load(ampdbc_config_path).await?;

        Ok(Self { common, drivers })
    }

    pub fn new(common: CommonConfig, drivers: Drivers) -> Self {
        Self { common, drivers }
    }

    pub async fn sql_client(&self) -> Result<SqlClient> {
        let client = SqlClient::new(&format!("grpc://{}", self.common.addrs.flight_addr)).await?;
        Ok(client)
    }

    pub fn provider_configs_store(&self) -> ProviderConfigsStore {
        ProviderConfigsStore::new(self.common.providers_store.object_store())
    }

    pub fn dataset_manifests_store(&self) -> DatasetManifestsStore {
        DatasetManifestsStore::new(self.common.manifests_store.object_store())
    }

    pub async fn dataset_store(&self) -> Result<Arc<DatasetStore>> {
        let metadata_db = self.common.metadata_db().await?;
        let provider_configs_store = self.provider_configs_store();
        let dataset_manifests_store = self.dataset_manifests_store();

        Ok(DatasetStore::new(
            metadata_db,
            provider_configs_store,
            dataset_manifests_store,
        ))
    }

    pub fn query_env(&self) -> Result<QueryEnv> {
        self.common.make_query_env().map_err(Error::query_env)
    }

    pub fn connections(&mut self) -> Result<Vec<Connection>> {
        self.drivers_mut().try_into()
    }

    pub fn schemas(&self, arrow_schema: &ArrowSchema) -> impl Iterator<Item = Schema> {
        use SupportedVendor::*;
        let schema_ref = Arc::from(arrow_schema.clone());

        let vendors = self.drivers.vendors();
        let table_refs = self.drivers.table_refs();

        vendors
            .zip(table_refs)
            .map(move |(vendor, table_ref)| match vendor {
                BigQuery => Schema::bigquery(&schema_ref, table_ref, Default::default()),
                Snowflake => Schema::snowflake(&schema_ref, table_ref, Default::default()),
                _ => unimplemented!(),
            })
    }

    pub fn drivers(&self) -> &Drivers {
        &self.drivers
    }

    pub fn drivers_mut(&mut self) -> &mut Drivers {
        &mut self.drivers
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct AmpdbcConfig {
    #[serde(flatten, with = "serde_vendors")]
    pub drivers: Drivers,
}

impl AmpdbcConfig {
    pub async fn load(config_path: impl Into<std::path::PathBuf>) -> Result<Self> {
        let config_path = config_path.into();
        let contents = tokio::fs::read_to_string(&config_path)
            .await
            .map_err(Error::io_error(config_path))?;
        let config: AmpdbcConfig = toml::from_str(&contents)?;
        Ok(config)
    }
}
