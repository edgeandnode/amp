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

use crate::{
    Connection, Error, Result, Schema, SupportedVendor,
    arrow::Schema as ArrowSchema,
    cli::{AuthType, CreateMode},
    config::driver::DriverConfig,
};

#[derive(Debug, Clone)]
pub struct AmpdbcConfig {
    pub common: CommonConfig,
    pub drivers: Drivers,
}

impl AmpdbcConfig {
    pub async fn load(
        config_path: impl Into<std::path::PathBuf>,
        ampdbc_config_path: impl Into<std::path::PathBuf>,
        auth_type: AuthType,
        create_mode: crate::cli::CreateMode,
    ) -> Result<Self> {
        let common = CommonConfig::load(config_path, true, None, false).await?;
        let AmpdbcConfigFile { drivers } =
            AmpdbcConfigFile::load(ampdbc_config_path, auth_type, create_mode).await?;
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
        ProviderConfigsStore::new(self.common.providers_store.prefixed_store())
    }

    pub fn dataset_manifests_store(&self) -> DatasetManifestsStore {
        DatasetManifestsStore::new(self.common.manifests_store.prefixed_store())
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
        use DriverOpts::*;
        let schema_ref = Arc::from(arrow_schema.clone());

        let table_refs = self.drivers.table_refs();

        self.drivers
            .iter()
            .zip(table_refs)
            .map(move |(DriverConfig { opts, .. }, table_ref)| match opts {
                BigQuery { .. } => Schema::bigquery(&schema_ref, table_ref, Default::default()),
                Snowflake { ddl_safety, .. } => {
                    Schema::snowflake(&schema_ref, table_ref, Default::default(), *ddl_safety)
                }
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
pub struct AmpdbcConfigFile {
    #[serde(flatten, with = "serde_vendors")]
    pub drivers: Drivers,
}

impl AmpdbcConfigFile {
    pub async fn load(
        config_path: impl Into<std::path::PathBuf>,
        auth_type: AuthType,
        create_mode: CreateMode,
    ) -> Result<Self> {
        let config_path = config_path.into();
        let contents = tokio::fs::read_to_string(&config_path)
            .await
            .map_err(Error::io_error(config_path))?;
        let mut config: AmpdbcConfigFile = toml::from_str(&contents)?;
        config
            .drivers
            .iter_mut_by_vendor(SupportedVendor::Snowflake)
            .for_each(|driver_config| {
                tracing::debug!(
                    "Updating Snowflake driver config with auth_type: {:?}, create_mode: {:?}",
                    auth_type,
                    create_mode
                );
                driver_config.update_auth_type(auth_type);
                driver_config.update_create_mode(create_mode);
            });
        Ok(config)
    }
}
