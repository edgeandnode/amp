//! Service context
use std::sync::Arc;

use amp_data_store::DataStore;
use amp_datasets_registry::DatasetsRegistry;
use amp_providers_registry::ProvidersRegistry;
use common::{datasets_cache::DatasetsCache, ethcall_udfs_cache::EthCallUdfsCache};
use metadata_db::MetadataDb;

use crate::{build_info::BuildInfo, scheduler::Scheduler};

/// The Admin API context
#[derive(Clone)]
pub struct Ctx {
    pub metadata_db: MetadataDb,
    /// Datasets registry for manifest and version tag operations.
    pub datasets_registry: DatasetsRegistry,
    /// Providers registry for provider configuration operations.
    pub providers_registry: ProvidersRegistry,
    /// Datasets cache for loading datasets.
    pub datasets_cache: DatasetsCache,
    /// EthCall UDFs cache for eth_call UDF creation.
    pub ethcall_udfs_cache: EthCallUdfsCache,
    /// Job scheduler for triggering and reconciling dataset sync jobs.
    pub scheduler: Arc<dyn Scheduler>,
    /// Object store for output data (used by dataset restore handler)
    pub data_store: DataStore,
    /// Build information (version, git SHA, etc.)
    pub build_info: BuildInfo,
}
