//! Service context
use std::sync::Arc;

use common::store::Store;
use config::BuildInfo;
use dataset_store::DatasetStore;
use metadata_db::MetadataDb;

use crate::scheduler::Scheduler;

/// The Admin API context
#[derive(Clone)]
pub struct Ctx {
    pub metadata_db: MetadataDb,
    pub dataset_store: DatasetStore,
    pub scheduler: Arc<dyn Scheduler>,
    /// Object store for output data (used by dataset restore handler)
    pub data_store: Arc<Store>,
    /// Build information (version, git SHA, etc.)
    pub build_info: BuildInfo,
}
