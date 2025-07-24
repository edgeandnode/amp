//! Service context
use std::sync::Arc;

use common::config::Config;
use dataset_store::DatasetStore;
use metadata_db::MetadataDb;

use crate::scheduler::Scheduler;

/// The Admin API context
#[derive(Clone)]
pub struct Ctx {
    pub _config: Arc<Config>,
    pub metadata_db: MetadataDb,
    pub store: Arc<DatasetStore>,
    pub scheduler: Scheduler,
}
