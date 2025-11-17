//! Service context
use std::sync::Arc;

use common::config::Config;
use dataset_store::DatasetStore;
use metadata_db::MetadataDb;

use crate::scheduler::Scheduler;

/// The Admin API context
#[derive(Clone)]
pub struct Ctx {
    pub metadata_db: MetadataDb,
    pub dataset_store: Arc<DatasetStore>,
    pub scheduler: Arc<dyn Scheduler>,
    pub config: Arc<Config>,
}
