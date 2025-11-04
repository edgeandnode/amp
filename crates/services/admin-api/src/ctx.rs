//! Service context
use std::sync::Arc;

use dataset_store::DatasetStore;
use metadata_db::MetadataDb;

use crate::scheduler::JobScheduler;

/// The Admin API context
#[derive(Clone)]
pub struct Ctx {
    pub metadata_db: MetadataDb,
    pub dataset_store: Arc<DatasetStore>,
    pub scheduler: Arc<dyn JobScheduler>,
}
