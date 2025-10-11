//! Service context
use std::sync::Arc;

use dataset_store::{DatasetStore, manifests::DatasetManifestsStore};
use metadata_db::MetadataDb;

use crate::scheduler::Scheduler;

/// The Admin API context
#[derive(Clone)]
pub struct Ctx {
    pub metadata_db: MetadataDb,
    pub dataset_store: Arc<DatasetStore>,
    pub dataset_manifests_store: DatasetManifestsStore,
    pub scheduler: Scheduler,
}
