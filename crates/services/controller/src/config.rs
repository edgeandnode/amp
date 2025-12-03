use std::sync::Arc;

use common::{config::BuildInfo, store::Store};

/// Controller-specific configuration
///
/// Contains only the configuration fields needed by the controller service
/// for dataset management and job scheduling.
#[derive(Debug, Clone)]
pub struct Config {
    /// Object store for output data
    pub data_store: Arc<Store>,
    /// Build information (passed to admin-api)
    pub build_info: BuildInfo,
}
