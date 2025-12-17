use config::BuildInfo;

/// Controller-specific configuration
///
/// Contains only the configuration fields needed by the controller service
/// for dataset management and job scheduling.
#[derive(Debug, Clone)]
pub struct Config {
    /// Build information (passed to admin-api)
    pub build_info: BuildInfo,
}
