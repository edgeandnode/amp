//! Daemon configuration and directory fixtures for isolated test environments.
//!
//! This fixture module consolidates configuration management functionality, including both
//! the configuration types/builders and the temporary directory management for test environments.

/// Daemon configuration for test environments.
///
/// Contains concrete configuration values that will be written to the generated config.toml file.
/// Most fields are guaranteed to have values, except `metadata_db_url` which is optional.
/// Use `DaemonConfigBuilder` to construct instances with custom values.
#[derive(Debug, Clone)]
pub struct DaemonConfig {
    manifests_dir: String,
    providers_dir: String,
    data_dir: String,
    metadata_db_url: Option<String>,
    max_mem_mb: u32,
    microbatch_max_interval: u64,
    flight_addr: String,
    jsonl_addr: String,
    registry_service_addr: String,
    admin_api_addr: String,
    partition_size_mb: u64,
    segment_compactor: bool,
    garbage_collector: bool,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            manifests_dir: "manifests".to_string(),
            providers_dir: "providers".to_string(),
            data_dir: "data".to_string(),
            metadata_db_url: None,
            max_mem_mb: 30_000,
            microbatch_max_interval: 100_000,
            flight_addr: "0.0.0.0:0".to_string(),
            jsonl_addr: "0.0.0.0:0".to_string(),
            registry_service_addr: "0.0.0.0:0".to_string(),
            admin_api_addr: "0.0.0.0:0".to_string(),
            partition_size_mb: 100,
            segment_compactor: false,
            garbage_collector: false,
        }
    }
}

impl DaemonConfig {
    /// Get the dataset manifests directory path.
    pub fn dataset_manifests_path(&self) -> &str {
        &self.manifests_dir
    }

    /// Get the providers directory path.
    pub fn provider_configs_path(&self) -> &str {
        &self.providers_dir
    }

    /// Get the data directory path.
    pub fn data_path(&self) -> &str {
        &self.data_dir
    }

    /// Serialize the configuration to TOML format.
    ///
    /// Creates a complete test configuration from this daemon configuration
    /// and serializes it to TOML format suitable for writing to config.toml.
    pub fn serialize_to_toml(&self) -> String {
        // Internal struct matching the expected TOML structure
        #[derive(serde::Serialize)]
        struct TestConfig {
            manifests_dir: String,
            providers_dir: String,
            data_dir: String,
            max_mem_mb: u32,
            microbatch_max_interval: u64,
            #[serde(skip_serializing_if = "Option::is_none")]
            metadata_db: Option<MetadataDbSection>,
            flight_addr: String,
            jsonl_addr: String,
            registry_service_addr: String,
            admin_api_addr: String,
            writer: WriterConfig,
        }

        #[derive(serde::Serialize)]
        struct MetadataDbSection {
            url: String,
        }

        #[derive(serde::Serialize)]
        struct WriterConfig {
            bytes: u64,
            compactor: CompactorConfig,
            garbage_collector: GarbageCollectorConfig,
        }

        #[derive(serde::Serialize)]
        struct CompactorConfig {
            active: bool,
        }

        #[derive(serde::Serialize)]
        struct GarbageCollectorConfig {
            active: bool,
        }

        toml::to_string_pretty(&TestConfig {
            manifests_dir: self.manifests_dir.clone(),
            providers_dir: self.providers_dir.clone(),
            data_dir: self.data_dir.clone(),
            max_mem_mb: self.max_mem_mb,
            microbatch_max_interval: self.microbatch_max_interval,
            metadata_db: self
                .metadata_db_url
                .clone()
                .map(|url| MetadataDbSection { url }),
            flight_addr: self.flight_addr.clone(),
            jsonl_addr: self.jsonl_addr.clone(),
            registry_service_addr: self.registry_service_addr.clone(),
            admin_api_addr: self.admin_api_addr.clone(),
            writer: WriterConfig {
                bytes: self.partition_size_mb * 1024 * 1024,
                compactor: CompactorConfig {
                    active: self.segment_compactor,
                },
                garbage_collector: GarbageCollectorConfig {
                    active: self.garbage_collector,
                },
            },
        })
        .expect("Failed to serialize test config to TOML")
    }
}

/// Builder for creating DaemonConfig with a fluent API.
///
/// This builder allows setting specific configuration values while leaving others at their defaults.
/// Values not explicitly set will use the default values defined in `DaemonConfig::default()`.
/// The builder accepts optional values and applies defaults at build time.
#[derive(Debug, Default)]
pub struct DaemonConfigBuilder {
    manifests_dir: Option<String>,
    providers_dir: Option<String>,
    data_dir: Option<String>,
    metadata_db_url: Option<String>,
    max_mem_mb: Option<u32>,
    microbatch_max_interval: Option<u64>,
    flight_addr: Option<String>,
    jsonl_addr: Option<String>,
    registry_service_addr: Option<String>,
    admin_api_addr: Option<String>,
    partition_size_mb: Option<u64>,
    segment_compactor: bool,
    garbage_collector: bool,
}

impl DaemonConfigBuilder {
    /// Create a new DaemonConfig builder.
    ///
    /// All configuration values will use their defaults unless explicitly overridden
    /// using the builder methods.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new [`DaemonConfig`] builder from an existing config.
    ///
    /// Creates a builder instance pre-populated with all values from the provided
    /// configuration. This allows you to modify specific fields while keeping
    /// others unchanged.
    pub fn from_config(config: &DaemonConfig) -> Self {
        Self {
            manifests_dir: Some(config.manifests_dir.clone()),
            providers_dir: Some(config.providers_dir.clone()),
            data_dir: Some(config.data_dir.clone()),
            metadata_db_url: config.metadata_db_url.clone(),
            max_mem_mb: Some(config.max_mem_mb),
            microbatch_max_interval: Some(config.microbatch_max_interval),
            flight_addr: Some(config.flight_addr.clone()),
            jsonl_addr: Some(config.jsonl_addr.clone()),
            registry_service_addr: Some(config.registry_service_addr.clone()),
            admin_api_addr: Some(config.admin_api_addr.clone()),
            partition_size_mb: Some(config.partition_size_mb),
            segment_compactor: config.segment_compactor,
            garbage_collector: config.garbage_collector,
        }
    }

    /// Set dataset definitions directory.
    ///
    /// Directory containing dataset definition files.
    ///
    /// **Default**: `".amp/manifests"`
    pub fn manifests_dir(mut self, dir: impl Into<Option<String>>) -> Self {
        self.manifests_dir = dir.into();
        self
    }

    /// Set providers directory.
    ///
    /// Directory containing provider configuration files.
    ///
    /// **Default**: `".amp/providers"`
    pub fn providers_dir(mut self, dir: impl Into<Option<String>>) -> Self {
        self.providers_dir = dir.into();
        self
    }

    /// Set data directory.
    ///
    /// Directory for storing dataset files and output data.
    ///
    /// **Default**: `".amp/data"`
    pub fn data_dir(mut self, dir: impl Into<Option<String>>) -> Self {
        self.data_dir = dir.into();
        self
    }

    /// Set metadata database URL.
    ///
    /// PostgreSQL connection URL for metadata storage.
    ///
    /// **Default**: `None` (optional field, will be skipped in TOML if not set)
    pub fn metadata_db_url<U>(mut self, url: impl Into<Option<U>>) -> Self
    where
        U: AsRef<str>,
    {
        self.metadata_db_url = url.into().map(|url| url.as_ref().to_string());
        self
    }

    /// Set maximum memory in MB.
    ///
    /// Controls the memory limit for the test environment's Amp instance.
    ///
    /// **Default**: `30_000` MB
    pub fn max_mem_mb(mut self, max_mem_mb: impl Into<Option<u32>>) -> Self {
        self.max_mem_mb = max_mem_mb.into();
        self
    }

    /// Set microbatch maximum interval.
    ///
    /// Controls the microbatch processing interval for data ingestion.
    ///
    /// **Default**: `100_000` blocks
    pub fn microbatch_max_interval(mut self, interval: impl Into<Option<u64>>) -> Self {
        self.microbatch_max_interval = interval.into();
        self
    }

    /// Set Flight server address.
    ///
    /// Address for the Apache Arrow Flight gRPC server endpoint.
    ///
    /// **Default**: `"0.0.0.0:0"` (bind to any interface, auto-assign port)
    pub fn flight_addr(mut self, addr: impl Into<Option<String>>) -> Self {
        self.flight_addr = addr.into();
        self
    }

    /// Set JSON Lines server address.
    ///
    /// Address for the JSON Lines HTTP API endpoint.
    ///
    /// **Default**: `"0.0.0.0:0"` (bind to any interface, auto-assign port)
    pub fn jsonl_addr(mut self, addr: impl Into<Option<String>>) -> Self {
        self.jsonl_addr = addr.into();
        self
    }

    /// Set registry service address.
    ///
    /// Address for the dataset registry service endpoint.
    ///
    /// **Default**: `"0.0.0.0:0"` (bind to any interface, auto-assign port)
    pub fn registry_service_addr(mut self, addr: impl Into<Option<String>>) -> Self {
        self.registry_service_addr = addr.into();
        self
    }

    /// Set admin API address.
    ///
    /// Address for the administrative HTTP API endpoint.
    ///
    /// **Default**: `"0.0.0.0:0"` (bind to any interface, auto-assign port)
    pub fn admin_api_addr(mut self, addr: impl Into<Option<String>>) -> Self {
        self.admin_api_addr = addr.into();
        self
    }

    /// Activate or deactivate the segment compactor.
    ///
    /// **Default**: `false`
    pub fn segment_compactor(mut self, active: bool) -> Self {
        self.segment_compactor = active;
        self
    }

    /// Activate or deactivate the garbage collector.
    ///
    /// **Default**: `false`
    pub fn garbage_collector(mut self, active: bool) -> Self {
        self.garbage_collector = active;
        self
    }

    /// Set partition size in MB.
    ///
    /// Controls the size of data partitions.
    /// **Default**: `100` MB
    pub fn partition_size_mb(mut self, size_mb: impl Into<Option<u64>>) -> Self {
        self.partition_size_mb = size_mb.into();
        self
    }

    /// Build the DaemonConfig instance.
    ///
    /// Creates a `DaemonConfig` with default values, then applies any values that were
    /// explicitly set on this builder. Unset values remain at their defaults.
    ///
    /// This method consumes the builder and returns the final configuration.
    pub fn build(self) -> DaemonConfig {
        let mut config = DaemonConfig::default();

        if let Some(value) = self.manifests_dir {
            config.manifests_dir = value;
        }
        if let Some(value) = self.providers_dir {
            config.providers_dir = value;
        }
        if let Some(value) = self.data_dir {
            config.data_dir = value;
        }
        config.metadata_db_url = self.metadata_db_url;
        if let Some(value) = self.max_mem_mb {
            config.max_mem_mb = value;
        }
        if let Some(value) = self.microbatch_max_interval {
            config.microbatch_max_interval = value;
        }
        if let Some(value) = self.flight_addr {
            config.flight_addr = value;
        }
        if let Some(value) = self.jsonl_addr {
            config.jsonl_addr = value;
        }
        if let Some(value) = self.registry_service_addr {
            config.registry_service_addr = value;
        }
        if let Some(value) = self.admin_api_addr {
            config.admin_api_addr = value;
        }
        if let Some(value) = self.partition_size_mb {
            config.partition_size_mb = value;
        }
        config.segment_compactor = self.segment_compactor;
        config.garbage_collector = self.garbage_collector;

        config
    }
}
