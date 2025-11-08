use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(name = "ampsync")]
#[command(version)]
#[command(about = "PostgreSQL synchronization tool for Amp datasets", long_about = None)]
pub struct Config {
    /// Dataset name to sync (required)
    ///
    /// Can also be set via DATASET_NAME environment variable
    #[arg(short = 'd', long, env = "DATASET_NAME", required = true)]
    pub dataset_name: String,

    /// Dataset version to sync (default: "latest")
    ///
    /// Can also be set via DATASET_VERSION environment variable
    #[arg(short = 'v', long, env = "DATASET_VERSION", default_value = "latest")]
    pub dataset_version: String,

    /// PostgreSQL connection URL (required)
    ///
    /// Format: postgresql://[user]:[password]@[host]:[port]/[database]
    /// Can also be set via DATABASE_URL environment variable
    #[arg(long, env = "DATABASE_URL", required = true)]
    pub database_url: String,

    /// Amp Arrow Flight server address (default: http://localhost:1602)
    ///
    /// Can also be set via AMP_FLIGHT_ADDR environment variable
    #[arg(long, env = "AMP_FLIGHT_ADDR", default_value = "http://localhost:1602")]
    pub amp_flight_addr: String,

    /// Amp Admin API server address (default: http://localhost:1610)
    ///
    /// Can also be set via AMP_ADMIN_API_ADDR environment variable
    #[arg(
        long,
        env = "AMP_ADMIN_API_ADDR",
        default_value = "http://localhost:1610"
    )]
    pub amp_admin_api_addr: String,

    /// Maximum database connections (default: 10, valid range: 1-1000)
    ///
    /// Can also be set via MAX_DB_CONNECTIONS environment variable
    #[arg(long, env = "MAX_DB_CONNECTIONS", default_value_t = 10, value_parser = clap::value_parser!(u32).range(1..=1000))]
    pub max_db_connections: u32,

    /// Retention window in blocks for watermark buffer (default: 128, minimum: 64)
    ///
    /// Can also be set via RETENTION_BLOCKS environment variable
    #[arg(long, env = "RETENTION_BLOCKS", default_value_t = 128, value_parser = clap::value_parser!(u64).range(64..))]
    pub retention_blocks: u64,

    /// Maximum retry attempts for manifest fetch (default: 5, valid range: 1-20)
    ///
    /// Can also be set via MANIFEST_FETCH_MAX_RETRIES environment variable
    #[arg(long, env = "MANIFEST_FETCH_MAX_RETRIES", default_value_t = 5, value_parser = clap::value_parser!(u32).range(1..=20))]
    pub manifest_fetch_max_retries: u32,
}
