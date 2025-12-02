use clap::{Args, Parser, Subcommand};
use datasets_common::partial_reference::PartialReference;

#[derive(Parser, Debug)]
#[command(name = "ampsync")]
#[command(version)]
#[command(about = "PostgreSQL synchronization tool for Amp datasets")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Synchronize dataset to PostgreSQL
    Sync(SyncConfig),
}

#[derive(Args, Debug, Clone)]
pub struct SyncConfig {
    /// Dataset reference to sync
    ///
    /// Supports flexible formats:
    ///   - Full: namespace/name@revision (e.g., _/eth_rpc@1.0.0)
    ///   - No namespace: name@revision (e.g., eth_rpc@1.0.0, defaults to _ namespace)
    ///   - No revision: namespace/name (e.g., _/eth_rpc, defaults to latest)
    ///   - Minimal: name (e.g., eth_rpc, defaults to _/eth_rpc@latest)
    ///
    /// Revision can be:
    ///   - Semantic version (e.g., 1.0.0)
    ///   - Hash (64-character hex string)
    ///   - 'latest' (resolves to latest version)
    ///   - 'dev' (resolves to development version)
    ///
    /// Can also be set via DATASET environment variable
    #[arg(short = 'd', long, env = "DATASET", required = true)]
    pub dataset: PartialReference,

    /// Tables to synchronize (required, comma-separated)
    ///
    /// Specify which tables from the dataset to sync to PostgreSQL.
    /// Example: --tables logs,blocks,transactions
    ///
    /// Can also be set via TABLES environment variable
    #[arg(
        short = 't',
        long,
        env = "TABLES",
        required = true,
        value_delimiter = ',',
        num_args = 1..
    )]
    pub tables: Vec<String>,

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

    /// Authentication token for Arrow Flight
    ///
    /// Bearer token for authenticating requests to the Arrow Flight server (for data streaming).
    ///
    /// The token will be sent as an Authorization header: `Authorization: Bearer <token>`
    ///
    /// Can also be set via AMP_AUTH_TOKEN environment variable
    #[arg(long, env = "AMP_AUTH_TOKEN")]
    pub auth_token: Option<String>,

    /// Health check server port (optional)
    ///
    /// When provided, starts an HTTP server on 0.0.0.0 that exposes a /healthz
    /// endpoint.
    ///
    /// Example: --health-port 8080
    ///
    /// Can also be set via HEALTH_PORT environment variable
    #[arg(long, env = "HEALTH_PORT")]
    pub health_port: Option<u16>,
}
