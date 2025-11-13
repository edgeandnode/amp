use ampctl::client::BearerToken;
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

    /// Maximum backoff duration in seconds for manifest fetch retries (default: 60)
    ///
    /// Retries continue indefinitely for transient errors, but backoff is capped at this value.
    /// Can also be set via MANIFEST_FETCH_MAX_BACKOFF_SECS environment variable
    #[arg(long, env = "MANIFEST_FETCH_MAX_BACKOFF_SECS", default_value_t = 60)]
    pub manifest_fetch_max_backoff_secs: u64,

    /// Authentication token for Admin API and Arrow Flight
    ///
    /// Bearer token for authenticating requests to both the Admin API (for manifest fetching)
    /// and the Arrow Flight server (for data streaming).
    ///
    /// The token will be sent as an Authorization header: `Authorization: Bearer <token>`
    ///
    /// Can also be set via AMP_AUTH_TOKEN environment variable
    #[arg(long, env = "AMP_AUTH_TOKEN")]
    pub auth_token: Option<BearerToken>,
}
