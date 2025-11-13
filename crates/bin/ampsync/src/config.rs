use clap::{Args, Parser, Subcommand};
use datasets_common::partial_reference::PartialReference;
use http::header::{HeaderMap, HeaderName, HeaderValue};

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

    /// HTTP headers to include in Flight requests (format: key=value)
    ///
    /// Can be specified multiple times for multiple headers.
    /// Commonly used for authentication: --header "authorization=Bearer <token>"
    ///
    /// Can also be set via AMP_FLIGHT_HEADERS environment variable (comma-separated):
    ///   AMP_FLIGHT_HEADERS="authorization=Bearer abc123,x-api-key=xyz"
    ///
    /// Example:
    ///   ampsync sync --header "authorization=Bearer abc123" --header "x-api-key=xyz"
    #[arg(long = "header", env = "AMP_FLIGHT_HEADERS", value_delimiter = ',', value_parser = parse_header_to_map)]
    pub headers: HeaderMap,
}

/// Parse a header string and insert into HeaderMap
fn parse_header_to_map(s: &str) -> Result<HeaderMap, String> {
    let parts: Vec<&str> = s.splitn(2, '=').collect();
    if parts.len() != 2 {
        return Err(format!(
            "Invalid header format '{}'. Expected format: key=value",
            s
        ));
    }

    let name = HeaderName::from_bytes(parts[0].as_bytes())
        .map_err(|e| format!("Invalid header name '{}': {}", parts[0], e))?;

    let value =
        HeaderValue::from_str(parts[1]).map_err(|e| format!("Invalid header value: {}", e))?;

    let mut map = HeaderMap::new();
    map.insert(name, value);
    Ok(map)
}
