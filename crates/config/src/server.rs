use std::net::{AddrParseError, SocketAddr};

use crate::config_file::ConfigFile;

/// Default port for the Arrow Flight RPC server.
pub const DEFAULT_SERVER_FLIGHT_PORT: u16 = 1602;

/// Default port for the JSON Lines server.
pub const DEFAULT_SERVER_JSONL_PORT: u16 = 1603;

/// Network addresses for the query server's listening endpoints.
#[derive(Debug, Clone)]
pub struct ServerAddrs {
    /// Arrow Flight RPC server address (default: `0.0.0.0:1602`).
    pub flight_addr: SocketAddr,
    /// JSON Lines server address (default: `0.0.0.0:1603`).
    pub jsonl_addr: SocketAddr,
}

impl Default for ServerAddrs {
    fn default() -> Self {
        Self {
            flight_addr: ([0, 0, 0, 0], DEFAULT_SERVER_FLIGHT_PORT).into(),
            jsonl_addr: ([0, 0, 0, 0], DEFAULT_SERVER_JSONL_PORT).into(),
        }
    }
}

impl ServerAddrs {
    pub fn from_config_file(
        config_file: &ConfigFile,
        defaults: &ServerAddrs,
    ) -> Result<Self, InvalidAddrError> {
        Ok(Self {
            flight_addr: parse_addr(
                &config_file.flight_addr,
                defaults.flight_addr,
                "flight_addr",
            )?,
            jsonl_addr: parse_addr(&config_file.jsonl_addr, defaults.jsonl_addr, "jsonl_addr")?,
        })
    }
}

/// A service address string could not be parsed as a [`SocketAddr`].
#[derive(Debug, thiserror::Error)]
#[error("Invalid address for {name}: {source}")]
pub struct InvalidAddrError {
    /// The config field name (e.g. `"flight_addr"`).
    pub name: String,
    /// The underlying parse error.
    #[source]
    pub source: AddrParseError,
}

fn parse_addr(
    addr_str: &Option<String>,
    default: SocketAddr,
    name: &str,
) -> Result<SocketAddr, InvalidAddrError> {
    match addr_str {
        Some(addr) => addr
            .parse::<SocketAddr>()
            .map_err(|source| InvalidAddrError {
                name: name.to_string(),
                source,
            }),
        None => Ok(default),
    }
}
