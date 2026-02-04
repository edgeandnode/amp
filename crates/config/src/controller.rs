use std::net::{AddrParseError, SocketAddr};

use crate::config_file::ConfigFile;

/// Default port for the Admin API server.
pub const DEFAULT_CONTROLLER_ADMIN_API_PORT: u16 = 1610;

/// Network address for the controller's Admin API endpoint.
#[derive(Debug, Clone)]
pub struct ControllerAddrs {
    /// Admin API server address (default: `0.0.0.0:1610`).
    pub admin_api_addr: SocketAddr,
}

impl Default for ControllerAddrs {
    fn default() -> Self {
        Self {
            admin_api_addr: ([0, 0, 0, 0], DEFAULT_CONTROLLER_ADMIN_API_PORT).into(),
        }
    }
}

impl ControllerAddrs {
    pub fn from_config_file(
        config_file: &ConfigFile,
        defaults: &ControllerAddrs,
    ) -> Result<Self, InvalidAddrError> {
        let admin_api_addr = match &config_file.admin_api_addr {
            Some(addr) => addr
                .parse::<SocketAddr>()
                .map_err(|source| InvalidAddrError { source })?,
            None => defaults.admin_api_addr,
        };

        Ok(Self { admin_api_addr })
    }
}

/// The admin API address string could not be parsed as a [`SocketAddr`].
#[derive(Debug, thiserror::Error)]
#[error("Invalid admin_api_addr: {source}")]
pub struct InvalidAddrError {
    /// The underlying parse error.
    #[source]
    pub source: AddrParseError,
}
