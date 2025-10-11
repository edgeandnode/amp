//! Amp Controller Service
//!
//! The controller service provides the admin API for managing Amp operations.

use std::{future::Future, net::SocketAddr, sync::Arc};

use common::{BoxResult, config::Config};

/// Start the controller service which includes the admin API server.
///
/// Returns the bound address and a future that runs the server.
pub async fn serve(
    at: SocketAddr,
    config: Arc<Config>,
    meter: Option<&monitoring::telemetry::metrics::Meter>,
) -> BoxResult<(SocketAddr, impl Future<Output = BoxResult<()>>)> {
    admin_api::serve(at, config, meter).await
}
