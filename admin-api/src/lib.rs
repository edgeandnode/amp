mod handlers;
mod scheduler;

use axum::{routing::post, Router};
use common::{config::Config, BoxResult};
use handlers::deploy_handler;
use metadata_db::MetadataDb;
use scheduler::Scheduler;
use std::{future::Future, net::SocketAddr, sync::Arc};
use tokio::sync::broadcast;

#[derive(Clone)]
pub struct ServiceState {
    pub config: Arc<Config>,
    pub scheduler: Scheduler,
}

pub async fn serve(
    at: SocketAddr,
    config: Arc<Config>,
    shutdown: broadcast::Receiver<()>,
) -> BoxResult<(SocketAddr, impl Future<Output = BoxResult<()>>)> {
    let scheduler = if let Some(url) = &config.metadata_db_url {
        let metadata_db = MetadataDb::connect(url).await?;
        Scheduler::new(config.clone(), metadata_db)
    } else {
        Scheduler::Ephemeral(config.clone())
    };
    let state = ServiceState { config, scheduler };

    // Build the application with the /deploy route
    let app = Router::new()
        .route("/deploy", post(deploy_handler))
        .with_state(state);

    http_common::serve_at(at, app, shutdown).await
}
