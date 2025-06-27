//! Nozzle Admin API

use std::{future::Future, net::SocketAddr, sync::Arc};

use axum::{
    Router,
    routing::{get, post},
};
use common::{BoxResult, config::Config};
use dataset_store::DatasetStore;

mod ctx;
mod handlers;
mod scheduler;

use ctx::Ctx;
use handlers::datasets;
use scheduler::Scheduler;

pub async fn serve(
    at: SocketAddr,
    config: Arc<Config>,
) -> BoxResult<(SocketAddr, impl Future<Output = BoxResult<()>>)> {
    let metadata_db = config.metadata_db().await?;

    let store = DatasetStore::new(config.clone(), Arc::new(metadata_db.clone()));
    let scheduler = Scheduler::new(config.clone(), metadata_db.clone());

    // Register the routes
    let app = Router::new()
        .route("/deploy", post(datasets::deploy::handler)) // TODO: Remove. Deprecated in favor of POST /datasets
        .route(
            "/datasets",
            get(datasets::get_all::handler).post(datasets::deploy::handler),
        )
        .route("/datasets/{id}", get(datasets::get_by_id::handler))
        .route("/datasets/{id}/dump", post(datasets::dump::handler))
        .with_state(Ctx {
            config,
            metadata_db,
            store,
            scheduler,
        });

    http_common::serve_at(at, app).await
}
