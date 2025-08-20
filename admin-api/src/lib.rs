//! Nozzle Admin API

use std::{future::Future, net::SocketAddr, sync::Arc};

use axum::{
    Router,
    routing::{get, post, put},
};
use common::{BoxResult, config::Config};
use dataset_store::DatasetStore;

mod ctx;
pub mod handlers;
mod scheduler;

use ctx::Ctx;
use handlers::{datasets, jobs, locations};
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
        .route("/deploy", post(datasets::deploy::handler))
        .route(
            "/datasets",
            get(datasets::get_all::handler).post(datasets::deploy::handler),
        )
        .route("/datasets/{id}", get(datasets::get_by_id::handler))
        .route("/datasets/{id}/dump", post(datasets::dump::handler))
        .route("/jobs", get(jobs::get_all::handler))
        .route("/jobs/{id}", get(jobs::get_by_id::handler))
        .route("/jobs/{id}/stop", put(jobs::stop::handler))
        .route("/locations", get(locations::get_all::handler))
        .route("/locations/{id}", get(locations::get_by_id::handler))
        .with_state(Ctx {
            config,
            metadata_db,
            store,
            scheduler,
        });

    http_common::serve_at(at, app).await
}
