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
use handlers::{datasets, files, jobs, locations, providers};
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
        .route(
            "/datasets",
            get(datasets::get_all::handler).post(datasets::register::handler),
        )
        .route(
            "/datasets/schema/analyze",
            post(datasets::schema_analyze::handler),
        )
        .route("/datasets/{name}", get(datasets::get_by_id::handler))
        .route(
            "/datasets/{name}/versions/{version}",
            get(datasets::get_by_id::handler_with_version),
        )
        .route("/datasets/{name}/dump", post(datasets::dump::handler))
        .route(
            "/datasets/{name}/versions/{version}/dump",
            post(datasets::dump::handler_with_version),
        )
        .route(
            "/jobs",
            get(jobs::get_all::handler).delete(jobs::delete::handler),
        )
        .route(
            "/jobs/{id}",
            get(jobs::get_by_id::handler).delete(jobs::delete_by_id::handler),
        )
        .route("/jobs/{id}/stop", put(jobs::stop::handler))
        .route("/locations", get(locations::get_all::handler))
        .route(
            "/locations/{id}",
            get(locations::get_by_id::handler).delete(locations::delete_by_id::handler),
        )
        .route(
            "/locations/{location_id}/files",
            get(locations::get_files::handler),
        )
        .route("/files/{file_id}", get(files::get_by_id::handler))
        .route(
            "/providers",
            get(providers::get_all::handler).post(providers::create::handler),
        )
        .route(
            "/providers/{name}",
            get(providers::get_by_id::handler).delete(providers::delete_by_id::handler),
        )
        .with_state(Ctx {
            config,
            metadata_db,
            store,
            scheduler,
        });

    http_common::serve_at(at, app).await
}
