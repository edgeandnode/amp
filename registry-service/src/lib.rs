pub mod handlers;

use std::{future::Future, net::SocketAddr, sync::Arc};

use axum::{Router, routing::post};
use common::{BoxResult, config::Config};
use dataset_store::DatasetStore;
use metadata_db::MetadataDb;

use crate::handlers::{output_schema::output_schema_handler, register::register_handler};

#[derive(Clone)]
pub struct ServiceState {
    pub dataset_store: Arc<DatasetStore>,
    pub metadata_db: Arc<MetadataDb>,
}

pub async fn serve(
    at: SocketAddr,
    config: Arc<Config>,
    metadata_db: Arc<MetadataDb>,
) -> BoxResult<(SocketAddr, impl Future<Output = BoxResult<()>>)> {
    let dataset_store = DatasetStore::new(config.clone(), metadata_db.clone());
    let state = ServiceState {
        dataset_store,
        metadata_db,
    };

    // Build the application with routes
    let app = Router::new()
        .route("/output_schema", post(output_schema_handler))
        .route("/register", post(register_handler))
        .with_state(state);

    http_common::serve_at(at, app).await
}
