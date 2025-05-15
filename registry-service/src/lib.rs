mod handlers;

use std::{future::Future, net::SocketAddr, sync::Arc};

use axum::{
    routing::{get, post},
    Router,
};
use common::{config::Config, BoxResult};
use dataset_store::DatasetStore;
use handlers::{datasets_handler, output_schema_handler};
use metadata_db::MetadataDb;
use tokio::sync::broadcast;

pub struct ServiceState {
    dataset_store: Arc<DatasetStore>,
}

pub async fn serve(
    at: SocketAddr,
    config: Arc<Config>,
    metadata_db: Arc<MetadataDb>,
    shutdown: broadcast::Receiver<()>,
) -> BoxResult<(SocketAddr, impl Future<Output = BoxResult<()>>)> {
    let state = Arc::new(ServiceState {
        dataset_store: DatasetStore::new(config, metadata_db),
    });

    // Build the application with routes
    let app = Router::new()
        .route("/output_schema", post(output_schema_handler))
        .route("/datasets", get(datasets_handler))
        .with_state(state);

    http_common::serve_at(at, app, shutdown).await
}
