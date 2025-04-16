mod handlers;

use axum::{
    routing::{get, post},
    Router,
};
use common::{config::Config, BoxError};
use dataset_store::DatasetStore;
use handlers::{datasets_handler, output_schema_handler};
use metadata_db::MetadataDb;
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::broadcast;

pub struct ServiceState {
    dataset_store: Arc<DatasetStore>,
}

pub async fn serve(
    at: SocketAddr,
    config: Arc<Config>,
    metadata_db: Option<MetadataDb>,
    shutdown: broadcast::Receiver<()>,
) -> Result<(), BoxError> {
    let state = Arc::new(ServiceState {
        dataset_store: DatasetStore::new(config, metadata_db),
    });

    // Build the application with routes
    let app = Router::new()
        .route("/output_schema", post(output_schema_handler))
        .route("/datasets", get(datasets_handler))
        .with_state(state);

    http_common::serve_at(at, app, shutdown).await?;

    Ok(())
}
