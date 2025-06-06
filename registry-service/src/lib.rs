mod handlers;

use std::{future::Future, net::SocketAddr, sync::Arc};

use axum::{routing::post, Router};
use common::{config::Config, BoxResult};
use dataset_store::DatasetStore;
use handlers::output_schema_handler;
use metadata_db::MetadataDb;

pub struct ServiceState {
    dataset_store: Arc<DatasetStore>,
}

pub async fn serve(
    at: SocketAddr,
    config: Arc<Config>,
    metadata_db: Arc<MetadataDb>,
) -> BoxResult<(SocketAddr, impl Future<Output = BoxResult<()>>)> {
    let state = Arc::new(ServiceState {
        dataset_store: DatasetStore::new(config, metadata_db),
    });

    // Build the application with routes
    let app = Router::new()
        .route("/output_schema", post(output_schema_handler))
        .with_state(state);

    http_common::serve_at(at, app).await
}
