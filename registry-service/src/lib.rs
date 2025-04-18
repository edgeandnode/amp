mod handlers;

use axum::{
    routing::{get, post},
    Router,
};
use common::{config::Config, BoxError};
use handlers::{datasets_handler, output_schema_handler};
use metadata_db::MetadataDb;
use std::{net::SocketAddr, sync::Arc};

pub struct ServiceState {
    config: Arc<Config>,
    metadata_db: Option<MetadataDb>,
}

pub async fn serve(
    at: SocketAddr,
    config: Arc<Config>,
    metadata_db: Option<MetadataDb>,
) -> Result<(), BoxError> {
    let state = Arc::new(ServiceState {
        config,
        metadata_db,
    });

    // Build the application with routes
    let app = Router::new()
        .route("/output_schema", post(output_schema_handler))
        .route("/datasets", get(datasets_handler))
        .with_state(state);

    http_common::serve_at(at, app).await?;

    Ok(())
}
