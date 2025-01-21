mod handlers;

use axum::{routing::post, Router};
use common::{config::Config, BoxError};
use handlers::output_schema_handler;
use metadata_db::MetadataDb;
use std::{net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;

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
        .with_state(state);

    // Specify the address to run the server
    let listener = TcpListener::bind(at).await?;

    // Run the server
    axum::serve(listener, app).await?;

    Ok(())
}
