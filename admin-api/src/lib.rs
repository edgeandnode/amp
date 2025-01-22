mod handlers;

use axum::{routing::post, Router};
use common::{config::Config, BoxError};
use handlers::deploy_handler;
use metadata_db::MetadataDb;
use std::{net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;

pub struct ServiceState {
    pub config: Arc<Config>,
    pub metadata_db: Option<MetadataDb>,
}

pub async fn serve(at: SocketAddr, config: Arc<Config>) -> Result<(), BoxError> {
    let metadata_db = if let Some(url) = &config.metadata_db_url {
        Some(MetadataDb::connect(url).await?)
    } else {
        None
    };
    let state = Arc::new(ServiceState {
        config,
        metadata_db,
    });

    // Build the application with the /deploy route
    let app = Router::new()
        .route("/deploy", post(deploy_handler))
        .with_state(state);

    // Specify the address to run the server
    let listener = TcpListener::bind(at).await?;

    // Run the server
    axum::serve(listener, app).await?;

    Ok(())
}
