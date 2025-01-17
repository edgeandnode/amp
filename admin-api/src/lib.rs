mod deploy;
mod error;

use axum::{routing::post, Router};
use common::{config::Config, BoxError};
use deploy::deploy_handler;
use std::{net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;

pub struct ServiceState {
    pub config: Arc<Config>,
    pub ipfs_client: reqwest::Client,
}

pub async fn serve(at: SocketAddr, config: Arc<Config>) -> Result<(), BoxError> {
    let state = Arc::new(ServiceState {
        config,
        ipfs_client: reqwest::Client::new(),
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
