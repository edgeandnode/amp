use axum::{extract::Json, response::IntoResponse, routing::post, BoxError, Router};
use serde::Deserialize;
use std::net::SocketAddr;
use tokio::net::TcpListener;

// Define the request payload structure
#[derive(Deserialize)]
struct DeployRequest {
    cid: String,
}

// Handler for the /deploy endpoint
async fn deploy_handler(Json(payload): Json<DeployRequest>) -> impl IntoResponse {
    // Simulate processing the CID (content identifier)
    println!("Received CID: {}", payload.cid);

    // Respond with a success message
    (axum::http::StatusCode::OK, "Deployment successful")
}

pub async fn serve(at: SocketAddr) -> Result<(), BoxError> {
    // Build the application with the /deploy route
    let app = Router::new().route("/deploy", post(deploy_handler));

    // Specify the address to run the server
    let listener = TcpListener::bind(at).await?;

    // Run the server
    axum::serve(listener, app).await?;

    Ok(())
}
