use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use common::config::Config;
use futures::{StreamExt, future::BoxFuture};
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, Response, StatusCode, service::Service as HyperService};
use metadata_db::MetadataDb;
use tower::Service as TowerService;

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type Body = http_body_util::combinators::UnsyncBoxBody<Bytes, BoxError>;
type TonicBody = http_body_util::combinators::UnsyncBoxBody<Bytes, tonic::Status>;

/// Multiplexer service that routes HTTP/1 health checks and HTTP/2 gRPC requests
#[derive(Clone)]
pub struct Multiplexer<G> {
    grpc_service: G,
    config: Arc<Config>,
    metadata_db: MetadataDb,
}

impl<G> Multiplexer<G> {
    pub fn new(grpc_service: G, config: Arc<Config>, metadata_db: MetadataDb) -> Self {
        Self {
            grpc_service,
            config,
            metadata_db,
        }
    }
}

impl<G> HyperService<Request<hyper::body::Incoming>> for Multiplexer<G>
where
    G: TowerService<Request<TonicBody>, Response = Response<TonicBody>> + Clone + Send + 'static,
    G::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    G::Future: Send + 'static,
{
    type Response = Response<Body>;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn call(&self, req: Request<hyper::body::Incoming>) -> Self::Future {
        let version = req.version();
        let path = req.uri().path();
        let method = req.method().clone();

        // Route HTTP/1.1 requests to /health or /ready
        if version == hyper::Version::HTTP_11 && (path == "/health" || path == "/ready") {
            let config = self.config.clone();
            let metadata_db = self.metadata_db.clone();
            let is_health = path == "/health";

            Box::pin(async move {
                if method != Method::GET {
                    return Ok(Response::builder()
                        .status(StatusCode::METHOD_NOT_ALLOWED)
                        .body(boxed_body(Full::new(Bytes::from("Method not allowed"))))
                        .unwrap());
                }

                if is_health {
                    handle_health().await
                } else {
                    handle_ready(config, metadata_db).await
                }
            })
        } else {
            // Route HTTP/2 and other requests to gRPC service
            // Map the body from Incoming to TonicBody
            let (parts, body) = req.into_parts();
            let body = body
                .map_err(|e| tonic::Status::from_error(Box::new(e)))
                .boxed_unsync();
            let req = Request::from_parts(parts, body);

            let mut grpc = self.grpc_service.clone();
            Box::pin(async move {
                let response = grpc.call(req).await.map_err(Into::into)?;
                // Map the response body from TonicBody to Body
                let (parts, body) = response.into_parts();
                let body = body.map_err(|e| Box::new(e) as BoxError).boxed_unsync();
                Ok(Response::from_parts(parts, body))
            })
        }
    }
}

/// Health check handler
async fn handle_health() -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync>> {
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/plain")
        .body(boxed_body(Full::new(Bytes::from("OK"))))
        .unwrap())
}

/// Readiness check handler
async fn handle_ready(
    config: Arc<Config>,
    metadata_db: MetadataDb,
) -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync>> {
    // Run all checks concurrently with overall timeout, short-circuit on first failure
    let result = tokio::time::timeout(Duration::from_millis(500), async {
        futures::try_join!(
            check_database(&metadata_db),
            check_storage(&config),
            check_workers(&metadata_db)
        )
    })
    .await;

    match result {
        Ok(Ok(_)) => Ok(Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/plain")
            .body(boxed_body(Full::new(Bytes::from("OK"))))
            .unwrap()),
        Ok(Err(e)) => Ok(error_response(e)),
        Err(_) => Ok(error_response("Readiness check timeout".to_string())),
    }
}

/// Check database connectivity
pub async fn check_database(metadata_db: &MetadataDb) -> Result<(), String> {
    metadata_db
        .active_workers()
        .await
        .map_err(|e| format!("Database unhealthy: {}", e))?;
    Ok(())
}

/// Check storage backend accessibility
pub async fn check_storage(config: &Config) -> Result<(), String> {
    config
        .data_store
        .list("")
        .next()
        .await
        .transpose()
        .map_err(|e| format!("Storage unhealthy: {}", e))?;
    Ok(())
}

/// Check worker availability
pub async fn check_workers(metadata_db: &MetadataDb) -> Result<(), String> {
    let workers = metadata_db
        .active_workers()
        .await
        .map_err(|e| format!("Failed to get workers: {}", e))?;

    if workers.is_empty() {
        return Err("No active workers available".to_string());
    }

    Ok(())
}

/// Helper to create error response
fn error_response(message: String) -> Response<Body> {
    Response::builder()
        .status(StatusCode::SERVICE_UNAVAILABLE)
        .header("content-type", "text/plain")
        .body(boxed_body(Full::new(Bytes::from(message))))
        .unwrap()
}

/// Helper to box the body
fn boxed_body(body: Full<Bytes>) -> Body {
    body.map_err(|never| match never {}).boxed_unsync()
}
