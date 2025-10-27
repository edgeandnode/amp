//! Amp Admin API

use std::{future::Future, net::SocketAddr, sync::Arc};

use axum::{
    Router,
    routing::{get, post, put},
    serve::{Listener as _, ListenerExt as _},
};
use common::{BoxResult, config::Config, utils::shutdown_signal};
use dataset_store::DatasetStore;

mod ctx;
pub mod handlers;
mod scheduler;

use ctx::Ctx;
use dataset_store::providers::ProviderConfigsStore;
use handlers::{datasets, files, jobs, locations, manifests, providers, schema, workers};
use scheduler::Scheduler;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;

pub async fn serve(
    at: SocketAddr,
    config: Arc<Config>,
    meter: Option<&monitoring::telemetry::metrics::Meter>,
) -> BoxResult<(SocketAddr, impl Future<Output = BoxResult<()>>)> {
    let metadata_db = config.metadata_db().await?;

    let provider_configs_store = ProviderConfigsStore::new(config.providers_store.prefixed_store());
    let dataset_manifests_store = dataset_store::manifests::DatasetManifestsStore::new(
        config.manifests_store.prefixed_store(),
    );

    let dataset_store = DatasetStore::new(
        metadata_db.clone(),
        provider_configs_store,
        dataset_manifests_store,
    );

    let scheduler = Scheduler::new(config.clone(), metadata_db.clone());

    let ctx = Ctx {
        metadata_db,
        dataset_store,
        scheduler,
    };

    // Register the routes
    let mut app = Router::new()
        .route(
            "/datasets",
            get(datasets::list_all::handler).post(datasets::register::handler),
        )
        .route(
            "/datasets/{namespace}/{name}",
            get(datasets::get::handler).delete(datasets::delete::handler),
        )
        .route(
            "/datasets/{namespace}/{name}/versions",
            get(datasets::list_versions::handler),
        )
        .route(
            "/datasets/{namespace}/{name}/versions/{version}",
            get(datasets::get::handler).delete(datasets::delete_version::handler),
        )
        .route(
            "/datasets/{namespace}/{name}/versions/{revision}/manifest",
            get(datasets::get_manifest::handler),
        )
        .route(
            "/datasets/{namespace}/{name}/versions/{revision}/deploy",
            post(datasets::deploy::handler),
        )
        .route("/files/{file_id}", get(files::get_by_id::handler))
        .route(
            "/jobs",
            get(jobs::get_all::handler).delete(jobs::delete::handler),
        )
        .route(
            "/jobs/{id}",
            get(jobs::get_by_id::handler).delete(jobs::delete_by_id::handler),
        )
        .route("/jobs/{id}/stop", put(jobs::stop::handler))
        .route("/locations", get(locations::get_all::handler))
        .route(
            "/locations/{id}",
            get(locations::get_by_id::handler).delete(locations::delete_by_id::handler),
        )
        .route(
            "/locations/{location_id}/files",
            get(locations::get_files::handler),
        )
        .route("/manifests", post(manifests::register::handler))
        .route(
            "/manifests/{hash}",
            get(manifests::get_by_id::handler).delete(manifests::delete_by_id::handler),
        )
        .route(
            "/manifests/{hash}/datasets",
            get(manifests::list_datasets::handler),
        )
        .route(
            "/providers",
            get(providers::get_all::handler).post(providers::create::handler),
        )
        .route(
            "/providers/{name}",
            get(providers::get_by_id::handler).delete(providers::delete_by_id::handler),
        )
        .route("/schema", post(schema::handler))
        .route("/workers", get(workers::get_all::handler))
        .with_state(ctx);

    // Add OpenTelemetry HTTP metrics middleware if meter is provided
    if let Some(meter) = meter {
        let metrics_layer = opentelemetry_instrumentation_tower::HTTPMetricsLayerBuilder::builder()
            .with_meter(meter.clone())
            .build()?;
        app = app.layer(metrics_layer);
    }

    let listener = TcpListener::bind(at)
        .await?
        .tap_io(|tcp_stream| tcp_stream.set_nodelay(true).unwrap());
    let addr = listener.local_addr()?;

    let router = app.layer(CorsLayer::permissive());
    let server = async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .map_err(Into::into)
    };
    Ok((addr, server))
}

#[cfg(feature = "utoipa")]
#[derive(utoipa::OpenApi)]
#[openapi(
    info(
        title = "Amp Admin API",
        version = "1.0.0",
        description = include_str!("../SPEC_DESCRIPTION.md")
    ),
    paths(
        // Dataset endpoints
        handlers::datasets::list_all::handler,
        handlers::datasets::list_versions::handler,
        handlers::datasets::get::handler,
        handlers::datasets::get_manifest::handler,
        handlers::datasets::register::handler,
        handlers::datasets::deploy::handler,
        handlers::datasets::delete::handler,
        handlers::datasets::delete_version::handler,
        // Manifest endpoints
        handlers::manifests::register::handler,
        handlers::manifests::get_by_id::handler,
        handlers::manifests::delete_by_id::handler,
        handlers::manifests::list_datasets::handler,
        // Job endpoints
        handlers::jobs::get_all::handler,
        handlers::jobs::get_by_id::handler,
        handlers::jobs::stop::handler,
        handlers::jobs::delete::handler,
        handlers::jobs::delete_by_id::handler,
        // Location endpoints
        handlers::locations::get_all::handler,
        handlers::locations::get_by_id::handler,
        handlers::locations::delete_by_id::handler,
        handlers::locations::get_files::handler,
        // Provider endpoints
        handlers::providers::get_all::handler,
        handlers::providers::get_by_id::handler,
        handlers::providers::create::handler,
        handlers::providers::delete_by_id::handler,
        // Files endpoints
        handlers::files::get_by_id::handler,
        // Schema endpoints
        handlers::schema::handler,
        // Worker endpoints
        handlers::workers::get_all::handler,
    ),
    components(schemas(
        // Common schemas
        handlers::error::ErrorResponse,
        // Manifest schemas
        handlers::manifests::register::RegisterManifestResponse,
        handlers::manifests::list_datasets::ManifestDatasetsResponse,
        handlers::manifests::list_datasets::Dataset,
        // Dataset schemas
        handlers::datasets::get::DatasetInfo,
        handlers::datasets::list_all::DatasetsResponse,
        handlers::datasets::list_all::DatasetSummary,
        handlers::datasets::list_versions::VersionsResponse,
        handlers::datasets::list_versions::VersionInfo,
        handlers::datasets::register::RegisterRequest,
        handlers::datasets::deploy::DeployRequest,
        handlers::datasets::deploy::DeployResponse,
        // Job schemas
        handlers::jobs::job_info::JobInfo,
        handlers::jobs::get_all::JobsResponse,
        handlers::jobs::delete::JobStatusFilter,
        // Location schemas
        handlers::locations::location_info::LocationInfoWithDetails,
        handlers::locations::location_info::LocationInfo,
        handlers::locations::get_all::LocationsResponse,
        handlers::locations::get_files::LocationFilesResponse,
        // Provider schemas
        handlers::providers::provider_info::ProviderInfo,
        handlers::providers::get_all::ProvidersResponse,
        // File schemas
        handlers::files::get_by_id::FileInfo,
        handlers::locations::get_files::FileListInfo,
        // Schema schemas
        handlers::schema::OutputSchemaRequest,
        handlers::schema::OutputSchemaResponse,
        // Worker schemas
        handlers::workers::get_all::WorkerInfo,
        handlers::workers::get_all::WorkersResponse,
    )),
    tags(
        (name = "datasets", description = "Dataset management endpoints"),
        (name = "jobs", description = "Job management endpoints"),
        (name = "locations", description = "Location management endpoints"),
        (name = "manifests", description = "Manifest management endpoints"),
        (name = "providers", description = "Provider management endpoints"),
        (name = "files", description = "File access endpoints"),
        (name = "schema", description = "Schema generation endpoints"),
        (name = "workers", description = "Worker management endpoints"),
    )
)]
struct ApiDoc;

#[cfg(feature = "utoipa")]
pub fn generate_openapi_spec() -> utoipa::openapi::OpenApi {
    <ApiDoc as utoipa::OpenApi>::openapi()
}
