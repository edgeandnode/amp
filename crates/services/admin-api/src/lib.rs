//! Amp Admin API

use axum::{
    Router,
    routing::{get, post, put},
};

pub mod ctx;
pub mod handlers;
pub mod scheduler;

use ctx::Ctx;
use handlers::{datasets, files, jobs, manifests, providers, schema, workers};

/// Create the admin API router with all routes registered
///
/// Returns a router configured with all admin API endpoints.
pub fn router(ctx: Ctx) -> Router<()> {
    Router::new()
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
        .route(
            "/datasets/{namespace}/{name}/versions/{revision}/restore",
            post(datasets::restore::handler),
        )
        .route(
            "/datasets/{namespace}/{name}/versions/{revision}/jobs",
            get(datasets::list_jobs::handler),
        )
        .route(
            "/datasets/{namespace}/{name}/versions/{revision}/progress",
            get(datasets::progress::handler),
        )
        .route(
            "/datasets/{namespace}/{name}/versions/{revision}/tables/{table}/progress",
            get(datasets::progress::table_handler),
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
        .route(
            "/manifests",
            get(manifests::list_all::handler)
                .post(manifests::register::handler)
                .delete(manifests::prune::handler),
        )
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
        .route("/workers/{id}", get(workers::get_by_id::handler))
        .with_state(ctx)
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
        handlers::datasets::list_jobs::handler,
        handlers::datasets::progress::handler,
        handlers::datasets::progress::table_handler,
        handlers::datasets::get::handler,
        handlers::datasets::get_manifest::handler,
        handlers::datasets::register::handler,
        handlers::datasets::deploy::handler,
        handlers::datasets::restore::handler,
        handlers::datasets::delete::handler,
        handlers::datasets::delete_version::handler,
        // Manifest endpoints
        handlers::manifests::list_all::handler,
        handlers::manifests::register::handler,
        handlers::manifests::get_by_id::handler,
        handlers::manifests::delete_by_id::handler,
        handlers::manifests::list_datasets::handler,
        handlers::manifests::prune::handler,
        // Job endpoints
        handlers::jobs::get_all::handler,
        handlers::jobs::get_by_id::handler,
        handlers::jobs::stop::handler,
        handlers::jobs::delete::handler,
        handlers::jobs::delete_by_id::handler,
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
        handlers::workers::get_by_id::handler,
    ),
    components(schemas(
        // Common schemas
        handlers::error::ErrorResponse,
        // Manifest schemas
        handlers::manifests::list_all::ManifestsResponse,
        handlers::manifests::list_all::ManifestInfo,
        handlers::manifests::register::RegisterManifestResponse,
        handlers::manifests::list_datasets::ManifestDatasetsResponse,
        handlers::manifests::list_datasets::Dataset,
        handlers::manifests::prune::PruneResponse,
        // Dataset schemas
        handlers::datasets::get::DatasetInfo,
        handlers::datasets::list_all::DatasetsResponse,
        handlers::datasets::list_all::DatasetSummary,
        handlers::datasets::list_versions::VersionsResponse,
        handlers::datasets::list_versions::VersionInfo,
        handlers::datasets::register::RegisterRequest,
        handlers::datasets::deploy::DeployRequest,
        handlers::datasets::deploy::DeployResponse,
        handlers::datasets::restore::RestoreResponse,
        handlers::datasets::restore::RestoredTableInfo,
        handlers::datasets::progress::SyncProgressResponse,
        handlers::datasets::progress::TableSyncProgress,
        // Job schemas
        handlers::jobs::job_info::JobInfo,
        handlers::jobs::get_all::JobsResponse,
        handlers::jobs::delete::JobStatusFilter,
        // Provider schemas
        handlers::providers::provider_info::ProviderInfo,
        handlers::providers::get_all::ProvidersResponse,
        // File schemas
        handlers::files::get_by_id::FileInfo,
        // Schema schemas
        handlers::schema::SchemaRequest,
        handlers::schema::SchemaResponse,
        // Worker schemas
        handlers::workers::get_all::WorkerInfo,
        handlers::workers::get_all::WorkersResponse,
        handlers::workers::get_by_id::WorkerDetailResponse,
        handlers::workers::get_by_id::WorkerMetadata,
    )),
    tags(
        (name = "datasets", description = "Dataset management endpoints"),
        (name = "jobs", description = "Job management endpoints"),
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
