use std::sync::Once;

use datafusion::common::internal_datafusion_err;
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::WithExportConfig;
use tracing_subscriber::{
    EnvFilter, filter::LevelFilter, layer::SubscriberExt, util::SubscriberInitExt,
};

static NOZZLE_LOG_ENV_VAR: &str = "NOZZLE_LOG";

/// List of crates in the workspace.
const NOZZLE_CRATES: &[&str] = &[
    "admin_api",
    "common",
    "dataset_store",
    "dump",
    "dump_check",
    "evm_rpc_datasets",
    "firehose_datasets",
    "generate_manifest",
    "http_common",
    "js_runtime",
    "metadata_db",
    "nozzle",
    "registry_service",
    "server",
    "substreams_datasets",
    "tests",
];

pub fn register_logger() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let (env_filter, nozzle_log_level) = env_filter_and_log_level();

        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_ansi(atty::is(atty::Stream::Stderr))
            .init();

        tracing::info!("log level: {}", nozzle_log_level);
    });
}

pub fn register_logger_with_telemetry(
    url: String,
    trace_ratio: f64,
) -> datafusion::error::Result<()> {
    let (env_filter, nozzle_log_level) = env_filter_and_log_level();

    // Initialize OpenTelemetry tracing infrastructure to enable tracing of query execution.
    let (telemetry_layer, _telemetry_tracing_provider) = {
        let resource = opentelemetry_sdk::Resource::builder()
            .with_attribute(opentelemetry::KeyValue::new(
                "service.name",
                "datafusion-tracing",
            ))
            .build();

        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(url)
            .build()
            .map_err(|e| internal_datafusion_err!("OTLP exporter error: {}", e))?;

        let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(resource)
            .with_sampler(opentelemetry_sdk::trace::Sampler::TraceIdRatioBased(
                trace_ratio,
            ))
            .build();

        let tracer = tracer_provider.tracer("nozzle-datafusion-tracing-query");

        let telemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

        (telemetry_layer, tracer_provider)
    };

    let fmt_layer = tracing_subscriber::fmt::layer().with_writer(std::io::stdout);

    tracing_subscriber::Registry::default()
        .with(env_filter)
        .with(fmt_layer)
        .with(telemetry_layer)
        .init();

    tracing::info!("log level: {}", nozzle_log_level);

    Ok(())
}

fn env_filter_and_log_level() -> (EnvFilter, String) {
    // Parse directives from RUST_LOG
    let log_filter = EnvFilter::builder().with_default_directive(LevelFilter::ERROR.into());
    let directive_string = std::env::var(EnvFilter::DEFAULT_ENV).unwrap_or_default();
    let mut env_filter = log_filter.parse(&directive_string).unwrap();

    let log_level = std::env::var(NOZZLE_LOG_ENV_VAR).unwrap_or_else(|_| "debug".to_string());

    for crate_name in NOZZLE_CRATES {
        // Add directives for each crate in NOZZLE_CRATES, if not overriden by RUST_LOG
        if !directive_string.contains(&format!("{crate_name}=")) {
            env_filter =
                env_filter.add_directive(format!("{crate_name}={log_level}").parse().unwrap());
        }
    }

    (env_filter, log_level)
}

/// If this fails, just update the above `NOZZLE_CRATES` to match reality.
#[test]
fn assert_nozzle_crates() {
    use cargo_metadata::MetadataCommand;

    let cmd = MetadataCommand::new().exec().unwrap();
    let mut names: Vec<String> = cmd
        .workspace_packages()
        .into_iter()
        .map(|pkg| pkg.name.replace("-", "_").clone())
        .collect();
    names.sort();
    assert_eq!(names, NOZZLE_CRATES);
}
