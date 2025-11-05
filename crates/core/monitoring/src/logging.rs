//! A set of utilities to enable logging configuration using tracing_subscriber.

use std::{io::IsTerminal, sync::Once};

use opentelemetry::trace::TracerProvider;
use tracing_subscriber::{
    self, EnvFilter, filter::LevelFilter, layer::SubscriberExt, util::SubscriberInitExt,
};

use crate::telemetry;

static AMP_LOG_ENV_VAR: &str = "AMP_LOG";

/// Initializes a tracing subscriber for logging.
pub fn init() {
    // Since we also use this function to enable logging in tests, wrap it in `Once` to prevent
    // multiple initializations.
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let (env_filter, amp_log_level) = env_filter_and_log_level();

        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_ansi(std::io::stderr().is_terminal())
            .init();

        tracing::info!("log level: {}", amp_log_level);
    });
}

/// Initializes a tracing subscriber for logging with OpenTelemetry tracing support.
pub fn init_with_telemetry(url: String, trace_ratio: f64) -> telemetry::traces::Result {
    let (env_filter, amp_log_level) = env_filter_and_log_level();

    // Initialize OpenTelemetry tracing infrastructure to enable tracing of query execution.
    let (telemetry_layer, traces_provider) = {
        let tracer_provider = telemetry::traces::provider(url, trace_ratio)?;
        let tracer = tracer_provider.tracer("amp-tracer");
        let telemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

        (telemetry_layer, tracer_provider)
    };

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal());

    tracing_subscriber::Registry::default()
        .with(env_filter)
        .with(fmt_layer)
        .with(telemetry_layer)
        .init();

    tracing::info!("log level: {}", amp_log_level);

    Ok(traces_provider)
}

/// List of crates in the workspace.
const AMP_CRATES: &[&str] = &[
    "admin_api",
    "amp_client",
    "amp_debezium_client",
    "ampctl",
    "ampd",
    "ampsync",
    "ampup",
    "arrow_to_postgres",
    "auth_http",
    "common",
    "controller",
    "dataset_store",
    "datasets_common",
    "datasets_derived",
    "dump",
    "eth_beacon_datasets",
    "evm_rpc_datasets",
    "firehose_datasets",
    "js_runtime",
    "metadata_db",
    "monitoring",
    "server",
    "tempdb",
    "tests",
    "worker",
];

fn env_filter_and_log_level() -> (EnvFilter, String) {
    // Parse directives from RUST_LOG
    let log_filter = EnvFilter::builder().with_default_directive(LevelFilter::ERROR.into());
    let directive_string = std::env::var(EnvFilter::DEFAULT_ENV).unwrap_or_default();
    let mut env_filter = log_filter.parse(&directive_string).unwrap();

    let log_level = std::env::var(AMP_LOG_ENV_VAR).unwrap_or_else(|_| "info".to_string());

    for crate_name in AMP_CRATES {
        // Add directives for each crate in AMP_CRATES, if not overriden by RUST_LOG
        if !directive_string.contains(&format!("{crate_name}=")) {
            env_filter =
                env_filter.add_directive(format!("{crate_name}={log_level}").parse().unwrap());
        }
    }

    (env_filter, log_level)
}

/// If this fails, just update the above `AMP_CRATES` to match reality.
#[test]
fn assert_amp_crates() {
    use cargo_metadata::MetadataCommand;

    let cmd = MetadataCommand::new().exec().unwrap();
    let mut names: Vec<String> = cmd
        .workspace_packages()
        .into_iter()
        .map(|pkg| pkg.name.replace("-", "_").clone())
        .filter(|pkg| !pkg.ends_with("_gen")) // Exclude codegen crates
        .collect();
    names.sort();
    assert_eq!(names, AMP_CRATES);
}
