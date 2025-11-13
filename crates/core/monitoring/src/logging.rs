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
        let env_filter = env_filter();

        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_writer(std::io::stderr)
            .with_ansi(std::io::stderr().is_terminal())
            .init();
    });
}

/// Initializes a tracing subscriber for logging with OpenTelemetry tracing support.
pub fn init_with_telemetry(
    url: String,
    trace_ratio: f64,
    compression: Option<String>,
) -> telemetry::traces::Result {
    let (env_filter, amp_log_level) = env_filter_and_log_level();

    // Initialize OpenTelemetry tracing infrastructure to enable tracing of query execution.
    let (telemetry_layer, traces_provider) = {
        let tracer_provider = telemetry::traces::provider(url, trace_ratio, compression)?;
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

    Ok(traces_provider)
}

/// List of crates in the workspace.
const AMP_CRATES: &[&str] = &[
    "admin_api",
    "amp_client",
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
    "datasets_raw",
    "dump",
    "eth_beacon_datasets",
    "evm_rpc_datasets",
    "firehose_datasets",
    "js_runtime",
    "metadata_db",
    "monitoring",
    "server",
    "tests",
    "worker",
];

fn env_filter() -> EnvFilter {
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

    env_filter
}

/// Collect the error source chain as a vector of strings for tracing.
///
/// Walks the `.source()` chain of the provided error and collects each source's
/// Display representation into a vector. Returns a `DebugValue<Vec<String>>` that
/// can be used directly in tracing macros. Returns an empty vector if the error
/// has no source chain.
pub fn error_source(err: &dyn std::error::Error) -> tracing::field::DebugValue<Vec<String>> {
    let mut sources = Vec::new();
    let mut current = err.source();

    while let Some(curr) = current {
        sources.push(curr.to_string());
        current = curr.source();
    }

    tracing::field::debug(sources)
}

#[cfg(test)]
mod tests {
    use cargo_metadata::MetadataCommand;

    use super::*;

    /// If this fails, just update the above `AMP_CRATES` to match reality.
    #[test]
    fn workspace_crates_match_amp_crates_list() {
        //* Given
        let cmd = MetadataCommand::new()
            .exec()
            .expect("should execute cargo metadata command");

        //* When
        let mut names: Vec<String> = cmd
            .workspace_packages()
            .into_iter()
            .map(|pkg| pkg.name.replace("-", "_").clone())
            .filter(|pkg| !pkg.ends_with("_gen")) // Exclude codegen crates
            .collect();
        names.sort();

        //* Then
        assert_eq!(names, AMP_CRATES);
    }

    #[test]
    fn error_source_with_three_level_chain_returns_two_sources() {
        //* Given
        /// Root error representing database connection failure
        #[derive(Debug, thiserror::Error)]
        #[error("database connection refused")]
        struct DatabaseConnectionError;

        /// Error that occurs when a database query fails
        #[derive(Debug, thiserror::Error)]
        #[error("failed to execute query")]
        struct QueryExecutionError(#[source] DatabaseConnectionError);

        /// Error that occurs when fetching user data fails
        #[derive(Debug, thiserror::Error)]
        #[error("failed to fetch user data")]
        struct FetchUserDataError(#[source] QueryExecutionError);

        let error = FetchUserDataError(QueryExecutionError(DatabaseConnectionError));

        //* When
        let result = error_source(&error);

        //* Then
        // The error should be logged using `%` to display the error message
        // Example: tracing::error!(error = %err, ...)
        let error_str = format!("{}", error);
        assert_eq!(
            error_str, "failed to fetch user data",
            "top-level error message should match"
        );

        // The error_source array should NOT include the top-level error message,
        // only the underlying source errors in the chain
        let error_source_str = format!("{:?}", result);
        assert_eq!(
            error_source_str, r#"["failed to execute query", "database connection refused"]"#,
            "error source chain should contain both sources in order"
        );
    }

    #[test]
    fn error_source_with_no_source_returns_empty_vec() {
        //* Given
        /// Simple error with no underlying cause
        #[derive(Debug, thiserror::Error)]
        #[error("something went wrong")]
        struct SimpleError;

        let error = SimpleError;

        //* When
        let result = error_source(&error);

        //* Then
        // The error should be logged using `%` to display the error message
        // Example: tracing::error!(error = %err, ...)
        let error_str = format!("{}", error);
        assert_eq!(
            error_str, "something went wrong",
            "error message should match"
        );

        // The error_source array should NOT include the top-level error message,
        // only the underlying source errors in the chain (empty in this case)
        let error_source_str = format!("{:?}", result);
        assert_eq!(
            error_source_str, "[]",
            "error source chain should be empty for errors with no source"
        );
    }
}
