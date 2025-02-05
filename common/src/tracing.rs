use std::sync::Once;

use tracing_subscriber::{filter::LevelFilter, EnvFilter};

static NOZZLE_LOG_ENV_VAR: &str = "NOZZLE_LOG";

/// List of crates in the workspace.
const NOZZLE_CRATES: &[&str] = &[
    "admin-api",
    "common",
    "dataset-store",
    "dump",
    "dump-check",
    "evm-rpc-datasets",
    "firehose-datasets",
    "http-common",
    "metadata-db",
    "nozzle",
    "registry-service",
    "server",
    "substreams-datasets",
    "tests",
];

pub fn register_logger() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        // Parse directives from RUST_LOG
        let log_filter = EnvFilter::builder().with_default_directive(LevelFilter::ERROR.into());
        let directive_string = std::env::var(EnvFilter::DEFAULT_ENV).unwrap_or_default();
        let mut env_filter = log_filter.parse(&directive_string).unwrap();

        let nozzle_log_level =
            std::env::var(NOZZLE_LOG_ENV_VAR).unwrap_or_else(|_| "debug".to_string());

        for crate_name in NOZZLE_CRATES {
            // Add directives for each crate in NOZZLE_CRATES, if not overriden by RUST_LOG
            if !directive_string.contains(&format!("{crate_name}=")) {
                env_filter = env_filter
                    .add_directive(format!("{crate_name}={nozzle_log_level}").parse().unwrap());
            }
        }

        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_ansi(atty::is(atty::Stream::Stderr))
            .init();
    });
}

/// If this fails, just update the above `NOZZLE_CRATES` to match reality.
#[test]
fn assert_nozzle_crates() {
    use cargo_metadata::MetadataCommand;

    let cmd = MetadataCommand::new().exec().unwrap();
    let mut names: Vec<String> = cmd
        .workspace_packages()
        .into_iter()
        .map(|pkg| pkg.name.clone())
        .collect();
    names.sort();
    assert_eq!(names, NOZZLE_CRATES);
}
