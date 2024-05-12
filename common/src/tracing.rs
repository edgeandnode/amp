use tracing_subscriber::{filter::LevelFilter, EnvFilter};

pub fn register_logger() {
    let log_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    tracing_subscriber::fmt()
        .with_env_filter(log_filter)
        .with_ansi(atty::is(atty::Stream::Stderr))
        .init();
}
