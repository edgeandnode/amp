use opentelemetry::metrics::Meter;

pub mod config;
pub mod logging;
pub mod telemetry;

use self::{
    config::OpenTelemetryConfig,
    telemetry::{
        metrics::{self, MeterProvider},
        traces::TracerProvider,
    },
};

/// Return type for the `init` function containing optional telemetry providers and meter.
///
/// The tuple contains:
/// - `(Option<TracerProvider>, Option<MeterProvider>)`: RAII guards for providers
/// - `Option<Meter>`: Meter instance if metrics are enabled
pub type TelemetryKit = (
    (Option<TracerProvider>, Option<MeterProvider>),
    Option<Meter>,
);

pub fn init(
    config: Option<&OpenTelemetryConfig>,
) -> Result<TelemetryKit, telemetry::ExporterBuildError> {
    let Some(config) = config else {
        logging::init();
        return Ok(((None, None), None));
    };

    // Initialize tracing
    let tracing_provider = match config.trace_url.as_deref() {
        Some(url) => Some(logging::init_with_telemetry(url, config.trace_ratio)?),
        None => {
            logging::init();
            None
        }
    };

    // Initialize metrics
    let (metrics_provider, meter) = match config.metrics_url.as_deref() {
        Some(url) => {
            let (provider, meter) = metrics::start(url, config.metrics_export_interval)?;
            (Some(provider), Some(meter))
        }
        None => (None, None),
    };

    Ok(((tracing_provider, metrics_provider), meter))
}
