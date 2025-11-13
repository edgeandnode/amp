pub mod instrumented_task;
pub mod logging;
pub mod plan_histogram;
pub mod runtime_metrics;
pub mod task_id;
pub mod task_runtime_context;
pub mod task_type;
pub mod telemetry;

// Re-export commonly used types
pub use instrumented_task::{InstrumentedTaskExecution, InstrumentedTaskGuard};
pub use task_id::TaskId;
pub use task_runtime_context::{TASK_CONTEXT, TaskMetrics, TaskRuntimeContext};
pub use task_type::TaskType;

pub type TelemetryKit = (
    Option<telemetry::traces::SdkTracerProvider>,
    Option<telemetry::metrics::SdkMeterProvider>,
    Option<telemetry::metrics::Meter>,
);

pub fn init(
    opentelemetry_config: Option<&common::config::OpenTelemetryConfig>,
) -> Result<TelemetryKit, telemetry::ExporterBuildError> {
    let Some(opentelemetry_config) = opentelemetry_config else {
        // Make sure logging is enabled in any case.
        logging::init();
        return Ok((None, None, None));
    };

    let telemetry_tracing_provider = match (
        opentelemetry_config.trace_url.clone(),
        opentelemetry_config.trace_ratio,
    ) {
        (Some(url), trace_ratio) => {
            let provider = logging::init_with_telemetry(url, trace_ratio.unwrap_or(1.0))?;
            Some(provider)
        }
        (None, trace_ratio) => {
            logging::init();

            if trace_ratio.is_some() {
                tracing::warn!(
                    "OpenTelemetry trace ratio is set but will not be used. Please provide an OpenTelemetry trace URL to enable tracing."
                );
            }

            None
        }
    };

    let (telemetry_metrics_provider, telemetry_metrics_meter) = match (
        opentelemetry_config.metrics_url.clone(),
        opentelemetry_config.metrics_export_interval,
    ) {
        (Some(url), export_interval) => {
            let (provider, meter) = telemetry::metrics::start(url, export_interval)?;
            (Some(provider), Some(meter))
        }
        (None, export_interval) => {
            if export_interval.is_some() {
                tracing::warn!(
                    "OpenTelemetry metrics export interval is set but will not be used. Please provide an OpenTelemetry metrics URL to enable metrics."
                );
            }

            (None, None)
        }
    };

    Ok((
        telemetry_tracing_provider,
        telemetry_metrics_provider,
        telemetry_metrics_meter,
    ))
}

pub fn deinit(
    metrics_provider: Option<telemetry::metrics::SdkMeterProvider>,
    tracing_provider: Option<telemetry::traces::SdkTracerProvider>,
) -> Result<(), String> {
    if let Some(provider) = metrics_provider {
        telemetry::metrics::provider_flush_shutdown(provider).map_err(|e| {
            format!("Failed to flush and shutdown OpenTelemetry metrics provider: {e}")
        })?;
    }
    if let Some(provider) = tracing_provider {
        telemetry::traces::provider_flush_shutdown(provider).map_err(|e| {
            format!("Failed to flush and shutdown OpenTelemetry tracing provider: {e}")
        })?;
    }

    Ok(())
}
