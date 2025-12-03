use opentelemetry_otlp::{ExporterBuildError, Protocol, WithExportConfig};
use opentelemetry_sdk::trace::SdkTracerProvider as Inner;

pub type Result = std::result::Result<TracerProvider, ExporterBuildError>;

/// RAII wrapper for OpenTelemetry tracer provider.
///
/// When dropped, flushes pending traces and shuts down the provider.
pub struct TracerProvider(Inner);

impl std::ops::Deref for TracerProvider {
    type Target = Inner;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for TracerProvider {
    fn drop(&mut self) {
        if let Err(err) = self.0.force_flush() {
            tracing::error!(
                error = %err,
                error_source = crate::logging::error_source(&err),
                "failed to flush OpenTelemetry tracing provider"
            );
        }
        if let Err(err) = self.0.shutdown() {
            tracing::error!(
                error = %err,
                error_source = crate::logging::error_source(&err),
                "failed to shutdown OpenTelemetry tracing provider"
            );
        }
    }
}

/// Create a new OpenTelemetry tracer provider set up with the given URL and HTTP transport.
pub fn provider(url: &str, trace_ratio: f64) -> Result {
    let resource = opentelemetry_sdk::Resource::builder()
        .with_attribute(opentelemetry::KeyValue::new("service.name", "tracing"))
        .build();

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_endpoint(url)
        .build()?;

    let provider = Inner::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .with_sampler(opentelemetry_sdk::trace::Sampler::TraceIdRatioBased(
            trace_ratio,
        ))
        .build();

    Ok(TracerProvider(provider))
}
