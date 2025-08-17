use opentelemetry_otlp::{ExporterBuildError, WithExportConfig};
use opentelemetry_sdk::trace::SdkTracerProvider;

pub type Result = std::result::Result<SdkTracerProvider, ExporterBuildError>;

/// Create a new OpenTelemetry tracer provider set up with the given URL and gRPC transport.
pub fn provider(url: String, trace_ratio: f64) -> Result {
    let resource = opentelemetry_sdk::Resource::builder()
        .with_attribute(opentelemetry::KeyValue::new(
            "service.name",
            "datafusion-tracing",
        ))
        .build();

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(url)
        .build()?;

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .with_sampler(opentelemetry_sdk::trace::Sampler::TraceIdRatioBased(
            trace_ratio,
        ))
        .build();

    Ok(provider)
}
