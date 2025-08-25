//! A set of utilities that enable running [OpenTelemetry](https://docs.rs/opentelemetry/latest/opentelemetry) exporters and collecting OpenTelemetry [metrics] and [traces].

pub use opentelemetry_otlp::ExporterBuildError;

pub mod metrics;
pub mod traces;
