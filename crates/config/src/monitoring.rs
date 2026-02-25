use std::time::Duration;

/// OpenTelemetry observability configuration for metrics and tracing.
///
/// Controls export of metrics and traces to an OpenTelemetry-compatible collector
/// via OTLP/HTTP.
#[derive(Debug, Clone, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct OpenTelemetryConfig {
    /// Remote OpenTelemetry metrics collector endpoint.
    /// Metrics are sent over binary HTTP (OTLP/HTTP).
    /// Example: `"http://localhost:4318"`
    pub metrics_url: Option<String>,

    /// Interval (in seconds) between metrics exports to the collector.
    /// Only used when `metrics_url` is set. Uses the SDK default if omitted.
    #[serde(
        default,
        rename = "metrics_export_interval_secs",
        deserialize_with = "deserialize_otel_duration"
    )]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<f64>"))]
    pub metrics_export_interval: Option<Duration>,

    /// Remote OpenTelemetry traces collector endpoint.
    /// Traces are sent over HTTP (OTLP/HTTP).
    /// Example: `"http://localhost:4318"`
    pub trace_url: Option<String>,

    /// Ratio of traces to sample, from 0.0 (none) to 1.0 (all).
    /// Default: 1.0 (sample every trace).
    #[serde(default = "default_trace_ratio")]
    pub trace_ratio: f64,
}

fn default_trace_ratio() -> f64 {
    1.0
}

fn deserialize_otel_duration<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    <Option<f64> as serde::Deserialize>::deserialize(deserializer)
        .map(|option| option.map(Duration::from_secs_f64))
}

impl From<&OpenTelemetryConfig> for amp_monitoring::config::OpenTelemetryConfig {
    fn from(config: &OpenTelemetryConfig) -> Self {
        Self {
            metrics_url: config.metrics_url.clone(),
            metrics_export_interval: config.metrics_export_interval,
            trace_url: config.trace_url.clone(),
            trace_ratio: config.trace_ratio,
        }
    }
}
