use std::time::Duration;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct OpenTelemetryConfig {
    /// Remote OpenTelemetry metrics collector endpoint. Metrics are sent over binary HTTP.
    pub metrics_url: Option<String>,
    /// The interval (in seconds) at which to export metrics to the OpenTelemetry collector.
    ///
    /// Only used when `metrics_url` is provided. If not set, uses the default export interval.
    #[serde(
        default,
        rename = "metrics_export_interval_secs",
        deserialize_with = "deserialize_duration"
    )]
    pub metrics_export_interval: Option<Duration>,
    /// Remote OpenTelemetry traces collector endpoint. Traces are sent over HTTP.
    pub trace_url: Option<String>,
    /// The ratio of traces to sample (f64). Samples all traces by default (1.0).
    ///
    /// Only used when `trace_url` is provided. Valid range: 0.0 to 1.0.
    #[serde(default = "default_trace_ratio")]
    pub trace_ratio: f64,
}

fn default_trace_ratio() -> f64 {
    1.0 // Sample all traces by default
}

fn deserialize_duration<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    <Option<f64>>::deserialize(deserializer).map(|option| option.map(Duration::from_secs_f64))
}
