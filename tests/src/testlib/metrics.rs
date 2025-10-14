//! Test utilities for validating OpenTelemetry metrics collection.
//!
//! This module provides helpers for testing metrics using in-memory exporters,
//! allowing tests to validate that metrics are recorded correctly without requiring
//! external observability infrastructure.
//!
//! # Example
//!
//! ```rust,ignore
//! use crate::testlib::metrics_utils::{TestMetricsContext, find_counter};
//!
//! #[tokio::test]
//! async fn test_my_metric() {
//!     let ctx = TestMetricsContext::new();
//!     let registry = MyMetricsRegistry::new(&ctx.meter());
//!
//!     // Record some metrics
//!     registry.my_counter.inc();
//!
//!     // Collect and validate
//!     let metrics = ctx.collect().await;
//!     let counter = find_counter(&metrics, "my_counter").expect("counter not found");
//!     assert_eq!(counter.value, 1);
//! }
//! ```

use opentelemetry_sdk::metrics::InMemoryMetricExporter;

/// Test context for metrics collection.
///
/// Provides an in-memory metrics exporter and meter for testing, allowing
/// tests to record and validate metrics without external dependencies.
pub struct TestMetricsContext {
    provider: monitoring::telemetry::metrics::SdkMeterProvider,
    exporter: InMemoryMetricExporter,
    meter: monitoring::telemetry::metrics::Meter,
}

impl TestMetricsContext {
    /// Create a new test metrics context.
    ///
    /// Sets up an in-memory exporter and meter provider that can be used
    /// to create metrics registries for testing.
    pub fn new() -> Self {
        use opentelemetry::metrics::MeterProvider;

        let exporter = InMemoryMetricExporter::default();

        let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();

        let meter = provider.meter("test-meter");

        Self {
            provider,
            exporter,
            meter,
        }
    }

    /// Get a reference to the meter for creating metrics.
    pub fn meter(&self) -> &monitoring::telemetry::metrics::Meter {
        &self.meter
    }

    /// Get a new meter from the provider.
    ///
    /// This creates a meter that's connected to the same provider and exporter,
    /// ensuring all metrics are collected together. Use this instead of cloning
    /// the meter when passing it to components that take ownership.
    pub fn create_meter(&self) -> monitoring::telemetry::metrics::Meter {
        use opentelemetry::metrics::MeterProvider;
        self.provider.meter("test-meter")
    }

    /// Collect all recorded metrics.
    ///
    /// Forces a flush of the meter provider and returns all collected metrics
    /// from the in-memory exporter. Resets the exporter after collection to
    /// ensure that subsequent calls only return new metrics.
    pub async fn collect(&self) -> Vec<opentelemetry_sdk::metrics::data::ResourceMetrics> {
        // Force flush to ensure all metrics are collected
        self.provider
            .force_flush()
            .expect("Failed to flush metrics");

        // Get finished metrics from exporter
        let metrics = self
            .exporter
            .get_finished_metrics()
            .expect("Failed to get metrics from exporter");

        // Reset exporter so next collect only returns new metrics
        self.exporter.reset();

        metrics
    }
}

impl Default for TestMetricsContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Find a counter metric by name in the collected metrics.
///
/// Returns the first counter found with the given name, or None if not found.
pub fn find_counter(
    metrics: &[opentelemetry_sdk::metrics::data::ResourceMetrics],
    name: &str,
) -> Option<CounterData> {
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};

    for resource_metric in metrics {
        for scope_metric in resource_metric.scope_metrics() {
            for metric in scope_metric.metrics() {
                if metric.name() == name {
                    // Counters are u64 type
                    if let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data()
                        && let Some(data_point) = sum.data_points().next()
                    {
                        return Some(CounterData {
                            name: metric.name().to_string(),
                            description: metric.description().to_string(),
                            value: data_point.value(),
                            attributes: data_point
                                .attributes()
                                .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                                .collect(),
                        });
                    }
                }
            }
        }
    }
    None
}

/// Find all counter data points for a given metric name.
///
/// Returns all data points (potentially with different label combinations) for the counter.
pub fn find_all_counter_points(
    metrics: &[opentelemetry_sdk::metrics::data::ResourceMetrics],
    name: &str,
) -> Vec<CounterData> {
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};

    let mut results = Vec::new();

    for resource_metric in metrics {
        for scope_metric in resource_metric.scope_metrics() {
            for metric in scope_metric.metrics() {
                if metric.name() == name {
                    // Counters are u64 type
                    if let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() {
                        for data_point in sum.data_points() {
                            results.push(CounterData {
                                name: metric.name().to_string(),
                                description: metric.description().to_string(),
                                value: data_point.value(),
                                attributes: data_point
                                    .attributes()
                                    .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                                    .collect(),
                            });
                        }
                    }
                }
            }
        }
    }

    results
}

/// Find a gauge metric by name in the collected metrics.
pub fn find_gauge(
    metrics: &[opentelemetry_sdk::metrics::data::ResourceMetrics],
    name: &str,
) -> Option<GaugeData> {
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};

    for resource_metric in metrics {
        for scope_metric in resource_metric.scope_metrics() {
            for metric in scope_metric.metrics() {
                if metric.name() == name {
                    // Try U64 gauge first
                    if let AggregatedMetrics::U64(MetricData::Gauge(gauge)) = metric.data()
                        && let Some(data_point) = gauge.data_points().next()
                    {
                        return Some(GaugeData {
                            name: metric.name().to_string(),
                            description: metric.description().to_string(),
                            value: opentelemetry::Value::I64(data_point.value() as i64),
                            attributes: data_point
                                .attributes()
                                .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                                .collect(),
                        });
                    }
                    // Try F64 gauge
                    if let AggregatedMetrics::F64(MetricData::Gauge(gauge)) = metric.data()
                        && let Some(data_point) = gauge.data_points().next()
                    {
                        return Some(GaugeData {
                            name: metric.name().to_string(),
                            description: metric.description().to_string(),
                            value: opentelemetry::Value::F64(data_point.value()),
                            attributes: data_point
                                .attributes()
                                .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                                .collect(),
                        });
                    }
                }
            }
        }
    }
    None
}

/// Find a histogram metric by name in the collected metrics.
pub fn find_histogram(
    metrics: &[opentelemetry_sdk::metrics::data::ResourceMetrics],
    name: &str,
) -> Option<HistogramData> {
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};

    for resource_metric in metrics {
        for scope_metric in resource_metric.scope_metrics() {
            for metric in scope_metric.metrics() {
                if metric.name() == name {
                    // Histograms are f64 type
                    if let AggregatedMetrics::F64(MetricData::Histogram(histogram)) = metric.data()
                        && let Some(data_point) = histogram.data_points().next()
                    {
                        return Some(HistogramData {
                            name: metric.name().to_string(),
                            description: metric.description().to_string(),
                            count: data_point.count(),
                            sum: data_point.sum(),
                            min: data_point.min(),
                            max: data_point.max(),
                            attributes: data_point
                                .attributes()
                                .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                                .collect(),
                        });
                    }
                }
            }
        }
    }
    None
}

/// Find all histogram data points for a given metric name.
///
/// Returns all data points (potentially with different label combinations) for the histogram.
pub fn find_all_histogram_points(
    metrics: &[opentelemetry_sdk::metrics::data::ResourceMetrics],
    name: &str,
) -> Vec<HistogramData> {
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};

    let mut results = Vec::new();

    for resource_metric in metrics {
        for scope_metric in resource_metric.scope_metrics() {
            for metric in scope_metric.metrics() {
                if metric.name() == name {
                    // Histograms are f64 type
                    if let AggregatedMetrics::F64(MetricData::Histogram(histogram)) = metric.data()
                    {
                        for data_point in histogram.data_points() {
                            results.push(HistogramData {
                                name: metric.name().to_string(),
                                description: metric.description().to_string(),
                                count: data_point.count(),
                                sum: data_point.sum(),
                                min: data_point.min(),
                                max: data_point.max(),
                                attributes: data_point
                                    .attributes()
                                    .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                                    .collect(),
                            });
                        }
                    }
                }
            }
        }
    }

    results
}

/// List all metric names in the collected metrics.
///
/// Useful for debugging to see what metrics were actually recorded.
pub fn list_metric_names(
    metrics: &[opentelemetry_sdk::metrics::data::ResourceMetrics],
) -> Vec<String> {
    let mut names = Vec::new();

    for resource_metric in metrics {
        for scope_metric in resource_metric.scope_metrics() {
            for metric in scope_metric.metrics() {
                names.push(metric.name().to_string());
            }
        }
    }

    names
}

/// Counter metric data for assertions.
#[derive(Debug, Clone)]
pub struct CounterData {
    pub name: String,
    pub description: String,
    pub value: u64,
    pub attributes: Vec<(String, String)>,
}

impl CounterData {
    /// Check if the counter has a specific attribute with a given value.
    pub fn has_attribute(&self, key: &str, value: &str) -> bool {
        self.attributes.iter().any(|(k, v)| k == key && v == value)
    }

    /// Get the value of a specific attribute.
    pub fn get_attribute(&self, key: &str) -> Option<&str> {
        self.attributes
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
}

/// Gauge metric data for assertions.
#[derive(Debug, Clone)]
pub struct GaugeData {
    pub name: String,
    pub description: String,
    pub value: opentelemetry::Value,
    pub attributes: Vec<(String, String)>,
}

impl GaugeData {
    /// Get the gauge value as u64, if it is one.
    pub fn as_u64(&self) -> Option<u64> {
        match &self.value {
            opentelemetry::Value::I64(v) => Some(*v as u64),
            opentelemetry::Value::F64(v) => Some(*v as u64),
            _ => None,
        }
    }

    /// Get the gauge value as f64, if it is one.
    pub fn as_f64(&self) -> Option<f64> {
        match &self.value {
            opentelemetry::Value::F64(v) => Some(*v),
            opentelemetry::Value::I64(v) => Some(*v as f64),
            _ => None,
        }
    }

    /// Check if the gauge has a specific attribute with a given value.
    pub fn has_attribute(&self, key: &str, value: &str) -> bool {
        self.attributes.iter().any(|(k, v)| k == key && v == value)
    }

    /// Get the value of a specific attribute.
    pub fn get_attribute(&self, key: &str) -> Option<&str> {
        self.attributes
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
}

/// Histogram metric data for assertions.
#[derive(Debug, Clone)]
pub struct HistogramData {
    pub name: String,
    pub description: String,
    pub count: u64,
    pub sum: f64,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub attributes: Vec<(String, String)>,
}

impl HistogramData {
    /// Calculate the average value (sum / count).
    pub fn average(&self) -> Option<f64> {
        if self.count > 0 {
            Some(self.sum / self.count as f64)
        } else {
            None
        }
    }

    /// Check if the histogram has a specific attribute with a given value.
    pub fn has_attribute(&self, key: &str, value: &str) -> bool {
        self.attributes.iter().any(|(k, v)| k == key && v == value)
    }

    /// Get the value of a specific attribute.
    pub fn get_attribute(&self, key: &str) -> Option<&str> {
        self.attributes
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
}
