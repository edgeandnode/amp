use std::{
    borrow::Cow,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

pub use opentelemetry::KeyValue;
use opentelemetry_otlp::{ExporterBuildError, Protocol, WithExportConfig};
pub use opentelemetry_sdk::metrics::SdkMeterProvider;

pub const DEFAULT_METRICS_EXPORT_INTERVAL: Duration = Duration::from_secs(60);

pub type Result = std::result::Result<SdkMeterProvider, ExporterBuildError>;

const NOZZLE_METER: &str = "nozzle-meter";

/// Starts a periodic OpenTelemetry metrics exporter over binary HTTP transport.
pub fn start(url: String, export_interval: Option<Duration>) -> Result {
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_endpoint(url)
        .build()?;
    // If not set, use the default periodic exporter value.
    let export_interval = export_interval.unwrap_or(DEFAULT_METRICS_EXPORT_INTERVAL);
    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter)
        .with_interval(export_interval)
        .build();

    let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_reader(reader)
        .build();
    opentelemetry::global::set_meter_provider(meter_provider.clone());

    Ok(meter_provider)
}

/// An OpenTelemetry gauge.
pub struct Gauge<T>(opentelemetry::metrics::Gauge<T>);

impl<T> Gauge<T> {
    /// Add a new observation point with additional key-value pairs.
    pub fn record_with_kvs(&self, value: T, kv_pairs: &[KeyValue]) {
        self.0.record(value, kv_pairs);
    }

    /// Add a new observation point.
    pub fn record(&self, value: T) {
        self.record_with_kvs(value, &[]);
    }
}

impl Gauge<u64> {
    /// Create a new u64 OpenTelemetry gauge.
    pub fn new_u64(
        name: impl Into<Cow<'static, str>>,
        description: impl Into<Cow<'static, str>>,
        unit: impl Into<Cow<'static, str>>,
    ) -> Self {
        let inner = opentelemetry::global::meter(NOZZLE_METER)
            .u64_gauge(name)
            .with_description(description)
            .with_unit(unit)
            .build();

        Self(inner)
    }
}

impl Gauge<f64> {
    /// Create a new f64 OpenTelemetry gauge.
    pub fn new_f64(
        name: impl Into<Cow<'static, str>>,
        description: impl Into<Cow<'static, str>>,
        unit: impl Into<Cow<'static, str>>,
    ) -> Self {
        let inner = opentelemetry::global::meter(NOZZLE_METER)
            .f64_gauge(name)
            .with_description(description)
            .with_unit(unit)
            .build();

        Self(inner)
    }
}

/// An OpenTelemetry histogram.
pub struct Histogram<T>(opentelemetry::metrics::Histogram<T>);

impl<T> Histogram<T> {
    /// Record a new observation point with additional key-value pairs.
    pub fn record_with_kvs(&self, value: T, kv_pairs: &[KeyValue]) {
        self.0.record(value, kv_pairs);
    }

    /// Record a new observation point.
    pub fn record(&self, value: T) {
        self.record_with_kvs(value, &[]);
    }
}

impl Histogram<u64> {
    /// Create a new u64 OpenTelemetry histogram.
    pub fn new_u64(
        name: impl Into<Cow<'static, str>>,
        description: impl Into<Cow<'static, str>>,
        unit: impl Into<Cow<'static, str>>,
    ) -> Self {
        let inner = opentelemetry::global::meter(NOZZLE_METER)
            .u64_histogram(name)
            .with_description(description)
            .with_unit(unit)
            .build();

        Self(inner)
    }
}

impl Histogram<f64> {
    /// Create a new f64 OpenTelemetry histogram.
    pub fn new_f64(
        name: impl Into<Cow<'static, str>>,
        description: impl Into<Cow<'static, str>>,
        unit: impl Into<Cow<'static, str>>,
    ) -> Self {
        let inner = opentelemetry::global::meter(NOZZLE_METER)
            .f64_histogram(name)
            .with_description(description)
            .with_unit(unit)
            .build();

        Self(inner)
    }
}

/// An OpenTelemetry counter.
///
/// As this type does not allow reading the current value, use a [ReadableCounter] if this
/// functionality is needed.
pub struct Counter(opentelemetry::metrics::Counter<u64>);

impl Counter {
    /// Create a new OpenTelemetry counter.
    pub fn new(
        name: impl Into<Cow<'static, str>>,
        description: impl Into<Cow<'static, str>>,
    ) -> Self {
        let inner = opentelemetry::global::meter(NOZZLE_METER)
            .u64_counter(name)
            .with_description(description)
            .build();

        Self(inner)
    }

    /// Increment the OpenTelemetry counter by the given amount with additional key-value pairs.
    pub fn inc_by_with_kvs(&self, value: u64, kv_pairs: &[KeyValue]) {
        self.0.add(value, kv_pairs);
    }

    /// Increment the OpenTelemetry counter by one with additional key-value pairs.
    pub fn inc_with_kvs(&self, kv_pairs: &[KeyValue]) {
        self.inc_by_with_kvs(1, kv_pairs);
    }

    /// Increment the OpenTelemetry counter by the given amount.
    pub fn inc_by(&self, value: u64) {
        self.inc_by_with_kvs(value, &[]);
    }

    /// Increment the OpenTelemetry counter by one.
    pub fn inc(&self) {
        self.inc_with_kvs(&[]);
    }
}

/// An OpenTelemetry counter that can also be read from.
///
/// If reading the counter value isn't needed, users can create a regular [Counter].
pub struct ReadableCounter {
    counter: opentelemetry::metrics::Counter<u64>,
    /// A local copy used to enable reading the counter value.
    copy: AtomicU64,
}

impl ReadableCounter {
    /// Create a new readable OpenTelemetry counter.
    pub fn new(
        name: impl Into<Cow<'static, str>>,
        description: impl Into<Cow<'static, str>>,
    ) -> Self {
        let counter = opentelemetry::global::meter(NOZZLE_METER)
            .u64_counter(name)
            .with_description(description)
            .build();

        Self {
            counter,
            copy: AtomicU64::new(0),
        }
    }

    /// Increment the OpenTelemetry counter.
    pub fn inc(&self) {
        self.counter.add(1, &[]);
        self.copy.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the OpenTelemetry counter by the given amount.
    pub fn inc_by(&self, value: u64) {
        self.counter.add(value, &[]);
        self.copy.fetch_add(value, Ordering::Relaxed);
    }

    /// Get the current counter value.
    pub fn get(&self) -> u64 {
        self.copy.load(Ordering::Relaxed)
    }
}

/// Flushes the OpenTelemetry metrics provider and shuts it down. This ensures that all
/// metrics are sent before the application exits. Note that during normal operation, metrics
/// are sent periodically.
pub fn provider_flush_shutdown(
    provider: SdkMeterProvider,
) -> std::result::Result<(), opentelemetry_sdk::error::OTelSdkError> {
    provider.force_flush()?;
    provider.shutdown()
}
