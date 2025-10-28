use std::{
    borrow::Cow,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

pub use opentelemetry::{KeyValue, metrics::Meter};
use opentelemetry_otlp::{ExporterBuildError, Protocol, WithExportConfig};
pub use opentelemetry_sdk::metrics::SdkMeterProvider;

pub const DEFAULT_METRICS_EXPORT_INTERVAL: Duration = Duration::from_secs(60);

pub type Result = std::result::Result<(SdkMeterProvider, Meter), ExporterBuildError>;

const AMP_METER: &str = "amp-meter";

/// Starts a periodic OpenTelemetry metrics exporter over binary HTTP transport.
pub fn start(
    url: String,
    export_interval: Option<Duration>,
    compression: Option<String>,
) -> Result {
    // Set compression via environment variable - default to "none" if not already set
    let compression_value = match compression.as_deref() {
        Some("gzip") => "gzip",
        Some("none") => "none",
        _ => "none", // Default to "none" if not specified
    };

    unsafe {
        std::env::set_var("OTLP_METRICS_COMPRESSION", compression_value);
    }

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
    let meter = opentelemetry::global::meter(AMP_METER);

    Ok((meter_provider, meter))
}

/// An OpenTelemetry gauge.
#[derive(Debug, Clone)]
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
        meter: &Meter,
        name: impl Into<Cow<'static, str>>,
        description: impl Into<Cow<'static, str>>,
        unit: impl Into<Cow<'static, str>>,
    ) -> Self {
        let inner = meter
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
        meter: &Meter,
        name: impl Into<Cow<'static, str>>,
        description: impl Into<Cow<'static, str>>,
        unit: impl Into<Cow<'static, str>>,
    ) -> Self {
        let inner = meter
            .f64_gauge(name)
            .with_description(description)
            .with_unit(unit)
            .build();

        Self(inner)
    }
}

/// An OpenTelemetry histogram.
#[derive(Debug, Clone)]
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
        meter: &Meter,
        name: impl Into<Cow<'static, str>>,
        description: impl Into<Cow<'static, str>>,
        unit: impl Into<Cow<'static, str>>,
    ) -> Self {
        let inner = meter
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
        meter: &Meter,
        name: impl Into<Cow<'static, str>>,
        description: impl Into<Cow<'static, str>>,
        unit: impl Into<Cow<'static, str>>,
    ) -> Self {
        let inner = meter
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
#[derive(Debug, Clone)]
pub struct Counter(opentelemetry::metrics::Counter<u64>);

impl Counter {
    /// Create a new OpenTelemetry counter.
    pub fn new(
        meter: &Meter,
        name: impl Into<Cow<'static, str>>,
        description: impl Into<Cow<'static, str>>,
    ) -> Self {
        let inner = meter
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

/// An OpenTelemetry UpDownCounter for values that can increase and decrease.
///
/// Unlike Counter (which only increases), UpDownCounter supports both add and subtract operations.
/// Useful for tracking active connections, queue depth, or any metric that fluctuates.
#[derive(Debug, Clone)]
pub struct UpDownCounter(opentelemetry::metrics::UpDownCounter<i64>);

impl UpDownCounter {
    /// Create a new OpenTelemetry UpDownCounter.
    pub fn new(
        meter: &Meter,
        name: impl Into<Cow<'static, str>>,
        description: impl Into<Cow<'static, str>>,
        unit: impl Into<Cow<'static, str>>,
    ) -> Self {
        let inner = meter
            .i64_up_down_counter(name)
            .with_description(description)
            .with_unit(unit)
            .build();

        Self(inner)
    }

    /// Add (increment) or subtract (decrement) with additional key-value pairs.
    pub fn add_with_kvs(&self, value: i64, kv_pairs: &[KeyValue]) {
        self.0.add(value, kv_pairs);
    }

    /// Add (increment) or subtract (decrement) without labels.
    pub fn add(&self, value: i64) {
        self.add_with_kvs(value, &[]);
    }

    /// Increment by 1 with additional key-value pairs.
    pub fn inc_with_kvs(&self, kv_pairs: &[KeyValue]) {
        self.add_with_kvs(1, kv_pairs);
    }

    /// Increment by 1.
    pub fn inc(&self) {
        self.add(1);
    }

    /// Decrement by 1 with additional key-value pairs.
    pub fn dec_with_kvs(&self, kv_pairs: &[KeyValue]) {
        self.add_with_kvs(-1, kv_pairs);
    }

    /// Decrement by 1.
    pub fn dec(&self) {
        self.add(-1);
    }
}

/// An OpenTelemetry counter that can also be read from.
///
/// If reading the counter value isn't needed, users can create a regular [Counter].
#[derive(Debug)]
pub struct ReadableCounter {
    counter: opentelemetry::metrics::Counter<u64>,
    /// A local copy used to enable reading the counter value.
    copy: AtomicU64,
}

impl ReadableCounter {
    /// Create a new readable OpenTelemetry counter.
    pub fn new(
        meter: &Meter,
        name: impl Into<Cow<'static, str>>,
        description: impl Into<Cow<'static, str>>,
    ) -> Self {
        let counter = meter
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
